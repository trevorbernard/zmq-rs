//! One TCP connection: greeting, security handshake, then framed messages.
//!
//! The engine runs on an I/O thread and moves messages between the TCP
//! stream and the session's pipes. mio is edge-triggered, so every read and
//! write continues until the OS reports `WouldBlock`; when a full receive
//! pipe stops reading early, the pipe's waker brings the engine back.

use std::io::{self, Read, Write};
use std::mem;
use std::sync::{PoisonError, mpsc};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use mio::net::TcpStream;
use zmq_pipe::{Reader, TrySendError, Writer};
use zmtp::{
    ClientStep, Command, DecodeError, Decoder, DecoderConfig, Encoder, Established, Greeting,
    GreetingConfig, GreetingError, HandshakeError, Incoming, LegacyPeers, Message, NullHandshake,
    PeerGreeting, PendingAuthentication, Ping, PingContext, PlainClient, PlainServer,
    PlainServerConfig, Role, ServerStep, SocketType, Subscription, Ttl, Version, ZapReply,
};

use crate::context::PeerSlot;
use crate::{ConnectionConfig, Security, Topics, ZapResponder};

/// The shortest PING interval, so a zero interval cannot spin.
const MIN_PING_INTERVAL: Duration = Duration::from_millis(1);

/// Stop taking messages from the socket while this much output is unsent.
const MAX_PENDING_OUTPUT: usize = 64 * 1024;

pub(crate) struct Engine {
    stream: TcpStream,
    config: ConnectionConfig,
    connected: bool,
    state: State,
    out: Vec<u8>,
    out_pos: usize,
    /// The last look at the socket's pipe found it empty or closed.
    outbound_empty: bool,
    deadlines: Deadlines,
}

/// When the engine must act or give up, all `None` while not applicable.
#[derive(Debug, Default)]
struct Deadlines {
    /// The greeting and handshake must finish by then.
    handshake: Option<Instant>,
    /// The next PING is due.
    next_ping: Option<Instant>,
    /// Traffic must arrive by then, after we sent a PING.
    ping_timeout: Option<Instant>,
    /// Traffic must arrive by then, per the TTL in the peer's last PING.
    ttl: Option<Instant>,
}

impl Deadlines {
    fn earliest(&self) -> Option<Instant> {
        [self.handshake, self.next_ping, self.ping_timeout, self.ttl]
            .into_iter()
            .flatten()
            .min()
    }

    /// Whether the peer has failed one of its deadlines at `now`.
    fn expired(&self, now: Instant) -> bool {
        [self.handshake, self.ping_timeout, self.ttl]
            .into_iter()
            .flatten()
            .any(|deadline| deadline <= now)
    }

    /// Any frame from the peer shows it is alive.
    fn heard_from_peer(&mut self) {
        self.ping_timeout = None;
        self.ttl = None;
    }
}

enum State {
    Greeting(Greeting),
    Handshake {
        mechanism: Mechanism,
        decoder: Decoder,
    },
    Active {
        decoder: Decoder,
        encoder: Encoder,
        /// A message the receive pipe refused because it was full.
        pending: Option<Message>,
    },
    /// Only seen after an error, which ends the engine.
    Failed,
}

/// The security handshake in progress.
enum Mechanism {
    Null(NullHandshake),
    PlainClient(PlainClient),
    PlainServer(PlainServer),
    /// A PLAIN server waiting for its authenticator's answer.
    Authenticating {
        pending: PendingAuthentication,
        reply: mpsc::Receiver<ZapReply>,
    },
}

/// Where a handshake step leads.
enum Step {
    Continue(Mechanism),
    Established(Established),
}

/// Why an engine stopped. The connection is closed.
#[derive(Debug)]
#[expect(
    dead_code,
    reason = "the details reach the socket once monitor events exist"
)]
pub(crate) enum EngineError {
    Io(io::Error),
    PeerClosed,
    Greeting(GreetingError),
    Handshake(HandshakeError),
    Decode(DecodeError),
    /// The handshake took too long, or the peer went quiet past a heartbeat
    /// deadline.
    Timeout,
}

impl EngineError {
    /// Whether the connection broke, rather than the peer misbehaving.
    /// libzmq reconnects after the former but not the latter.
    pub(crate) fn is_connection_error(&self) -> bool {
        matches!(self, Self::Io(_) | Self::PeerClosed | Self::Timeout)
    }
}

/// The session's pipe ends, lent to the engine while it runs.
pub(crate) struct Pipes<'a> {
    /// Towards the socket.
    pub(crate) inbound: &'a mut Writer<Message>,
    /// From the socket.
    pub(crate) outbound: &'a mut Reader<Message>,
    /// Where the handshake's outcome goes, for the socket to read.
    pub(crate) peer: &'a PeerSlot,
    /// The topics a SUB or XSUB has subscribed to over this session's
    /// connections, replayed to each new one as libzmq does on a hiccup.
    pub(crate) subscriptions: &'a mut Topics,
}

impl Engine {
    pub(crate) fn new(stream: TcpStream, config: ConnectionConfig) -> Self {
        let mechanism = match &config.security {
            Security::Null => zmtp::Mechanism::Null {
                legacy_peers: LegacyPeers::Allow,
            },
            Security::PlainClient(_) => zmtp::Mechanism::Plain(Role::Client),
            Security::PlainServer { .. } => zmtp::Mechanism::Plain(Role::Server),
        };
        let mut greeting = Greeting::new(GreetingConfig {
            socket_type: config.socket_type,
            identity: config.identity.clone(),
            mechanism,
        });
        let mut out = Vec::new();
        greeting.write_output(&mut out);
        Self {
            stream,
            config,
            connected: false,
            state: State::Greeting(greeting),
            out,
            out_pos: 0,
            outbound_empty: false,
            deadlines: Deadlines::default(),
        }
    }

    pub(crate) fn stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    /// When the engine next needs driving even without I/O: a PING is due,
    /// or a deadline expires.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.earliest()
    }

    /// Sends a PING if one is due, and starts waiting for traffic.
    fn maybe_ping(&mut self, now: Instant) {
        let (Some(heartbeat), Some(due)) = (self.config.heartbeat, self.deadlines.next_ping) else {
            return;
        };
        if due > now {
            return;
        }
        let State::Active { encoder, .. } = &self.state else {
            return;
        };
        let ping = Command::Ping(Ping {
            ttl: Ttl::from_duration(heartbeat.ttl),
            context: PingContext::default(),
        });
        let mut body = Vec::new();
        ping.encode(&mut body);
        if encoder.encode_command(&body, &mut self.out).is_ok() {
            self.deadlines.next_ping = Some(now + heartbeat.interval.max(MIN_PING_INTERVAL));
            self.deadlines
                .ping_timeout
                .get_or_insert(now + heartbeat.timeout);
        }
    }

    /// Whether the handshake has completed and messages are flowing.
    pub(crate) fn is_established(&self) -> bool {
        matches!(self.state, State::Active { .. })
    }

    /// Whether everything the socket queued has been written to the stream:
    /// the pipe was empty at the last look and no output is pending.
    pub(crate) fn caught_up(&self) -> bool {
        self.is_established() && self.outbound_empty && self.out.is_empty()
    }

    /// Does all the work currently possible: writing, taking messages from
    /// the socket, reading, and handing messages to the socket.
    pub(crate) fn drive(
        &mut self,
        pipes: &mut Pipes<'_>,
        cx: &mut Context<'_>,
    ) -> Result<(), EngineError> {
        if !self.connected && !self.finish_connect()? {
            return Ok(());
        }
        let now = Instant::now();
        if self.deadlines.expired(now) {
            return Err(EngineError::Timeout);
        }
        self.maybe_ping(now);
        loop {
            let wrote = self.flush()?;
            let pulled = self.pull_outbound(pipes, cx)?;
            let received = self.receive(pipes, cx)?;
            if !(wrote || pulled || received) {
                return Ok(());
            }
        }
    }

    /// Whether the non-blocking connect has completed.
    fn finish_connect(&mut self) -> Result<bool, EngineError> {
        if let Some(err) = self.stream.take_error().map_err(EngineError::Io)? {
            return Err(EngineError::Io(err));
        }
        match self.stream.peer_addr() {
            Ok(_) => {
                // libzmq disables Nagle; batching happens in the encoder.
                let _ = self.stream.set_nodelay(true);
                self.connected = true;
                // As libzmq, the handshake clock starts once connected.
                self.deadlines.handshake = self
                    .config
                    .handshake_timeout
                    .map(|timeout| Instant::now() + timeout);
                Ok(true)
            }
            Err(err) if err.kind() == io::ErrorKind::NotConnected => Ok(false),
            Err(err) => Err(EngineError::Io(err)),
        }
    }

    /// Writes pending output until done or the OS would block.
    fn flush(&mut self) -> Result<bool, EngineError> {
        let mut progressed = false;
        while self.out_pos < self.out.len() {
            match self.stream.write(&self.out[self.out_pos..]) {
                Ok(0) => return Err(EngineError::Io(io::ErrorKind::WriteZero.into())),
                Ok(n) => {
                    self.out_pos += n;
                    progressed = true;
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => return Err(EngineError::Io(err)),
            }
        }
        if self.out_pos == self.out.len() {
            self.out.clear();
            self.out_pos = 0;
        }
        Ok(progressed)
    }

    /// Encodes messages from the socket while output is not backed up.
    fn pull_outbound(
        &mut self,
        pipes: &mut Pipes<'_>,
        cx: &mut Context<'_>,
    ) -> Result<bool, EngineError> {
        let State::Active { encoder, .. } = &self.state else {
            return Ok(false);
        };
        let encoder = *encoder;
        let mut progressed = false;
        while self.out.len() - self.out_pos < MAX_PENDING_OUTPUT {
            match pipes.outbound.poll_recv(cx) {
                Poll::Ready(Some(message)) => {
                    match self.outgoing_subscription(&message) {
                        Some(subscription) => {
                            remember(pipes.subscriptions, &subscription);
                            self.encode_subscription(encoder, &subscription);
                        }
                        None => encoder.encode(&message, &mut self.out),
                    }
                    self.outbound_empty = false;
                    progressed = true;
                }
                // Empty, or closed and drained: the session decides when a
                // closed connection is finished.
                Poll::Ready(None) | Poll::Pending => {
                    self.outbound_empty = true;
                    break;
                }
            }
        }
        Ok(progressed)
    }

    /// Reads and handles input for the current state.
    fn receive(
        &mut self,
        pipes: &mut Pipes<'_>,
        cx: &mut Context<'_>,
    ) -> Result<bool, EngineError> {
        match mem::replace(&mut self.state, State::Failed) {
            State::Greeting(greeting) => self.receive_greeting(greeting),
            State::Handshake { mechanism, decoder } => {
                self.receive_handshake(mechanism, decoder, pipes, cx)
            }
            State::Active {
                mut decoder,
                encoder,
                mut pending,
            } => {
                let result = self.receive_active(&mut decoder, encoder, &mut pending, pipes, cx);
                self.state = State::Active {
                    decoder,
                    encoder,
                    pending,
                };
                result
            }
            State::Failed => Ok(false),
        }
    }

    fn receive_greeting(&mut self, mut greeting: Greeting) -> Result<bool, EngineError> {
        let mut buf = [0u8; 64];
        let n = match self.stream.read(&mut buf) {
            Ok(0) => return Err(EngineError::PeerClosed),
            Ok(n) => n,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                self.state = State::Greeting(greeting);
                return Ok(false);
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                self.state = State::Greeting(greeting);
                return Ok(true);
            }
            Err(err) => return Err(EngineError::Io(err)),
        };
        let feed = greeting.feed(&buf[..n]).map_err(EngineError::Greeting)?;
        greeting.write_output(&mut self.out);
        let Some(peer) = feed.peer else {
            self.state = State::Greeting(greeting);
            return Ok(true);
        };
        let mechanism = self.start_mechanism(&peer)?;
        let mut decoder = Decoder::new(
            peer.version(),
            DecoderConfig {
                max_frame_size: self.config.max_frame_size,
            },
        );
        // An unversioned ZMTP 1.0 peer's greeting octets were the start of
        // its identity frame.
        if let PeerGreeting::Zmtp1Unversioned { replay } = &peer {
            decoder.extend_from_slice(replay);
        }
        decoder.extend_from_slice(&buf[feed.consumed..n]);
        self.state = State::Handshake { mechanism, decoder };
        Ok(true)
    }

    /// Starts the configured mechanism once the greeting is done.
    fn start_mechanism(&mut self, peer: &PeerGreeting) -> Result<Mechanism, EngineError> {
        let config = &self.config;
        let mechanism = match &config.security {
            Security::Null => {
                NullHandshake::start(config.socket_type, &config.identity, peer, &mut self.out)
                    .map(Mechanism::Null)
            }
            Security::PlainClient(credentials) => PlainClient::start(
                config.socket_type,
                config.identity.clone(),
                credentials,
                peer,
                &mut self.out,
            )
            .map(Mechanism::PlainClient),
            Security::PlainServer { domain, .. } => {
                let peer_address = self
                    .stream
                    .peer_addr()
                    .map(|addr| addr.ip().to_string())
                    .unwrap_or_default();
                PlainServer::start(
                    PlainServerConfig {
                        socket_type: config.socket_type,
                        identity: config.identity.clone(),
                        domain: domain.clone(),
                        peer_address,
                    },
                    peer,
                )
                .map(Mechanism::PlainServer)
            }
        };
        mechanism.map_err(EngineError::Handshake)
    }

    /// Advances the handshake: takes an authenticator's answer, or handles
    /// the next command. Publishes the peer once established.
    fn receive_handshake(
        &mut self,
        mechanism: Mechanism,
        mut decoder: Decoder,
        pipes: &mut Pipes<'_>,
        cx: &mut Context<'_>,
    ) -> Result<bool, EngineError> {
        let step = match mechanism {
            // Nothing is read while the authenticator decides.
            Mechanism::Authenticating { pending, reply } => {
                let answer = match reply.try_recv() {
                    Ok(answer) => answer,
                    Err(mpsc::TryRecvError::Empty) => {
                        self.state = State::Handshake {
                            mechanism: Mechanism::Authenticating { pending, reply },
                            decoder,
                        };
                        return Ok(false);
                    }
                    // Dropped unanswered: an internal error, as ZAP's 500.
                    Err(mpsc::TryRecvError::Disconnected) => ZapReply::InternalError,
                };
                let server = pending.reply(answer, &mut self.out);
                Step::Continue(Mechanism::PlainServer(self.handshake_result(server)?))
            }
            mechanism => match decoder.decode().map_err(EngineError::Decode)? {
                Some(item) => self.handshake_step(mechanism, item, cx)?,
                None => {
                    let read = self.fill(&mut decoder);
                    self.state = State::Handshake { mechanism, decoder };
                    return read;
                }
            },
        };
        match step {
            Step::Continue(mechanism) => {
                self.state = State::Handshake { mechanism, decoder };
            }
            Step::Established(established) => {
                let version = established.version;
                self.deadlines.handshake = None;
                // ZMTP 1.0 and 2.0 have no commands, so no heartbeats.
                if matches!(version, Version::V3_0 | Version::V3_1) {
                    self.deadlines.next_ping = self
                        .config
                        .heartbeat
                        .map(|heartbeat| Instant::now() + heartbeat.interval);
                }
                // Published before any message from this peer can be
                // delivered, so a ROUTER always knows who sent it.
                *pipes.peer.lock().unwrap_or_else(PoisonError::into_inner) = Some(established);
                let encoder = Encoder::new(version);
                for topic in pipes.subscriptions.iter() {
                    let topic = zmtp::Frame::copy_from_slice(topic);
                    self.encode_subscription(encoder, &Subscription::Subscribe(topic));
                }
                self.state = State::Active {
                    decoder,
                    encoder,
                    pending: None,
                };
            }
        }
        Ok(true)
    }

    /// Hands one decoded item to the mechanism.
    fn handshake_step(
        &mut self,
        mechanism: Mechanism,
        item: Incoming,
        cx: &mut Context<'_>,
    ) -> Result<Step, EngineError> {
        Ok(match mechanism {
            Mechanism::Null(handshake) => {
                let result = handshake.receive(item, &mut self.out);
                Step::Established(self.handshake_result(result)?)
            }
            Mechanism::PlainClient(client) => {
                let result = client.receive(item, &mut self.out);
                match self.handshake_result(result)? {
                    ClientStep::Continue(client) => Step::Continue(Mechanism::PlainClient(client)),
                    ClientStep::Established(established) => Step::Established(established),
                }
            }
            Mechanism::PlainServer(server) => {
                let result = server.receive(item, &mut self.out);
                match self.handshake_result(result)? {
                    ServerStep::Authenticate(pending, request) => {
                        let (answer, reply) = mpsc::channel();
                        let respond = ZapResponder::new(answer, cx.waker().clone());
                        if let Security::PlainServer { authenticator, .. } = &self.config.security {
                            authenticator.authenticate(request, respond);
                        }
                        // Without an authenticator the responder is dropped
                        // unanswered, rejecting the client.
                        Step::Continue(Mechanism::Authenticating { pending, reply })
                    }
                    ServerStep::Established(established) => Step::Established(established),
                }
            }
            // Handled before decoding; nothing is read while waiting.
            Mechanism::Authenticating { pending, reply } => {
                Step::Continue(Mechanism::Authenticating { pending, reply })
            }
        })
    }

    /// Passes a successful handshake step through. On failure, tries to
    /// deliver any ERROR written to the output before reporting it.
    fn handshake_result<T>(&mut self, result: Result<T, HandshakeError>) -> Result<T, EngineError> {
        result.map_err(|err| {
            let _ = self.flush();
            EngineError::Handshake(err)
        })
    }

    /// Hands decoded messages to the socket until input runs out or the
    /// receive pipe is full.
    fn receive_active(
        &mut self,
        decoder: &mut Decoder,
        encoder: Encoder,
        pending: &mut Option<Message>,
        pipes: &mut Pipes<'_>,
        cx: &mut Context<'_>,
    ) -> Result<bool, EngineError> {
        let mut progressed = false;
        if let Some(message) = pending.take() {
            if let Some(refused) = deliver(pipes.inbound, message, cx)? {
                *pending = Some(refused);
                return Ok(false);
            }
            progressed = true;
        }
        loop {
            let item = decoder.decode().map_err(EngineError::Decode)?;
            if item.is_some() {
                self.deadlines.heard_from_peer();
            }
            match item {
                Some(Incoming::Message(message)) => {
                    if let Some(refused) = deliver(pipes.inbound, message, cx)? {
                        *pending = Some(refused);
                        return Ok(progressed);
                    }
                    progressed = true;
                }
                Some(Incoming::Command(body)) => {
                    if let Some(message) = self.handle_command(&body, encoder)?
                        && let Some(refused) = deliver(pipes.inbound, message, cx)?
                    {
                        *pending = Some(refused);
                        return Ok(true);
                    }
                    progressed = true;
                }
                None => {
                    if !self.fill(decoder)? {
                        return Ok(progressed);
                    }
                    progressed = true;
                }
            }
        }
    }

    /// Answers heartbeats. Returns a subscription for a PUB or XPUB socket
    /// to read, in the message form; other commands are ignored.
    fn handle_command(
        &mut self,
        body: &[u8],
        encoder: Encoder,
    ) -> Result<Option<Message>, EngineError> {
        let command = Command::parse(body)
            .map_err(|err| EngineError::Handshake(HandshakeError::MalformedCommand(err)))?;
        if let Command::Subscribe(_) | Command::Cancel(_) = command {
            let publishes = matches!(self.config.socket_type, SocketType::Pub | SocketType::XPub);
            return Ok(Subscription::from_command(command)
                .filter(|_| publishes)
                .map(|subscription| subscription.to_message()));
        }
        if let Command::Ping(ping) = command {
            if ping.ttl != Ttl::ZERO {
                self.deadlines
                    .ttl
                    .get_or_insert(Instant::now() + ping.ttl.as_duration());
            }
            let mut pong = Vec::new();
            Command::Pong(ping.context).encode(&mut pong);
            let _ = encoder.encode_command(&pong, &mut self.out);
        }
        Ok(None)
    }

    /// The subscription `message` carries, if this is a SUB or XSUB socket
    /// and it is one.
    fn outgoing_subscription(&self, message: &Message) -> Option<Subscription> {
        matches!(self.config.socket_type, SocketType::Sub | SocketType::XSub)
            .then(|| Subscription::from_message(message))
            .flatten()
    }

    /// Writes `subscription` as ZMTP 3.1 does, as a command, or in the
    /// message form older peers expect.
    fn encode_subscription(&mut self, encoder: Encoder, subscription: &Subscription) {
        if encoder.version() == Version::V3_1 {
            let mut body = Vec::new();
            subscription.to_command().encode(&mut body);
            let _ = encoder.encode_command(&body, &mut self.out);
        } else {
            encoder.encode(&subscription.to_message(), &mut self.out);
        }
    }

    /// Reads from the stream into `decoder`. Returns whether anything arrived.
    fn fill(&mut self, decoder: &mut Decoder) -> Result<bool, EngineError> {
        match decoder.fill(|buf| self.stream.read(buf)) {
            Ok(0) => Err(EngineError::PeerClosed),
            Ok(_) => Ok(true),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(false),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => Ok(true),
            Err(err) => Err(EngineError::Io(err)),
        }
    }
}

/// Records a subscription sent on the session, for replay. Topics are a
/// set here: a subscriber cancels only its last subscription to a topic.
fn remember(subscriptions: &mut Topics, subscription: &Subscription) {
    match subscription {
        Subscription::Subscribe(topic) => {
            if !subscriptions.contains(topic) {
                subscriptions.add(topic);
            }
        }
        Subscription::Cancel(topic) => {
            subscriptions.remove(topic);
        }
    }
}

/// Offers `message` to the socket. Returns it if the pipe is full, in which
/// case the pipe's waker will bring the engine back. If the socket dropped
/// its receiver the message is discarded, so lingering sends carry on.
fn deliver(
    inbound: &mut Writer<Message>,
    message: Message,
    cx: &mut Context<'_>,
) -> Result<Option<Message>, EngineError> {
    match inbound.poll_ready(cx) {
        Poll::Ready(Ok(())) => match inbound.try_send(message) {
            Ok(()) | Err(TrySendError::Closed(_)) => Ok(None),
            Err(TrySendError::Full(message)) => Ok(Some(message)),
        },
        Poll::Ready(Err(())) => Ok(None),
        Poll::Pending => Ok(Some(message)),
    }
}
