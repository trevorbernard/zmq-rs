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

use zmq_log::{LogReader, LogWriter};
use zmq_pipe::{Reader, TrySendError, Writer};
use zmtp::{
    ClientStep, Command, CurveClient, CurveClientStep, CurveCodec, CurveError, CurveServer,
    CurveServerConfig, CurveServerStep, DecodeError, Decoder, DecoderConfig, Encoder, Established,
    FrameWalker, Greeting, GreetingConfig, GreetingError, Group, HandshakeError, InboundWalker,
    Incoming, LegacyPeers, Membership, Message, NullHandshake, NullZapConfig, PeerGreeting,
    PendingAuthentication, PendingCurveAuthentication, PendingNullAuthentication, Ping,
    PingContext, PlainClient, PlainServer, PlainServerConfig, Role, ServerStep, SocketType,
    Subscription, Ttl, Version, WalkUntil, ZapFailure, ZapReply,
};

use crate::context::PeerSlot;
use crate::inbound::SessionInbound;
use crate::outbound::{SessionOutbound, Streaming, encoded_len, poll_stream_into, write_message};
use crate::transport::Stream;
use crate::{ConnectionConfig, HandshakeFailure, ProtocolError, Security, Topics, ZapResponder};

/// The shortest PING interval, so a zero interval cannot spin.
const MIN_PING_INTERVAL: Duration = Duration::from_millis(1);

/// Stop taking messages from the socket while this much output is unsent.
const MAX_PENDING_OUTPUT: usize = 64 * 1024;

pub(crate) struct Engine {
    stream: Stream,
    config: ConnectionConfig,
    connected: bool,
    state: State,
    out: Vec<u8>,
    out_pos: usize,
    /// The last look at the socket's pipe found it empty or closed.
    outbound_empty: bool,
    deadlines: Deadlines,
    /// Boxes and unboxes every frame once a CURVE handshake is done.
    curve: Option<CurveCodec>,
    /// How this engine takes from a log, once it starts to.
    log: LogProgress,
}

/// An engine's progress through the socket's log.
#[derive(Default)]
struct LogProgress {
    /// Whether the engine has looked at the log yet.
    started: bool,
    /// Dropping what is left of a message an earlier connection was part
    /// way through sending.
    skipping: bool,
    /// The stream ends part way through a message, so commands must wait.
    mid_message: bool,
    /// Rebuilds messages for peers whose framing differs from the log's.
    decoder: Option<Decoder>,
    /// Reading straight into the receive log, past the handshake's decoder.
    direct: bool,
    /// Follows what was read into the receive log.
    walker: Option<InboundWalker>,
    /// Part of the message being received is already committed, so the
    /// socket must be told if it never finishes.
    partial_committed: bool,
    /// A decoded message larger than the receive log, part way into it.
    inbound_streaming: Option<Streaming>,
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
    CurveClient(Box<CurveClient>),
    CurveServer(Box<CurveServer>),
    /// A server waiting for its authenticator's answer.
    Authenticating {
        pending: Pending,
        reply: mpsc::Receiver<ZapReply>,
    },
}

/// A server handshake waiting for ZAP, by mechanism.
enum Pending {
    Null(PendingNullAuthentication),
    Plain(PendingAuthentication),
    Curve(Box<PendingCurveAuthentication>),
}

/// Where a handshake step leads.
enum Step {
    Continue(Mechanism),
    /// Done, with the codec every later frame goes through for CURVE.
    Established(Established, Option<CurveCodec>),
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
    /// A CURVE frame did not open, or arrived out of order.
    Curve(CurveError),
    /// The connection broke part way through a second message larger than
    /// the receive log before the socket had read past the first, so the
    /// log could not mark both; the session ends.
    InboundBacklog,
}

impl EngineError {
    /// What a monitor is told when this ends a handshake, as libzmq tells
    /// it: authentication with a ZAP status, a protocol error, or no
    /// detail for a broken connection or a timeout.
    pub(crate) fn handshake_failure(&self) -> HandshakeFailure {
        let protocol = |error| HandshakeFailure::Protocol(error);
        match self {
            Self::Handshake(HandshakeError::Authentication(failure)) => {
                HandshakeFailure::Authentication {
                    status: match failure {
                        ZapFailure::Temporary => 300,
                        ZapFailure::Denied => 400,
                        ZapFailure::Internal => 500,
                    },
                }
            }
            // A client learns why from the server's ERROR, whose reason is
            // the ZAP status when authentication refused it.
            Self::Handshake(HandshakeError::PeerError(reason))
                if matches!(reason.as_slice(), b"300" | b"400" | b"500") =>
            {
                let status = std::str::from_utf8(reason.as_slice())
                    .ok()
                    .and_then(|digits| digits.parse().ok())
                    .expect("three ASCII digits");
                HandshakeFailure::Authentication { status }
            }
            Self::Handshake(err) => protocol(ProtocolError::Handshake(err.clone())),
            Self::Greeting(err) => protocol(ProtocolError::Greeting(err.clone())),
            Self::Decode(err) => protocol(ProtocolError::Decode(err.clone())),
            Self::Curve(err) => protocol(ProtocolError::Curve(*err)),
            Self::Io(_) | Self::PeerClosed | Self::Timeout | Self::InboundBacklog => {
                HandshakeFailure::NoDetail
            }
        }
    }

    /// Whether the connection broke, rather than the peer misbehaving.
    /// libzmq reconnects after the former but not the latter.
    pub(crate) fn is_connection_error(&self) -> bool {
        matches!(self, Self::Io(_) | Self::PeerClosed | Self::Timeout)
    }
}

/// The session's pipe ends, lent to the engine while it runs.
pub(crate) struct Pipes<'a> {
    /// Towards the socket.
    pub(crate) inbound: &'a mut SessionInbound,
    /// From the socket.
    pub(crate) outbound: &'a mut SessionOutbound,
    /// Where the handshake's outcome goes, for the socket to read.
    pub(crate) peer: &'a PeerSlot,
    /// The topics a SUB or XSUB has subscribed to over this session's
    /// connections, replayed to each new one as libzmq does on a hiccup.
    pub(crate) subscriptions: &'a mut Topics,
}

impl Engine {
    pub(crate) fn new(stream: Stream, config: ConnectionConfig) -> Self {
        let mechanism = match &config.security {
            Security::Null => zmtp::Mechanism::Null {
                legacy_peers: LegacyPeers::Allow,
            },
            // libzmq refuses ZMTP 1.0 and 2.0 peers whenever ZAP is in use.
            Security::NullAuthenticated { .. } => zmtp::Mechanism::Null {
                legacy_peers: LegacyPeers::Refuse,
            },
            Security::PlainClient(_) => zmtp::Mechanism::Plain(Role::Client),
            Security::PlainServer { .. } => zmtp::Mechanism::Plain(Role::Server),
            Security::CurveClient { .. } => zmtp::Mechanism::Curve(Role::Client),
            Security::CurveServer { .. } => zmtp::Mechanism::Curve(Role::Server),
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
            curve: None,
            log: LogProgress::default(),
        }
    }

    /// Whether the stream has connected, for connections we make; accepted
    /// streams count once the engine first runs.
    pub(crate) fn is_connected(&self) -> bool {
        self.connected
    }

    pub(crate) fn stream(&mut self) -> &mut Stream {
        &mut self.stream
    }

    /// When the engine next needs driving even without I/O: a PING is due,
    /// or a deadline expires.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.earliest()
    }

    /// Sends a PING if one is due, and starts waiting for traffic.
    fn maybe_ping(&mut self, now: Instant) -> Result<(), EngineError> {
        let (Some(heartbeat), Some(due)) = (self.config.heartbeat, self.deadlines.next_ping) else {
            return Ok(());
        };
        if due > now {
            return Ok(());
        }
        let State::Active { encoder, .. } = &self.state else {
            return Ok(());
        };
        let encoder = *encoder;
        let ping = Command::Ping(Ping {
            ttl: Ttl::from_duration(heartbeat.ttl),
            context: PingContext::default(),
        });
        let mut body = Vec::new();
        ping.encode(&mut body);
        if self.write_command(encoder, &body)? {
            self.deadlines.next_ping = Some(now + heartbeat.interval.max(MIN_PING_INTERVAL));
            self.deadlines
                .ping_timeout
                .get_or_insert(now + heartbeat.timeout);
        }
        Ok(())
    }

    /// Writes `message`, boxed if CURVE is in use.
    fn write_message(&mut self, encoder: Encoder, message: &Message) -> Result<(), EngineError> {
        match &mut self.curve {
            Some(codec) => codec
                .encode_message(message, encoder, &mut self.out)
                .map_err(EngineError::Curve),
            None => {
                encoder.encode(message, &mut self.out);
                Ok(())
            }
        }
    }

    /// Writes a command, boxed if CURVE is in use. Returns false for ZMTP
    /// 1.0 and 2.0 peers, which have no commands.
    fn write_command(&mut self, encoder: Encoder, body: &[u8]) -> Result<bool, EngineError> {
        match &mut self.curve {
            Some(codec) => codec
                .encode_command(body, encoder, &mut self.out)
                .map(|()| true)
                .map_err(EngineError::Curve),
            None => Ok(encoder.encode_command(body, &mut self.out).is_ok()),
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
        let result = self.drive_until_idle(pipes, cx);
        match result {
            Err(err) => Err(self.abandon_inbound(pipes.inbound).unwrap_or(err)),
            ok => ok,
        }
    }

    /// Drops what the connection delivered of a message it did not finish:
    /// uncommitted octets, or a mark for the socket if part was committed.
    /// Returns the error that ends the session if the log cannot mark it.
    fn abandon_inbound(&mut self, inbound: &mut SessionInbound) -> Option<EngineError> {
        let SessionInbound::Log(writer) = inbound else {
            return None;
        };
        if !self.log.partial_committed {
            writer.abandon();
            return None;
        }
        writer
            .abandon_message()
            .err()
            .map(|zmq_log::MarkBusy| EngineError::InboundBacklog)
    }

    fn drive_until_idle(
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
        self.maybe_ping(now)?;
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
        match self.stream.finish_connect() {
            Ok(true) => {
                self.connected = true;
                // As libzmq, the handshake clock starts once connected.
                self.deadlines.handshake = self
                    .config
                    .handshake_timeout
                    .map(|timeout| Instant::now() + timeout);
                Ok(true)
            }
            Ok(false) => Ok(false),
            Err(err) => Err(EngineError::Io(err)),
        }
    }

    /// Writes pending output until done or the OS would block.
    fn flush(&mut self) -> Result<bool, EngineError> {
        // A command must not land between the frames of a message.
        if self.log.mid_message {
            return Ok(false);
        }
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

    /// Takes what the socket has queued: whole messages from a pipe, or
    /// encoded octets from a log.
    fn pull_outbound(
        &mut self,
        pipes: &mut Pipes<'_>,
        cx: &mut Context<'_>,
    ) -> Result<bool, EngineError> {
        let State::Active { encoder, .. } = &self.state else {
            return Ok(false);
        };
        let encoder = *encoder;
        match pipes.outbound {
            SessionOutbound::Pipe(reader) => {
                self.pull_pipe(encoder, reader, pipes.subscriptions, cx)
            }
            SessionOutbound::Log { reader, walker } => self.pull_log(encoder, reader, walker, cx),
        }
    }

    /// Writes the socket's log to the stream: as it is for ZMTP 2.0 and 3.x
    /// peers without CURVE, since the log holds their framing, and decoded
    /// and encoded again for the rest.
    fn pull_log(
        &mut self,
        encoder: Encoder,
        reader: &mut LogReader,
        walker: &mut FrameWalker,
        cx: &mut Context<'_>,
    ) -> Result<bool, EngineError> {
        if !self.log.started {
            self.log.started = true;
            // The rest of a message an earlier connection was part way
            // through is dropped, as libzmq drops what its engine held.
            self.log.skipping = !walker.at_message_boundary();
        }
        let as_is = self.curve.is_none() && encoder.version() != Version::V1;
        let mut progressed = false;
        loop {
            let [first, second] = reader.committed();
            if first.is_empty() {
                // Register, then look again so a commit in between is seen.
                match reader.poll_committed(cx) {
                    Poll::Ready(true) => continue,
                    Poll::Ready(false) | Poll::Pending => {
                        self.outbound_empty = true;
                        return Ok(progressed);
                    }
                }
            }
            self.outbound_empty = false;
            if self.log.skipping {
                let (walked, ended) = walker.walk(first, WalkUntil::MessageEnd);
                reader.release(walked, ended);
                self.log.skipping = ended == 0;
                progressed = true;
                continue;
            }
            if !as_is {
                if self.out.len() - self.out_pos >= MAX_PENDING_OUTPUT {
                    return Ok(progressed);
                }
                let decoder = self
                    .log
                    .decoder
                    .get_or_insert_with(|| Decoder::new(Version::V3_1, DecoderConfig::default()));
                decoder.extend_from_slice(first);
                decoder.extend_from_slice(second);
                let len = first.len() + second.len();
                let ended =
                    walker.walk(first, WalkUntil::End).1 + walker.walk(second, WalkUntil::End).1;
                reader.release(len, ended);
                let mut decoder = self.log.decoder.take().expect("decoder");
                while let Some(incoming) = decoder.decode().map_err(EngineError::Decode)? {
                    let Incoming::Message(message) = incoming else {
                        unreachable!("the log holds no commands");
                    };
                    self.write_message(encoder, &message)?;
                }
                self.log.decoder = Some(decoder);
                progressed = true;
                continue;
            }
            // Queued commands go out between messages: finish the message
            // under way, then send them.
            let mut limit = first.len() + second.len();
            if !self.out.is_empty() {
                if self.log.mid_message {
                    let mut probe = walker.clone();
                    let (walked, ended) = probe.walk(first, WalkUntil::MessageEnd);
                    limit = if ended > 0 {
                        walked
                    } else {
                        walked + probe.walk(second, WalkUntil::MessageEnd).0
                    };
                } else {
                    self.flush()?;
                    if !self.out.is_empty() {
                        return Ok(progressed);
                    }
                }
            }
            let (first, second) = (
                &first[..limit.min(first.len())],
                &second[..limit.saturating_sub(first.len())],
            );
            let written = match self
                .stream
                .write_vectored(&[io::IoSlice::new(first), io::IoSlice::new(second)])
            {
                Ok(0) => return Err(EngineError::Io(io::ErrorKind::WriteZero.into())),
                Ok(n) => n,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(progressed),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(EngineError::Io(err)),
            };
            let in_first = written.min(first.len());
            let mut ended = walker.walk(&first[..in_first], WalkUntil::End).1;
            if written > in_first {
                ended += walker.walk(&second[..written - in_first], WalkUntil::End).1;
            }
            reader.release(written, ended);
            self.log.mid_message = !walker.at_message_boundary();
            progressed = true;
        }
    }

    /// Encodes messages from the socket's pipe while output is not backed up.
    fn pull_pipe(
        &mut self,
        encoder: Encoder,
        reader: &mut Reader<Message>,
        subscriptions: &mut Topics,
        cx: &mut Context<'_>,
    ) -> Result<bool, EngineError> {
        let mut progressed = false;
        while self.out.len() - self.out_pos < MAX_PENDING_OUTPUT {
            match reader.poll_recv(cx) {
                Poll::Ready(Some(message)) => {
                    if let Some(subscription) = self.outgoing_subscription(&message) {
                        remember(subscriptions, &subscription);
                        self.encode_subscription(encoder, &subscription)?;
                    } else if let Some(change) = self.outgoing_membership(&message) {
                        remember_membership(subscriptions, &change);
                        self.encode_membership(encoder, &change)?;
                    } else {
                        self.write_message(encoder, &message)?;
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
            State::Greeting(greeting) => self.receive_greeting(greeting, cx),
            State::Handshake { mechanism, decoder } => {
                self.receive_handshake(mechanism, decoder, pipes, cx)
            }
            State::Active {
                mut decoder,
                encoder,
                mut pending,
            } => {
                // The log holds 2.0/3.x framing: a peer sending it without
                // CURVE is read straight in; the rest is decoded first.
                let direct = matches!(pipes.inbound, SessionInbound::Log(_))
                    && self.curve.is_none()
                    && encoder.version() != Version::V1;
                let result = if direct {
                    self.receive_direct(&mut decoder, encoder, &mut pending, pipes.inbound, cx)
                } else {
                    self.receive_active(&mut decoder, encoder, &mut pending, pipes.inbound, cx)
                };
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

    fn receive_greeting(
        &mut self,
        mut greeting: Greeting,
        cx: &mut Context<'_>,
    ) -> Result<bool, EngineError> {
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
        let mechanism = self.start_mechanism(&peer, cx)?;
        let mut decoder = Decoder::new(
            peer.version(),
            DecoderConfig {
                max_message_size: self.config.max_message_size,
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
    fn start_mechanism(
        &mut self,
        peer: &PeerGreeting,
        cx: &mut Context<'_>,
    ) -> Result<Mechanism, EngineError> {
        let config = &self.config;
        let mechanism = match &config.security {
            Security::NullAuthenticated {
                domain,
                authenticator,
            } => {
                let zap = NullZapConfig {
                    domain: domain.clone(),
                    peer_address: self.stream.zap_address(),
                };
                let started = NullHandshake::start_authenticated(
                    config.socket_type,
                    &config.identity,
                    zap,
                    peer,
                );
                // READY waits for the authenticator; nothing is read meanwhile.
                started.map(|(pending, request)| {
                    let (answer, reply) = mpsc::channel();
                    authenticator
                        .authenticate(request, ZapResponder::new(answer, cx.waker().clone()));
                    Mechanism::Authenticating {
                        pending: Pending::Null(pending),
                        reply,
                    }
                })
            }
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
                let peer_address = self.stream.zap_address();
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
            Security::CurveClient {
                keypair,
                server_key,
            } => CurveClient::start(
                config.socket_type,
                config.identity.clone(),
                keypair,
                *server_key,
                peer,
                &mut self.out,
            )
            .map(|client| Mechanism::CurveClient(Box::new(client))),
            Security::CurveServer {
                keypair, domain, ..
            } => CurveServer::start(
                CurveServerConfig {
                    socket_type: config.socket_type,
                    identity: config.identity.clone(),
                    domain: domain.clone(),
                    peer_address: self.stream.zap_address(),
                },
                keypair,
                peer,
            )
            .map(|server| Mechanism::CurveServer(Box::new(server))),
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
                match pending {
                    Pending::Null(pending) => {
                        let handshake = pending.reply(answer, &mut self.out);
                        Step::Continue(Mechanism::Null(self.handshake_result(handshake)?))
                    }
                    Pending::Plain(pending) => {
                        let server = pending.reply(answer, &mut self.out);
                        Step::Continue(Mechanism::PlainServer(self.handshake_result(server)?))
                    }
                    Pending::Curve(pending) => {
                        let result = pending.reply(answer, &mut self.out);
                        let (established, codec) = self.handshake_result(result)?;
                        Step::Established(established, Some(codec))
                    }
                }
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
            Step::Established(established, codec) => {
                let version = established.version;
                self.curve = codec;
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
                // A SUB's topics, or a DISH's groups.
                let dish = self.config.socket_type == SocketType::Dish;
                for topic in pipes.subscriptions.iter() {
                    let topic = zmtp::Frame::copy_from_slice(topic);
                    if dish {
                        let group = Group::new(topic).expect("remembered only valid groups");
                        self.encode_membership(encoder, &Membership::Join(group))?;
                    } else {
                        self.encode_subscription(encoder, &Subscription::Subscribe(topic))?;
                    }
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
                Step::Established(self.handshake_result(result)?, None)
            }
            Mechanism::PlainClient(client) => {
                let result = client.receive(item, &mut self.out);
                match self.handshake_result(result)? {
                    ClientStep::Continue(client) => Step::Continue(Mechanism::PlainClient(client)),
                    ClientStep::Established(established) => Step::Established(established, None),
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
                        Step::Continue(Mechanism::Authenticating {
                            pending: Pending::Plain(pending),
                            reply,
                        })
                    }
                    ServerStep::Established(established) => Step::Established(established, None),
                }
            }
            Mechanism::CurveClient(client) => {
                let result = client.receive(item, &mut self.out);
                match self.handshake_result(result)? {
                    CurveClientStep::Continue(client) => {
                        Step::Continue(Mechanism::CurveClient(Box::new(client)))
                    }
                    CurveClientStep::Established(established, codec) => {
                        Step::Established(established, Some(codec))
                    }
                }
            }
            Mechanism::CurveServer(server) => {
                let result = server.receive(item, &mut self.out);
                match self.handshake_result(result)? {
                    CurveServerStep::Continue(server) => {
                        Step::Continue(Mechanism::CurveServer(Box::new(server)))
                    }
                    CurveServerStep::Authenticate(pending, request) => {
                        let authenticator = match &self.config.security {
                            Security::CurveServer { authenticator, .. } => authenticator.clone(),
                            _ => None,
                        };
                        match authenticator {
                            Some(authenticator) => {
                                let (answer, reply) = mpsc::channel();
                                let respond = ZapResponder::new(answer, cx.waker().clone());
                                authenticator.authenticate(request, respond);
                                Step::Continue(Mechanism::Authenticating {
                                    pending: Pending::Curve(Box::new(pending)),
                                    reply,
                                })
                            }
                            // No authenticator: any client that knows our
                            // key is welcome, and nobody vouched for it.
                            None => {
                                let accept = ZapReply::Ok {
                                    user_id: zmtp::Frame::new(),
                                    metadata: zmtp::Metadata::new(),
                                };
                                let result = pending.reply(accept, &mut self.out);
                                let (mut established, codec) = self.handshake_result(result)?;
                                established.authenticated = None;
                                Step::Established(established, Some(codec))
                            }
                        }
                    }
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
        inbound: &mut SessionInbound,
        cx: &mut Context<'_>,
    ) -> Result<bool, EngineError> {
        if let SessionInbound::Log(writer) = inbound
            && !self.poll_inbound_streaming(writer, cx)
        {
            return Ok(false);
        }
        let mut progressed = false;
        if let Some(message) = pending.take() {
            if let Some(refused) = self.deliver(inbound, message, cx)? {
                *pending = Some(refused);
                return Ok(false);
            }
            if self.log.inbound_streaming.is_some() {
                return Ok(true);
            }
            progressed = true;
        }
        loop {
            let mut item = decoder.decode().map_err(EngineError::Decode)?;
            if item.is_some() {
                self.deadlines.heard_from_peer();
            }
            if let Some(codec) = &mut self.curve
                && let Some(boxed) = item.take()
            {
                match codec.decode(boxed).map_err(EngineError::Curve)? {
                    Some(unboxed) => item = Some(unboxed),
                    // Part of a multipart message: keep reading.
                    None => {
                        progressed = true;
                        continue;
                    }
                }
            }
            match item {
                Some(Incoming::Message(message)) => {
                    if let Some(refused) = self.deliver(inbound, message, cx)? {
                        *pending = Some(refused);
                        return Ok(progressed);
                    }
                    if self.log.inbound_streaming.is_some() {
                        return Ok(true);
                    }
                    progressed = true;
                }
                Some(Incoming::Command(body)) => {
                    if let Some(message) = self.handle_command(&body, encoder)?
                        && let Some(refused) = self.deliver(inbound, message, cx)?
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

    /// Offers `message` to the socket. Returns it if there is no room, in
    /// which case the socket's reading brings the engine back. If the socket
    /// dropped its receiver the message is discarded, so lingering sends
    /// carry on. A message larger than a receive log streams into it.
    fn deliver(
        &mut self,
        inbound: &mut SessionInbound,
        message: Message,
        cx: &mut Context<'_>,
    ) -> Result<Option<Message>, EngineError> {
        let writer = match inbound {
            SessionInbound::Pipe(writer) => return deliver_to_pipe(writer, message, cx),
            SessionInbound::Log(writer) => writer,
        };
        let len = encoded_len(&message);
        if len > writer.capacity() {
            self.log.inbound_streaming = Some(Streaming::new(message));
            self.log.partial_committed = true;
            self.poll_inbound_streaming(writer, cx);
            return Ok(None);
        }
        match writer.poll_writable(cx, len) {
            Poll::Ready(Ok(())) => {
                write_message(writer, &message);
                Ok(None)
            }
            Poll::Ready(Err(_)) => Ok(None),
            Poll::Pending => Ok(Some(message)),
        }
    }

    /// Continues a message streaming into the receive log. Whether it is
    /// done, or the socket is gone.
    fn poll_inbound_streaming(&mut self, writer: &mut LogWriter, cx: &mut Context<'_>) -> bool {
        let Some(streaming) = &mut self.log.inbound_streaming else {
            return true;
        };
        if poll_stream_into(writer, streaming, cx).is_pending() {
            return false;
        }
        self.log.inbound_streaming = None;
        self.log.partial_committed = false;
        true
    }

    /// Reads from the stream straight into the receive log, for a ZMTP 2.0
    /// or 3.x peer without CURVE. Only complete messages are committed,
    /// unless one grows past half the log; commands are answered as they
    /// are found. The handshake's decoder is emptied into the log first.
    fn receive_direct(
        &mut self,
        decoder: &mut Decoder,
        encoder: Encoder,
        pending: &mut Option<Message>,
        inbound: &mut SessionInbound,
        cx: &mut Context<'_>,
    ) -> Result<bool, EngineError> {
        let mut progressed = false;
        if !self.log.direct {
            // What the handshake's reads brought past it: deliver what decodes,
            // then hand the decoder's octets, between messages, to the log.
            loop {
                if pending.is_some() {
                    let (moved, done) = self.hand_over(decoder, encoder, pending, inbound, cx)?;
                    progressed |= moved;
                    if !done {
                        return Ok(progressed);
                    }
                    continue;
                }
                let SessionInbound::Log(writer) = inbound else {
                    unreachable!("direct reads need a log");
                };
                if writer.free() < decoder.buffered() {
                    if writer.poll_writable(cx, decoder.buffered()).is_pending() {
                        return Ok(progressed);
                    }
                    continue;
                }
                if let Some(octets) = decoder.take_buffered() {
                    writer.put(&octets);
                    self.log.direct = true;
                    self.take_in(writer, octets.len(), encoder)?;
                    progressed = true;
                    break;
                }
                let (moved, done) = self.hand_over(decoder, encoder, pending, inbound, cx)?;
                progressed |= moved;
                if !done {
                    return Ok(progressed);
                }
            }
        }
        let SessionInbound::Log(writer) = inbound else {
            unreachable!("direct reads need a log");
        };
        loop {
            if writer.is_closed() {
                // The socket is gone: answer heartbeats, drop the rest.
                let mut scratch = [0u8; 8192];
                let n = match self.read_into(&mut [io::IoSliceMut::new(&mut scratch)])? {
                    Some(n) => n,
                    None => return Ok(progressed),
                };
                let mut commands = Vec::new();
                let walker = self.inbound_walker(encoder);
                let walked = walker.walk(&scratch[..n], |body| commands.push(body.to_vec()));
                for body in commands {
                    self.handle_command(&body, encoder)?;
                }
                if let Some(err) = walked.error {
                    return Err(EngineError::Decode(err));
                }
                progressed = true;
                continue;
            }
            if !writer.has_room(1) {
                if writer.uncommitted() > 0 {
                    // A message fills the log: the socket must start on it.
                    writer.commit();
                    self.log.partial_committed = true;
                }
                match writer.poll_writable(cx, 1) {
                    Poll::Ready(_) => continue,
                    Poll::Pending => return Ok(progressed),
                }
            }
            let [first, second] = writer.spare();
            let read =
                self.read_into(&mut [io::IoSliceMut::new(first), io::IoSliceMut::new(second)])?;
            let Some(n) = read else {
                return Ok(progressed);
            };
            writer.advance(n);
            self.take_in(writer, n, encoder)?;
            progressed = true;
        }
    }

    /// Delivers the pending message, or decodes and delivers the next from
    /// the handshake's decoder, reading more if it needs it. Returns whether
    /// anything moved, and whether to carry on.
    fn hand_over(
        &mut self,
        decoder: &mut Decoder,
        encoder: Encoder,
        pending: &mut Option<Message>,
        inbound: &mut SessionInbound,
        cx: &mut Context<'_>,
    ) -> Result<(bool, bool), EngineError> {
        if let Some(message) = pending.take() {
            if let Some(refused) = self.deliver(inbound, message, cx)? {
                *pending = Some(refused);
                return Ok((false, false));
            }
            return Ok((true, true));
        }
        match decoder.decode().map_err(EngineError::Decode)? {
            Some(Incoming::Message(message)) => {
                self.deadlines.heard_from_peer();
                *pending = Some(message);
                Ok((true, true))
            }
            Some(Incoming::Command(body)) => {
                self.deadlines.heard_from_peer();
                self.handle_command(&body, encoder)?;
                Ok((true, true))
            }
            None => {
                let filled = self.fill(decoder)?;
                Ok((filled, filled))
            }
        }
    }

    /// Reads from the stream into `bufs`. `None` if the OS would block.
    fn read_into(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> Result<Option<usize>, EngineError> {
        loop {
            match self.stream.read_vectored(bufs) {
                Ok(0) => return Err(EngineError::PeerClosed),
                Ok(n) => {
                    self.deadlines.heard_from_peer();
                    return Ok(Some(n));
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => return Err(EngineError::Io(err)),
            }
        }
    }

    fn inbound_walker(&mut self, encoder: Encoder) -> &mut InboundWalker {
        let config = DecoderConfig {
            max_message_size: self.config.max_message_size,
        };
        self.log
            .walker
            .get_or_insert_with(|| InboundWalker::new(encoder.version(), config))
    }

    /// Walks the last `n` octets put into the receive log, answers the
    /// commands among them, and commits up to the last complete message.
    /// The rest of a message stays uncommitted unless it passes half the
    /// log, when it is committed for the socket to start decoding.
    fn take_in(
        &mut self,
        writer: &mut LogWriter,
        n: usize,
        encoder: Encoder,
    ) -> Result<(), EngineError> {
        let before = writer.uncommitted() - n;
        let mut commands = Vec::new();
        let walker = self.inbound_walker(encoder);
        let [first, second] = writer.uncommitted_tail(n);
        let mut walked = walker.walk(first, |body| commands.push(body.to_vec()));
        if walked.error.is_none() {
            let more = walker.walk(second, |body| commands.push(body.to_vec()));
            walked.messages += more.messages;
            if let Some(boundary) = more.last_boundary {
                walked.last_boundary = Some(first.len() + boundary);
            }
            walked.error = more.error;
        }
        let at_boundary = walker.at_boundary();
        for _ in 0..walked.messages {
            writer.end_message();
        }
        if at_boundary && walked.error.is_none() {
            writer.commit();
            self.log.partial_committed = false;
        } else {
            if let Some(boundary) = walked.last_boundary {
                writer.commit_prefix(before + boundary);
                self.log.partial_committed = false;
            }
            if writer.uncommitted() > writer.capacity() / 2 {
                writer.commit();
                self.log.partial_committed = true;
            }
        }
        for body in commands {
            self.handle_command(&body, encoder)?;
        }
        match walked.error {
            Some(err) => Err(EngineError::Decode(err)),
            None => Ok(()),
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
        if let Command::Join(_) | Command::Leave(_) = command {
            let radio = self.config.socket_type == SocketType::Radio;
            return Ok(Membership::from_command(command)
                .filter(|_| radio)
                .map(|change| change.to_message()));
        }
        if let Command::Ping(ping) = command {
            if ping.ttl != Ttl::ZERO {
                self.deadlines
                    .ttl
                    .get_or_insert(Instant::now() + ping.ttl.as_duration());
            }
            let mut pong = Vec::new();
            Command::Pong(ping.context).encode(&mut pong);
            self.write_command(encoder, &pong)?;
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

    /// The membership change `message` carries, if this is a DISH socket
    /// and it is one.
    fn outgoing_membership(&self, message: &Message) -> Option<Membership> {
        (self.config.socket_type == SocketType::Dish)
            .then(|| Membership::from_message(message))
            .flatten()
    }

    /// Writes a JOIN or LEAVE command, as libzmq's DISH does whatever the
    /// peer's ZMTP 3.x version.
    fn encode_membership(
        &mut self,
        encoder: Encoder,
        change: &Membership,
    ) -> Result<(), EngineError> {
        let mut body = Vec::new();
        change.to_command().encode(&mut body);
        self.write_command(encoder, &body).map(drop)
    }

    /// Writes `subscription` as ZMTP 3.1 does, as a command, or in the
    /// message form older peers expect.
    fn encode_subscription(
        &mut self,
        encoder: Encoder,
        subscription: &Subscription,
    ) -> Result<(), EngineError> {
        if encoder.version() == Version::V3_1 {
            let mut body = Vec::new();
            subscription.to_command().encode(&mut body);
            self.write_command(encoder, &body).map(drop)
        } else {
            self.write_message(encoder, &subscription.to_message())
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

/// Records a DISH's join or leave for replay, as subscriptions are.
fn remember_membership(groups: &mut Topics, change: &Membership) {
    match change {
        Membership::Join(group) => {
            if !groups.contains(group.as_slice()) {
                groups.add(group.as_slice());
            }
        }
        Membership::Leave(group) => {
            groups.remove(group.as_slice());
        }
    }
}

/// Offers `message` to the socket's pipe. Returns it if the pipe is full, in
/// which case the pipe's waker will bring the engine back. If the socket
/// dropped its receiver the message is discarded, so lingering sends carry
/// on.
fn deliver_to_pipe(
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
