//! One stream connection, TCP or IPC: greeting, security handshake, then
//! framed messages.
//!
//! The engine runs on an I/O thread and moves messages between the stream
//! and the session's pipes. mio is edge-triggered, so every read and
//! write continues until the OS reports `WouldBlock`; when a full receive
//! pipe stops reading early, the pipe's waker brings the engine back.

use std::io::{self, Read};
use std::mem;
use std::sync::{PoisonError, mpsc};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use zmq_pipe::{Reader, TrySendError, Writer};
use zmtp::{
    ClientStep, Command, CurveClient, CurveClientStep, CurveCodec, CurveError, CurveServer,
    CurveServerStep, DecodeError, Decoder, DecoderConfig, Encoder, Established, Frame, Greeting,
    GreetingConfig, GreetingError, Group, HandshakeError, Incoming, LegacyPeers, Membership,
    Message, MessageMetadata, NullHandshake, NullZapConfig, PeerGreeting, PendingAuthentication,
    PendingCurveAuthentication, PendingNullAuthentication, Ping, PingContext, PlainClient,
    PlainServer, ReadInto, Role, ServerConfig, ServerStep, SocketType, Subscription, Ttl, Version,
    ZapFailure, ZapReply, ZapRequest,
};

use crate::context::PeerSlot;
use crate::output::Output;
use crate::transport::Stream;
use crate::{
    Authenticator, ConnectionConfig, HandshakeFailure, ProtocolError, Security, Topics,
    ZapResponder,
};

/// The shortest PING interval, so a zero interval cannot spin.
const MIN_PING_INTERVAL: Duration = Duration::from_millis(1);

/// Stop taking messages from the socket while this much output is unsent.
const MAX_PENDING_OUTPUT: usize = 64 * 1024;

/// Aligned to a cache line (128 octets, as `zmq-pipe` pads): where its
/// fields fell across lines moved `multipart/2` by 15% on the M1 as
/// `ConnectionConfig` grew.
#[repr(align(128))]
pub(crate) struct Engine {
    stream: Stream,
    config: ConnectionConfig,
    connected: bool,
    state: State,
    out: Output,
    /// The last look at the socket's pipe found it empty or closed.
    outbound_empty: bool,
    deadlines: Deadlines,
    /// Boxes and unboxes every frame once a CURVE handshake is done.
    curve: Option<CurveCodec>,
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
        /// What `zmq_msg_gets` reads on every message from this peer, until
        /// attached to the first: the socket's [`Inbound`] gives it to the
        /// rest. Empty if the connection has none, so that is known too.
        ///
        /// [`Inbound`]: crate::Inbound
        metadata: Option<MessageMetadata>,
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
pub(crate) enum EngineError {
    #[expect(
        dead_code,
        reason = "monitors learn of a broken connection, as libzmq's, without the cause"
    )]
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
}

impl EngineError {
    /// What a monitor is told when this ends a handshake, as libzmq tells
    /// it: authentication with a ZAP status, a protocol error, or no
    /// detail for a broken connection or a timeout.
    pub(crate) fn handshake_failure(&self) -> HandshakeFailure {
        // A client learns why from the server's ERROR, whose reason is the
        // ZAP status when authentication refused it.
        if let Self::Handshake(HandshakeError::PeerError(reason)) = self
            && let Some(status) = zap_status(reason.as_slice())
        {
            return HandshakeFailure::Authentication { status };
        }
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
            Self::Handshake(err) => {
                HandshakeFailure::Protocol(ProtocolError::Handshake(err.clone()))
            }
            Self::Greeting(err) => HandshakeFailure::Protocol(ProtocolError::Greeting(err.clone())),
            Self::Decode(err) => HandshakeFailure::Protocol(ProtocolError::Decode(err.clone())),
            Self::Curve(err) => HandshakeFailure::Protocol(ProtocolError::Curve(*err)),
            Self::Io(_) | Self::PeerClosed | Self::Timeout => HandshakeFailure::NoDetail,
        }
    }

    /// Whether the connection broke, rather than the peer misbehaving.
    /// libzmq reconnects after the former but not the latter.
    pub(crate) fn is_connection_error(&self) -> bool {
        matches!(self, Self::Io(_) | Self::PeerClosed | Self::Timeout)
    }
}

/// The ZAP status an ERROR reason carries, if it is one (RFC 27).
fn zap_status(reason: &[u8]) -> Option<u16> {
    match reason {
        b"300" => Some(300),
        b"400" => Some(400),
        b"500" => Some(500),
        _ => None,
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
        let mut out = Output::default();
        greeting.write_output(&mut out.buf);
        Self {
            stream,
            config,
            connected: false,
            state: State::Greeting(greeting),
            out,
            outbound_empty: false,
            deadlines: Deadlines::default(),
            curve: None,
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

    /// Writes `message`, boxed if CURVE is in use. Without CURVE, a large
    /// frame is sent from its own buffer, not copied.
    #[inline]
    fn write_message(&mut self, encoder: Encoder, message: Message) -> Result<(), EngineError> {
        if let Some(codec) = &mut self.curve {
            return codec
                .encode_message(&message, encoder, &mut self.out.buf)
                .map_err(EngineError::Curve);
        }
        self.out.encode(encoder, message);
        Ok(())
    }

    /// Writes a command, boxed if CURVE is in use. Returns false for ZMTP
    /// 1.0 and 2.0 peers, which have no commands.
    fn write_command(&mut self, encoder: Encoder, body: &[u8]) -> Result<bool, EngineError> {
        match &mut self.curve {
            Some(codec) => codec
                .encode_command(body, encoder, &mut self.out.buf)
                .map(|()| true)
                .map_err(EngineError::Curve),
            None => Ok(encoder.encode_command(body, &mut self.out.buf).is_ok()),
        }
    }

    /// Whether the handshake has completed and messages are flowing.
    pub(crate) fn is_established(&self) -> bool {
        matches!(self.state, State::Active { .. })
    }

    /// Whether everything the socket queued has been written to the stream:
    /// the pipe was empty at the last look and no output is pending.
    pub(crate) fn caught_up(&self) -> bool {
        self.outbound_empty && self.flushed()
    }

    /// Whether everything taken from the socket has been written to the
    /// stream.
    pub(crate) fn flushed(&self) -> bool {
        self.is_established() && self.out.is_empty()
    }

    /// Does one round of work: writing, taking messages from the socket,
    /// reading, and handing messages to the socket. Returns whether it got
    /// anything done, in which case there may be more: the caller drives it
    /// again after its other sessions, as libzmq's engines return to the
    /// poller after each batch. Draining one connection at a time held the
    /// I/O thread on one socket's messages while that socket's thread made
    /// more, so their allocator contended for the same arena lock.
    pub(crate) fn drive(
        &mut self,
        pipes: &mut Pipes<'_>,
        cx: &mut Context<'_>,
    ) -> Result<bool, EngineError> {
        if !self.connected && !self.finish_connect()? {
            return Ok(false);
        }
        let now = Instant::now();
        if self.deadlines.expired(now) {
            return Err(EngineError::Timeout);
        }
        self.maybe_ping(now)?;
        let mut wrote = self.flush()?;
        let pulled = self.pull_outbound(pipes, cx)?;
        if pulled {
            wrote |= self.flush()?;
        }
        let received = self.receive(pipes, cx)?;
        Ok(wrote || pulled || received)
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
        self.out.write_to(&mut self.stream).map_err(EngineError::Io)
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
        while self.out.len() < MAX_PENDING_OUTPUT {
            match pipes.outbound.poll_recv(cx) {
                Poll::Ready(Some(message)) => {
                    if let Some(subscription) = self.outgoing_subscription(&message) {
                        remember(pipes.subscriptions, &subscription);
                        self.encode_subscription(encoder, &subscription)?;
                    } else if let Some(change) = self.outgoing_membership(&message) {
                        remember_membership(pipes.subscriptions, &change);
                        self.encode_membership(encoder, &change)?;
                    } else {
                        self.write_message(encoder, message)?;
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
                mut metadata,
            } => {
                let result = self.receive_active(
                    &mut decoder,
                    encoder,
                    &mut pending,
                    &mut metadata,
                    pipes,
                    cx,
                );
                self.state = State::Active {
                    decoder,
                    encoder,
                    pending,
                    metadata,
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
        greeting.write_output(&mut self.out.buf);
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

    /// What the configured security asks about peers, if anything: a CURVE
    /// server may have no authenticator, and clients have none.
    fn authenticator(&self) -> Option<&dyn Authenticator> {
        match &self.config.security {
            Security::NullAuthenticated { authenticator, .. }
            | Security::PlainServer { authenticator, .. } => Some(&**authenticator),
            Security::CurveServer { authenticator, .. } => authenticator.as_deref(),
            Security::Null | Security::PlainClient(_) | Security::CurveClient { .. } => None,
        }
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
                    ask(&**authenticator, request, Pending::Null(pending), cx)
                })
            }
            Security::Null => NullHandshake::start(
                config.socket_type,
                &config.identity,
                peer,
                &mut self.out.buf,
            )
            .map(Mechanism::Null),
            Security::PlainClient(credentials) => PlainClient::start(
                config.socket_type,
                config.identity.clone(),
                credentials,
                peer,
                &mut self.out.buf,
            )
            .map(Mechanism::PlainClient),
            Security::PlainServer { domain, .. } => {
                let peer_address = self.stream.zap_address();
                PlainServer::start(
                    ServerConfig {
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
                &mut self.out.buf,
            )
            .map(|client| Mechanism::CurveClient(Box::new(client))),
            Security::CurveServer {
                keypair, domain, ..
            } => CurveServer::start(
                ServerConfig {
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
        pipes: &Pipes<'_>,
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
                        let handshake = pending.reply(answer, &mut self.out.buf);
                        Step::Continue(Mechanism::Null(self.handshake_result(handshake)?))
                    }
                    Pending::Plain(pending) => {
                        let server = pending.reply(answer, &mut self.out.buf);
                        Step::Continue(Mechanism::PlainServer(self.handshake_result(server)?))
                    }
                    Pending::Curve(pending) => {
                        let result = pending.reply(answer, &mut self.out.buf);
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
                let metadata = Some(connection_metadata(&self.stream, &established));
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
                // delivered, so a ROUTER always knows who sent it. Poisoning
                // is ignored: storing a value cannot be left half done.
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
                    metadata,
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
                let result = handshake.receive(item, &mut self.out.buf);
                Step::Established(self.handshake_result(result)?, None)
            }
            Mechanism::PlainClient(client) => {
                let result = client.receive(item, &mut self.out.buf);
                match self.handshake_result(result)? {
                    ClientStep::Continue(client) => Step::Continue(Mechanism::PlainClient(client)),
                    ClientStep::Established(established) => Step::Established(established, None),
                }
            }
            Mechanism::PlainServer(server) => {
                let result = server.receive(item, &mut self.out.buf);
                match self.handshake_result(result)? {
                    ServerStep::Authenticate(pending, request) => {
                        let authenticator = self
                            .authenticator()
                            .expect("a PLAIN server is configured with an authenticator");
                        Step::Continue(ask(authenticator, request, Pending::Plain(pending), cx))
                    }
                    ServerStep::Established(established) => Step::Established(established, None),
                }
            }
            Mechanism::CurveClient(client) => {
                let result = client.receive(item, &mut self.out.buf);
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
                let result = server.receive(item, &mut self.out.buf);
                match self.handshake_result(result)? {
                    CurveServerStep::Continue(server) => {
                        Step::Continue(Mechanism::CurveServer(Box::new(server)))
                    }
                    CurveServerStep::Authenticate(pending, request) => {
                        match self.authenticator() {
                            Some(authenticator) => Step::Continue(ask(
                                authenticator,
                                request,
                                Pending::Curve(Box::new(pending)),
                                cx,
                            )),
                            // No authenticator: any client that knows our
                            // key is welcome, and nobody vouched for it.
                            None => {
                                let accept = ZapReply::Ok {
                                    user_id: zmtp::Frame::new(),
                                    metadata: zmtp::Metadata::new(),
                                };
                                let result = pending.reply(accept, &mut self.out.buf);
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
        metadata: &mut Option<MessageMetadata>,
        pipes: &mut Pipes<'_>,
        cx: &mut Context<'_>,
    ) -> Result<bool, EngineError> {
        let mut progressed = false;
        if let Some(message) = pending.take() {
            if let Some(refused) = deliver(pipes.inbound, message, cx) {
                *pending = Some(refused);
                return Ok(false);
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
                Some(Incoming::Message(mut message)) => {
                    announce(&mut message, metadata, self.config.conflate_inbound);
                    if let Some(refused) = deliver(pipes.inbound, message, cx) {
                        *pending = Some(refused);
                        return Ok(progressed);
                    }
                    progressed = true;
                }
                Some(Incoming::Command(body)) => {
                    if let Some(mut message) = self.handle_command(&body, encoder)? {
                        announce(&mut message, metadata, self.config.conflate_inbound);
                        if let Some(refused) = deliver(pipes.inbound, message, cx) {
                            *pending = Some(refused);
                            return Ok(true);
                        }
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
            self.write_message(encoder, subscription.to_message())
        }
    }

    /// Reads from the stream into `decoder`. Returns whether anything arrived.
    fn fill(&mut self, decoder: &mut Decoder) -> Result<bool, EngineError> {
        let stream = &mut self.stream;
        match decoder.fill_into(|into| match into {
            ReadInto::Slice(buf) => stream.read(buf),
            ReadInto::Append(body, limit) => stream.read_appending(body, limit),
        }) {
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

/// Asks `authenticator` about `request`, the handshake waiting in `pending`
/// until the answer arrives and wakes the engine.
fn ask(
    authenticator: &dyn Authenticator,
    request: ZapRequest,
    pending: Pending,
    cx: &Context<'_>,
) -> Mechanism {
    let (answer, reply) = mpsc::channel();
    authenticator.authenticate(request, ZapResponder::new(answer, cx.waker().clone()));
    Mechanism::Authenticating { pending, reply }
}

/// Offers `message` to the socket. Returns it if the pipe is full, in which
/// case the pipe's waker will bring the engine back. If the socket dropped
/// its receiver the message is discarded, so lingering sends carry on.
fn deliver(
    inbound: &mut Writer<Message>,
    message: Message,
    cx: &mut Context<'_>,
) -> Option<Message> {
    match inbound.poll_ready(cx) {
        Poll::Ready(Ok(())) => match inbound.try_send(message) {
            Ok(()) | Err(TrySendError::Closed(_)) => None,
            Err(TrySendError::Full(message)) => Some(message),
        },
        Poll::Ready(Err(())) => None,
        Poll::Pending => Some(message),
    }
}

/// What `zmq_msg_gets` reads on every message from this connection, built
/// once as libzmq's `stream_engine_base_t` builds it: the peer's IP address
/// over TCP, then what ZAP returned, then the peer's READY properties, the
/// first of two with the same name kept. Empty if there is nothing, as
/// libzmq then attaches none.
fn connection_metadata(stream: &Stream, established: &Established) -> MessageMetadata {
    let mut properties = Vec::new();
    let address = stream.zap_address();
    if !address.is_empty() {
        properties.push((Frame::from("Peer-Address"), Frame::from(address)));
    }
    if let Some(authenticated) = &established.authenticated {
        properties.push((Frame::from("User-Id"), authenticated.user_id.clone()));
        for (name, value) in authenticated.metadata.iter() {
            properties.push((Frame::copy_from_slice(name.as_slice()), value.clone()));
        }
    }
    for (name, value) in established.metadata.iter() {
        properties.push((Frame::copy_from_slice(name.as_slice()), value.clone()));
    }
    MessageMetadata::new(properties)
}

/// Attaches the connection's metadata to its first message only, for the
/// socket's [`Inbound`](crate::Inbound) to give to the rest, or to every
/// message when conflating, as a conflated pipe may replace the first.
fn announce(message: &mut Message, metadata: &mut Option<MessageMetadata>, every: bool) {
    let attached = if every {
        metadata.clone()
    } else {
        metadata.take()
    };
    if attached.is_some() {
        message.set_metadata(attached);
    }
}
