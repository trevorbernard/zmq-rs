//! One TCP connection: greeting, NULL handshake, then framed messages.
//!
//! The engine runs on an I/O thread and moves messages between the TCP
//! stream and the session's pipes. mio is edge-triggered, so every read and
//! write continues until the OS reports `WouldBlock`; when a full receive
//! pipe stops reading early, the pipe's waker brings the engine back.

use std::io::{self, Read, Write};
use std::mem;
use std::task::{Context, Poll};

use mio::net::TcpStream;
use zmq_pipe::{Reader, TrySendError, Writer};
use zmtp::{
    Command, DecodeError, Decoder, DecoderConfig, Encoder, Greeting, GreetingConfig, GreetingError,
    HandshakeError, Incoming, LegacyPeers, Mechanism, Message, NullHandshake,
};

use crate::ConnectionConfig;

/// Stop taking messages from the socket while this much output is unsent.
const MAX_PENDING_OUTPUT: usize = 64 * 1024;

pub(crate) struct Engine {
    stream: TcpStream,
    config: ConnectionConfig,
    connected: bool,
    state: State,
    out: Vec<u8>,
    out_pos: usize,
}

enum State {
    Greeting(Greeting),
    Handshake {
        handshake: NullHandshake,
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
    /// The socket dropped its end of a pipe.
    SocketClosed,
}

/// The session's pipe ends, lent to the engine while it runs.
pub(crate) struct Pipes<'a> {
    /// Towards the socket.
    pub(crate) inbound: &'a mut Writer<Message>,
    /// From the socket.
    pub(crate) outbound: &'a mut Reader<Message>,
}

impl Engine {
    pub(crate) fn new(stream: TcpStream, config: ConnectionConfig) -> Self {
        let mut greeting = Greeting::new(GreetingConfig {
            socket_type: config.socket_type,
            identity: config.identity.clone(),
            mechanism: Mechanism::Null {
                legacy_peers: LegacyPeers::Allow,
            },
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
        }
    }

    pub(crate) fn stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    /// Whether the handshake has completed and messages are flowing.
    pub(crate) fn is_established(&self) -> bool {
        matches!(self.state, State::Active { .. })
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
                    encoder.encode(&message, &mut self.out);
                    progressed = true;
                }
                Poll::Ready(None) => return Err(EngineError::SocketClosed),
                Poll::Pending => break,
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
            State::Handshake {
                handshake,
                mut decoder,
            } => match decoder.decode().map_err(EngineError::Decode)? {
                Some(item) => {
                    let result = handshake.receive(item, &mut self.out);
                    let established = match result {
                        Ok(established) => established,
                        Err(err) => {
                            // Try to deliver any ERROR before closing.
                            let _ = self.flush();
                            return Err(EngineError::Handshake(err));
                        }
                    };
                    self.state = State::Active {
                        decoder,
                        encoder: Encoder::new(established.version),
                        pending: None,
                    };
                    Ok(true)
                }
                None => {
                    let read = self.fill(&mut decoder);
                    self.state = State::Handshake { handshake, decoder };
                    read
                }
            },
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
        let handshake = NullHandshake::start(
            self.config.socket_type,
            &self.config.identity,
            &peer,
            &mut self.out,
        )
        .map_err(EngineError::Handshake)?;
        let mut decoder = Decoder::new(
            peer.version(),
            DecoderConfig {
                max_frame_size: self.config.max_frame_size,
            },
        );
        decoder.extend_from_slice(&buf[feed.consumed..n]);
        self.state = State::Handshake { handshake, decoder };
        Ok(true)
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
            match decoder.decode().map_err(EngineError::Decode)? {
                Some(Incoming::Message(message)) => {
                    if let Some(refused) = deliver(pipes.inbound, message, cx)? {
                        *pending = Some(refused);
                        return Ok(progressed);
                    }
                    progressed = true;
                }
                Some(Incoming::Command(body)) => {
                    self.handle_command(&body, encoder)?;
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

    fn handle_command(&mut self, body: &[u8], encoder: Encoder) -> Result<(), EngineError> {
        let command = Command::parse(body)
            .map_err(|err| EngineError::Handshake(HandshakeError::MalformedCommand(err)))?;
        // Answer heartbeats; subscriptions and the rest belong to patterns.
        if let Command::Ping(ping) = command {
            let mut pong = Vec::new();
            Command::Pong(ping.context).encode(&mut pong);
            let _ = encoder.encode_command(&pong, &mut self.out);
        }
        Ok(())
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

/// Offers `message` to the socket. Returns it if the pipe is full, in which
/// case the pipe's waker will bring the engine back.
fn deliver(
    inbound: &mut Writer<Message>,
    message: Message,
    cx: &mut Context<'_>,
) -> Result<Option<Message>, EngineError> {
    match inbound.poll_ready(cx) {
        Poll::Ready(Ok(())) => match inbound.try_send(message) {
            Ok(()) => Ok(None),
            Err(TrySendError::Full(message)) => Ok(Some(message)),
            Err(TrySendError::Closed(_)) => Err(EngineError::SocketClosed),
        },
        Poll::Ready(Err(())) => Err(EngineError::SocketClosed),
        Poll::Pending => Ok(Some(message)),
    }
}
