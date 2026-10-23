//! A blocking ZMTP peer built only from `zmtp`, for talking to libzmq.

use std::fmt;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use zmtp::{
    ClientStep, Command, CurveClient, CurveClientStep, CurveCodec, CurveError, CurveServer,
    CurveServerConfig, CurveServerStep, DecodeError, Decoder, DecoderConfig, Encoder, Established,
    Frame, Greeting, GreetingConfig, GreetingError, HandshakeError, Identity, Incoming, Keypair,
    LegacyPeers, Mechanism, Message, NullHandshake, PeerGreeting, PlainClient, PlainCredentials,
    PlainServer, PlainServerConfig, PublicKey, Role, ServerStep, SocketType, ZapReply, ZapRequest,
};

/// Which security mechanism a [`Peer`] uses, and its side of it.
pub enum Security {
    Null,
    PlainClient(PlainCredentials),
    /// A PLAIN server that asks `authenticate` about each client.
    PlainServer {
        domain: Frame,
        authenticate: Box<dyn FnOnce(&ZapRequest) -> ZapReply + Send>,
    },
    /// A CURVE client with its long-term key pair, for the server whose
    /// public key is `server_key`.
    CurveClient {
        keypair: Keypair,
        server_key: PublicKey,
    },
    /// A CURVE server that asks `authenticate` about the client's key.
    CurveServer {
        keypair: Keypair,
        authenticate: Box<dyn FnOnce(&ZapRequest) -> ZapReply + Send>,
    },
}

/// A connection that has completed the greeting and security handshake.
#[derive(Debug)]
pub struct Peer {
    stream: TcpStream,
    encoder: Encoder,
    decoder: Decoder,
    /// Boxes and unboxes frames after a CURVE handshake.
    curve: Option<CurveCodec>,
    /// What the handshake established.
    pub established: Established,
}

/// Why connecting failed.
#[derive(Debug)]
pub enum ConnectError {
    Io(io::Error),
    Greeting(GreetingError),
    Handshake(HandshakeError),
    Decode(DecodeError),
    Curve(CurveError),
    /// The peer closed the connection.
    Closed,
}

impl Peer {
    /// Connects to `addr` as `socket_type` with the NULL mechanism.
    pub fn connect(
        addr: SocketAddr,
        socket_type: SocketType,
        identity: &[u8],
    ) -> Result<Self, ConnectError> {
        let identity = Identity::new(identity.to_vec()).expect("valid test identity");
        Self::connect_with(addr, socket_type, identity, Security::Null)
    }

    /// Connects to `addr` with `security`.
    pub fn connect_with(
        addr: SocketAddr,
        socket_type: SocketType,
        identity: Identity,
        security: Security,
    ) -> Result<Self, ConnectError> {
        let stream = TcpStream::connect(addr).map_err(ConnectError::Io)?;
        Self::establish(stream, socket_type, identity, security)
    }

    /// Accepts one connection on `listener` with `security`.
    pub fn accept(
        listener: &TcpListener,
        socket_type: SocketType,
        identity: Identity,
        security: Security,
    ) -> Result<Self, ConnectError> {
        let (stream, _) = listener.accept().map_err(ConnectError::Io)?;
        Self::establish(stream, socket_type, identity, security)
    }

    fn establish(
        mut stream: TcpStream,
        socket_type: SocketType,
        identity: Identity,
        security: Security,
    ) -> Result<Self, ConnectError> {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(ConnectError::Io)?;
        let mechanism = match &security {
            Security::Null => Mechanism::Null {
                legacy_peers: LegacyPeers::Allow,
            },
            Security::PlainClient(_) => Mechanism::Plain(Role::Client),
            Security::PlainServer { .. } => Mechanism::Plain(Role::Server),
            Security::CurveClient { .. } => Mechanism::Curve(Role::Client),
            Security::CurveServer { .. } => Mechanism::Curve(Role::Server),
        };
        let (peer, leftover) = greet(&mut stream, socket_type, &identity, mechanism)?;
        let version = peer.version();
        let mut decoder = Decoder::new(version, DecoderConfig::default());
        decoder.extend_from_slice(&leftover);
        let mut io = Io {
            stream,
            decoder,
            out: Vec::new(),
        };
        let mut curve = None;
        let established = match security {
            Security::Null => {
                let handshake = NullHandshake::start(socket_type, &identity, &peer, &mut io.out);
                let handshake = io.flush(handshake)?;
                let first = io.next()?;
                let result = handshake.receive(first, &mut io.out);
                io.flush(result)?
            }
            Security::PlainClient(credentials) => {
                let start =
                    PlainClient::start(socket_type, identity, &credentials, &peer, &mut io.out);
                let mut client = io.flush(start)?;
                loop {
                    let item = io.next()?;
                    let step = client.receive(item, &mut io.out);
                    match io.flush(step)? {
                        ClientStep::Continue(next) => client = next,
                        ClientStep::Established(established) => break established,
                    }
                }
            }
            Security::PlainServer {
                domain,
                authenticate,
            } => {
                let peer_address = io
                    .stream
                    .peer_addr()
                    .map_err(ConnectError::Io)?
                    .ip()
                    .to_string();
                let config = PlainServerConfig {
                    socket_type,
                    identity,
                    domain,
                    peer_address,
                };
                let server = PlainServer::start(config, &peer).map_err(ConnectError::Handshake)?;
                let hello = io.next()?;
                let step = server.receive(hello, &mut io.out);
                let ServerStep::Authenticate(pending, request) = io.flush(step)? else {
                    unreachable!("HELLO always leads to authentication");
                };
                let reply = pending.reply(authenticate(&request), &mut io.out);
                let server = io.flush(reply)?;
                let initiate = io.next()?;
                let step = server.receive(initiate, &mut io.out);
                let ServerStep::Established(established) = io.flush(step)? else {
                    unreachable!("INITIATE always completes the handshake");
                };
                established
            }
            Security::CurveClient {
                keypair,
                server_key,
            } => {
                let start = CurveClient::start(
                    socket_type,
                    identity,
                    &keypair,
                    server_key,
                    &peer,
                    &mut io.out,
                );
                let mut client = io.flush(start)?;
                loop {
                    let item = io.next()?;
                    let step = client.receive(item, &mut io.out);
                    match io.flush(step)? {
                        CurveClientStep::Continue(next) => client = next,
                        CurveClientStep::Established(established, codec) => {
                            curve = Some(codec);
                            break established;
                        }
                    }
                }
            }
            Security::CurveServer {
                keypair,
                authenticate,
            } => {
                let config = CurveServerConfig {
                    socket_type,
                    identity,
                    domain: Frame::new(),
                    peer_address: String::new(),
                };
                let mut server =
                    CurveServer::start(config, &keypair, &peer).map_err(ConnectError::Handshake)?;
                let (pending, request) = loop {
                    let item = io.next()?;
                    let step = server.receive(item, &mut io.out);
                    match io.flush(step)? {
                        CurveServerStep::Continue(next) => server = next,
                        CurveServerStep::Authenticate(pending, request) => {
                            break (pending, request);
                        }
                    }
                };
                let reply = pending.reply(authenticate(&request), &mut io.out);
                let (established, codec) = io.flush(reply)?;
                curve = Some(codec);
                established
            }
        };
        Ok(Self {
            stream: io.stream,
            encoder: Encoder::new(version),
            decoder: io.decoder,
            curve,
            established,
        })
    }

    /// Sends a message.
    pub fn send(&mut self, message: &Message) -> io::Result<()> {
        let mut out = Vec::new();
        match &mut self.curve {
            Some(codec) => codec
                .encode_message(message, self.encoder, &mut out)
                .map_err(io::Error::other)?,
            None => self.encoder.encode(message, &mut out),
        }
        self.stream.write_all(&out)
    }

    /// Sends `octets` as they are, such as part of an encoded message.
    pub fn send_raw(&mut self, octets: &[u8]) -> io::Result<()> {
        self.stream.write_all(octets)
    }

    /// Sends a ZMTP 3.x command.
    pub fn send_command(&mut self, command: &Command) -> io::Result<()> {
        let mut body = Vec::new();
        command.encode(&mut body);
        let mut out = Vec::new();
        match &mut self.curve {
            Some(codec) => codec
                .encode_command(&body, self.encoder, &mut out)
                .map_err(io::Error::other)?,
            None => self
                .encoder
                .encode_command(&body, &mut out)
                .map_err(io::Error::other)?,
        }
        self.stream.write_all(&out)
    }

    /// Receives the next message or command.
    pub fn recv(&mut self) -> Result<Incoming, ConnectError> {
        loop {
            let item = next(&mut self.decoder, &mut self.stream)?;
            let Some(codec) = &mut self.curve else {
                return Ok(item);
            };
            if let Some(item) = codec.decode(item).map_err(ConnectError::Curve)? {
                return Ok(item);
            }
        }
    }

    /// Receives the next message, skipping commands.
    pub fn recv_message(&mut self) -> Result<Message, ConnectError> {
        loop {
            if let Incoming::Message(message) = self.recv()? {
                return Ok(message);
            }
        }
    }
}

/// Runs the greeting, returning the peer's greeting and any octets read past it.
fn greet(
    stream: &mut TcpStream,
    socket_type: SocketType,
    identity: &Identity,
    mechanism: Mechanism,
) -> Result<(PeerGreeting, Vec<u8>), ConnectError> {
    let mut greeting = Greeting::new(GreetingConfig {
        socket_type,
        identity: identity.clone(),
        mechanism,
    });
    let mut out = Vec::new();
    let mut buf = [0u8; 64];
    loop {
        greeting.write_output(&mut out);
        stream.write_all(&out).map_err(ConnectError::Io)?;
        out.clear();
        let n = stream.read(&mut buf).map_err(ConnectError::Io)?;
        if n == 0 {
            return Err(ConnectError::Closed);
        }
        let feed = greeting.feed(&buf[..n]).map_err(ConnectError::Greeting)?;
        if let Some(peer) = feed.peer {
            greeting.write_output(&mut out);
            stream.write_all(&out).map_err(ConnectError::Io)?;
            return Ok((peer, buf[feed.consumed..n].to_vec()));
        }
    }
}

/// The stream, decoder and pending output during a handshake.
struct Io {
    stream: TcpStream,
    decoder: Decoder,
    out: Vec<u8>,
}

impl Io {
    /// Sends pending output, including any ERROR, then passes `result` on.
    fn flush<T>(&mut self, result: Result<T, HandshakeError>) -> Result<T, ConnectError> {
        self.stream.write_all(&self.out).map_err(ConnectError::Io)?;
        self.out.clear();
        result.map_err(ConnectError::Handshake)
    }

    fn next(&mut self) -> Result<Incoming, ConnectError> {
        next(&mut self.decoder, &mut self.stream)
    }
}

fn next(decoder: &mut Decoder, stream: &mut TcpStream) -> Result<Incoming, ConnectError> {
    loop {
        if let Some(item) = decoder.decode().map_err(ConnectError::Decode)? {
            return Ok(item);
        }
        let n = decoder
            .fill(|buf| stream.read(buf))
            .map_err(ConnectError::Io)?;
        if n == 0 {
            return Err(ConnectError::Closed);
        }
    }
}

impl fmt::Debug for Security {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("Null"),
            Self::PlainClient(credentials) => {
                f.debug_tuple("PlainClient").field(credentials).finish()
            }
            Self::PlainServer { domain, .. } => f
                .debug_struct("PlainServer")
                .field("domain", domain)
                .finish_non_exhaustive(),
            Self::CurveClient {
                keypair,
                server_key,
            } => f
                .debug_struct("CurveClient")
                .field("keypair", keypair)
                .field("server_key", server_key)
                .finish(),
            Self::CurveServer { keypair, .. } => f
                .debug_struct("CurveServer")
                .field("keypair", keypair)
                .finish_non_exhaustive(),
        }
    }
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O: {err}"),
            Self::Greeting(err) => write!(f, "greeting: {err}"),
            Self::Handshake(err) => write!(f, "handshake: {err}"),
            Self::Decode(err) => write!(f, "decode: {err}"),
            Self::Curve(err) => write!(f, "CURVE: {err}"),
            Self::Closed => f.write_str("peer closed the connection"),
        }
    }
}

impl std::error::Error for ConnectError {}
