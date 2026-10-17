//! A blocking ZMTP peer built only from `zmtp`, for talking to libzmq.

use std::fmt;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use zmtp::{
    Command, DecodeError, Decoder, DecoderConfig, Encoder, Established, Greeting, GreetingConfig,
    GreetingError, HandshakeError, Identity, Incoming, LegacyPeers, Mechanism, Message,
    NullHandshake, SocketType,
};

/// A connection that has completed the greeting and NULL handshake.
#[derive(Debug)]
pub struct Peer {
    stream: TcpStream,
    encoder: Encoder,
    decoder: Decoder,
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
        Self::connect_with(
            addr,
            socket_type,
            identity,
            Mechanism::Null {
                legacy_peers: LegacyPeers::Allow,
            },
        )
    }

    /// Connects to `addr` announcing `mechanism`. Only NULL can complete the
    /// handshake; other mechanisms exercise the greeting.
    pub fn connect_with(
        addr: SocketAddr,
        socket_type: SocketType,
        identity: Identity,
        mechanism: Mechanism,
    ) -> Result<Self, ConnectError> {
        let mut stream = TcpStream::connect(addr).map_err(ConnectError::Io)?;
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(ConnectError::Io)?;

        let mut greeting = Greeting::new(GreetingConfig {
            socket_type,
            identity: identity.clone(),
            mechanism,
        });
        let mut out = Vec::new();
        let mut buf = [0u8; 64];
        let (peer, leftover) = loop {
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
                break (peer, &buf[feed.consumed..n]);
            }
        };

        let version = peer.version();
        let handshake = NullHandshake::start(socket_type, &identity, &peer, &mut out)
            .map_err(ConnectError::Handshake)?;
        stream.write_all(&out).map_err(ConnectError::Io)?;
        out.clear();

        let mut decoder = Decoder::new(version, DecoderConfig::default());
        decoder.extend_from_slice(leftover);
        let first = next(&mut decoder, &mut stream)?;
        let result = handshake.receive(first, &mut out);
        // Deliver any ERROR before reporting the failure.
        stream.write_all(&out).map_err(ConnectError::Io)?;
        let established = result.map_err(ConnectError::Handshake)?;
        Ok(Self {
            stream,
            encoder: Encoder::new(version),
            decoder,
            established,
        })
    }

    /// Sends a message.
    pub fn send(&mut self, message: &Message) -> io::Result<()> {
        let mut out = Vec::new();
        self.encoder.encode(message, &mut out);
        self.stream.write_all(&out)
    }

    /// Sends a ZMTP 3.x command.
    pub fn send_command(&mut self, command: &Command) -> io::Result<()> {
        let mut body = Vec::new();
        command.encode(&mut body);
        let mut out = Vec::new();
        self.encoder
            .encode_command(&body, &mut out)
            .map_err(io::Error::other)?;
        self.stream.write_all(&out)
    }

    /// Receives the next message or command.
    pub fn recv(&mut self) -> Result<Incoming, ConnectError> {
        next(&mut self.decoder, &mut self.stream)
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

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O: {err}"),
            Self::Greeting(err) => write!(f, "greeting: {err}"),
            Self::Handshake(err) => write!(f, "handshake: {err}"),
            Self::Decode(err) => write!(f, "decode: {err}"),
            Self::Closed => f.write_str("peer closed the connection"),
        }
    }
}

impl std::error::Error for ConnectError {}
