//! The CURVE security mechanism (RFC 25, and CurveZMQ, RFC 26).
//!
//! ```text
//! client                                         server
//! HELLO C' Box[64 zeros](C'->S)              ->
//!                                            <-  WELCOME Box[S' cookie](S->C')
//! INITIATE cookie Box[C vouch metadata](C'->S') ->
//!                                            <-  READY Box[metadata](S'->C')
//! MESSAGE Box[flags frame](C'->S')           <-> MESSAGE Box[flags frame](S'->C')
//! ```
//!
//! C and S are the long-term keys, C' and S' transient keys made for this
//! connection. The server asks ZAP about C before sending READY. After the
//! handshake every frame, commands included, travels boxed in a MESSAGE
//! command: see [`CurveCodec`].
//!
//! NaCl's boxes put the 16-octet authenticator before the ciphertext, so
//! the detached AEAD APIs are used and the layout is `tag || ciphertext`.

use std::fmt;

use crypto_box::SalsaBox;
use crypto_box::aead::generic_array::GenericArray;
use crypto_box::aead::rand_core::RngCore;
use crypto_box::aead::{AeadInPlace, OsRng};
use crypto_secretbox::{KeyInit, XSalsa20Poly1305};
use zeroize::Zeroizing;

use crate::command::{encode_metadata, parse_metadata};
use crate::handshake::{
    Rejection, accept_peer, local_metadata, require_zmtp_3, write_raw_command, zap_verdict,
};
use crate::{
    Command, CommandError, Encoder, Established, Frame, HandshakeError, Identity, Incoming,
    Keypair, Message, Metadata, PeerGreeting, PublicKey, SocketType, Version, ZapCredentials,
    ZapReply, ZapRequest,
};

const TAG: usize = 16;
const KEY: usize = 32;
const COOKIE: usize = 16 + KEY + KEY + TAG;
const MORE: u8 = 0x01;
const COMMAND: u8 = 0x02;

const HELLO_PREFIX: &[u8; 16] = b"CurveZMQHELLO---";
const INITIATE_PREFIX: &[u8; 16] = b"CurveZMQINITIATE";
const READY_PREFIX: &[u8; 16] = b"CurveZMQREADY---";
const CLIENT_MESSAGE_PREFIX: &[u8; 16] = b"CurveZMQMESSAGEC";
const SERVER_MESSAGE_PREFIX: &[u8; 16] = b"CurveZMQMESSAGES";
const MESSAGE: &[u8] = b"\x07MESSAGE";

/// The client side of a CURVE handshake.
#[must_use]
pub struct CurveClient {
    socket_type: SocketType,
    identity: Identity,
    version: Version,
    server_key: PublicKey,
    public: PublicKey,
    secret: crypto_box::SecretKey,
    transient: crypto_box::SecretKey,
    state: ClientState,
}

enum ClientState {
    WaitingForWelcome,
    WaitingForReady { cipher: SalsaBox },
}

/// Where a client handshake goes next.
#[derive(Debug)]
#[must_use]
pub enum CurveClientStep {
    Continue(CurveClient),
    /// Done: exchange messages through the codec from now on.
    Established(Established, CurveCodec),
}

/// Configuration for a [`CurveServer`].
#[derive(Debug, Clone)]
pub struct CurveServerConfig {
    pub socket_type: SocketType,
    pub identity: Identity,
    /// The ZAP domain to authenticate in, possibly empty.
    pub domain: Frame,
    /// The peer's address, passed to ZAP.
    pub peer_address: String,
}

/// The server side of a CURVE handshake.
#[must_use]
pub struct CurveServer {
    config: CurveServerConfig,
    version: Version,
    public: PublicKey,
    secret: crypto_box::SecretKey,
    state: ServerState,
}

enum ServerState {
    WaitingForHello,
    WaitingForInitiate {
        client_transient: crypto_box::PublicKey,
        transient: crypto_box::SecretKey,
        cookie_key: Zeroizing<[u8; KEY]>,
    },
}

/// Where a server handshake goes next.
#[derive(Debug)]
#[must_use]
pub enum CurveServerStep {
    Continue(CurveServer),
    /// Ask the authenticator about `request`, whose credentials are the
    /// client's long-term public key, then call
    /// [`PendingCurveAuthentication::reply`].
    Authenticate(PendingCurveAuthentication, ZapRequest),
}

/// A server handshake waiting for its ZAP reply.
#[must_use]
pub struct PendingCurveAuthentication {
    config: CurveServerConfig,
    version: Version,
    metadata: Metadata,
    cipher: SalsaBox,
    peer_nonce: u64,
}

/// Boxes and unboxes frames once a CURVE handshake is done. Every frame
/// becomes a MESSAGE whose box holds a flags octet (MORE, and COMMAND for
/// boxed commands, as libzmq sends them) and the frame.
///
/// RFC 25 calls MESSAGE a command, but libzmq sends it as a single-part
/// data frame: its mechanism replaces the message, flags and all, before
/// framing. We send it the same way, and accept either form, since libzmq
/// would take a COMMAND-flagged MESSAGE's contents for a command.
pub struct CurveCodec {
    cipher: SalsaBox,
    send_prefix: &'static [u8; 16],
    receive_prefix: &'static [u8; 16],
    /// The short nonce of the next frame we send.
    next_nonce: u64,
    /// The peer's last short nonce: its next must be greater.
    peer_nonce: u64,
    /// Frames of a multipart message still arriving.
    partial: Vec<Frame>,
}

/// Why a frame could not be boxed or unboxed. The connection must close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurveError {
    /// The peer sent something other than a MESSAGE command.
    NotMessage,
    /// A MESSAGE too short to hold a nonce, an authenticator and flags, or
    /// a boxed command in the middle of a multipart message.
    Malformed,
    /// A short nonce not greater than the last, as a replay would have.
    StaleNonce,
    /// The box did not open: tampered with, or not for us.
    Cryptographic,
    /// We have sent 2^64 - 1 frames; RFC 26 forbids reusing a nonce.
    NonceExhausted,
}

impl CurveClient {
    /// Starts the handshake with the server whose long-term public key is
    /// `server_key`, writing HELLO to `out`.
    pub fn start(
        socket_type: SocketType,
        identity: Identity,
        keypair: &Keypair,
        server_key: PublicKey,
        peer: &PeerGreeting,
        out: &mut Vec<u8>,
    ) -> Result<Self, HandshakeError> {
        let version = require_zmtp_3(peer)?;
        let transient = crypto_box::SecretKey::generate(&mut OsRng);
        let cipher = SalsaBox::new(&box_key(&server_key), &transient);
        let signature = seal(&cipher, &short_nonce(HELLO_PREFIX, 1), &[0; 64]);
        let mut hello = Vec::with_capacity(194);
        // Version 1.0, then anti-amplification padding.
        hello.extend_from_slice(&[1, 0]);
        hello.extend_from_slice(&[0; 72]);
        hello.extend_from_slice(transient.public_key().as_bytes());
        hello.extend_from_slice(&1u64.to_be_bytes());
        hello.extend_from_slice(&signature);
        write_raw_command(version, "HELLO", &hello, out);
        Ok(Self {
            socket_type,
            identity,
            version,
            server_key,
            public: keypair.public(),
            secret: crypto_box::SecretKey::from_bytes(*keypair.secret().expose()),
            transient,
            state: ClientState::WaitingForWelcome,
        })
    }

    /// Handles the next item from the server.
    pub fn receive(
        mut self,
        incoming: Incoming,
        out: &mut Vec<u8>,
    ) -> Result<CurveClientStep, HandshakeError> {
        let body = command_body(incoming)?;
        let (name, data) = split_command(&body)?;
        match (&self.state, name) {
            (_, b"ERROR") => Err(peer_error(&body)),
            (ClientState::WaitingForWelcome, b"WELCOME") => {
                let cipher = self.welcome(data, out)?;
                self.state = ClientState::WaitingForReady { cipher };
                Ok(CurveClientStep::Continue(self))
            }
            (ClientState::WaitingForReady { .. }, b"READY") => self.ready(data, out),
            (_, name) => Err(HandshakeError::UnexpectedCommand(Frame::copy_from_slice(
                name,
            ))),
        }
    }

    /// Opens WELCOME and answers with INITIATE. Returns the box for the
    /// rest of the connection.
    fn welcome(&self, data: &[u8], out: &mut Vec<u8>) -> Result<SalsaBox, HandshakeError> {
        if data.len() != 16 + 144 {
            return Err(malformed("WELCOME"));
        }
        let (nonce, sealed) = data.split_at(16);
        let opened = open(
            &SalsaBox::new(&box_key(&self.server_key), &self.transient),
            &long_nonce(b"WELCOME-", nonce),
            sealed,
        )?;
        let (server_transient, cookie) = opened.split_at(KEY);
        let server_transient = public_key(server_transient);
        let cipher = SalsaBox::new(&server_transient, &self.transient);

        // The vouch proves C and C' belong together: Box[C',S](C->S').
        let vouch_nonce = random::<16>();
        let mut vouched = Vec::with_capacity(2 * KEY);
        vouched.extend_from_slice(self.transient.public_key().as_bytes());
        vouched.extend_from_slice(self.server_key.as_bytes());
        let vouch = seal(
            &SalsaBox::new(&server_transient, &self.secret),
            &long_nonce(b"VOUCH---", &vouch_nonce),
            &vouched,
        );
        let mut plaintext = Zeroizing::new(Vec::new());
        plaintext.extend_from_slice(self.public.as_bytes());
        plaintext.extend_from_slice(&vouch_nonce);
        plaintext.extend_from_slice(&vouch);
        encode_metadata(
            &local_metadata(self.socket_type, &self.identity),
            &mut plaintext,
        );
        let sealed = seal(&cipher, &short_nonce(INITIATE_PREFIX, 2), &plaintext);
        let mut initiate = Vec::with_capacity(cookie.len() + 8 + sealed.len());
        initiate.extend_from_slice(cookie);
        initiate.extend_from_slice(&2u64.to_be_bytes());
        initiate.extend_from_slice(&sealed);
        write_raw_command(self.version, "INITIATE", &initiate, out);
        Ok(cipher)
    }

    fn ready(self, data: &[u8], out: &mut Vec<u8>) -> Result<CurveClientStep, HandshakeError> {
        let ClientState::WaitingForReady { cipher } = self.state else {
            unreachable!("READY is only handled while waiting for it");
        };
        if data.len() < 8 + TAG {
            return Err(malformed("READY"));
        }
        let (nonce, sealed) = data.split_at(8);
        let nonce = u64::from_be_bytes(nonce.try_into().expect("8 octets"));
        let opened = open(&cipher, &short_nonce(READY_PREFIX, nonce), sealed)?;
        let metadata =
            parse_metadata(&opened, "READY").map_err(HandshakeError::MalformedCommand)?;
        // In CURVE only the server sends ERROR.
        let established = accept_peer(
            self.socket_type,
            self.version,
            metadata,
            Rejection::Silent,
            out,
        )?;
        let codec = CurveCodec {
            cipher,
            send_prefix: CLIENT_MESSAGE_PREFIX,
            receive_prefix: SERVER_MESSAGE_PREFIX,
            next_nonce: 3,
            peer_nonce: nonce,
            partial: Vec::new(),
        };
        Ok(CurveClientStep::Established(established, codec))
    }
}

impl CurveServer {
    /// Starts the handshake with this server's long-term `keypair`. The
    /// client speaks first, so nothing is written.
    pub fn start(
        config: CurveServerConfig,
        keypair: &Keypair,
        peer: &PeerGreeting,
    ) -> Result<Self, HandshakeError> {
        let version = require_zmtp_3(peer)?;
        Ok(Self {
            config,
            version,
            public: keypair.public(),
            secret: crypto_box::SecretKey::from_bytes(*keypair.secret().expose()),
            state: ServerState::WaitingForHello,
        })
    }

    /// Handles the next item from the client.
    pub fn receive(
        mut self,
        incoming: Incoming,
        out: &mut Vec<u8>,
    ) -> Result<CurveServerStep, HandshakeError> {
        let body = command_body(incoming)?;
        let (name, data) = split_command(&body)?;
        match (&self.state, name) {
            (_, b"ERROR") => Err(peer_error(&body)),
            (ServerState::WaitingForHello, b"HELLO") => {
                self.state = self.hello(data, out)?;
                Ok(CurveServerStep::Continue(self))
            }
            (ServerState::WaitingForInitiate { .. }, b"INITIATE") => self.initiate(data),
            (_, name) => Err(HandshakeError::UnexpectedCommand(Frame::copy_from_slice(
                name,
            ))),
        }
    }

    /// Checks HELLO and answers with WELCOME.
    fn hello(&self, data: &[u8], out: &mut Vec<u8>) -> Result<ServerState, HandshakeError> {
        if data.len() != 194 || data[..2] != [1, 0] {
            return Err(malformed("HELLO"));
        }
        let client_transient = public_key(&data[74..106]);
        let nonce = u64::from_be_bytes(data[106..114].try_into().expect("8 octets"));
        // The signature box only proves the client knows our key; its 64
        // zero octets are not checked, as libzmq does not.
        open(
            &SalsaBox::new(&client_transient, &self.secret),
            &short_nonce(HELLO_PREFIX, nonce),
            &data[114..],
        )?;

        // The cookie holds C' and s' under a key only we know, so the
        // client must send them back in INITIATE.
        let transient = crypto_box::SecretKey::generate(&mut OsRng);
        let cookie_key = Zeroizing::new(random::<KEY>());
        let cookie_nonce = random::<16>();
        let mut cookie_plaintext = Zeroizing::new(Vec::with_capacity(2 * KEY));
        cookie_plaintext.extend_from_slice(client_transient.as_bytes());
        cookie_plaintext.extend_from_slice(&transient.to_bytes());
        let cookie = seal(
            &XSalsa20Poly1305::new(GenericArray::from_slice(&*cookie_key)),
            &long_nonce(b"COOKIE--", &cookie_nonce),
            &cookie_plaintext,
        );

        let mut plaintext = Zeroizing::new(Vec::with_capacity(KEY + COOKIE));
        plaintext.extend_from_slice(transient.public_key().as_bytes());
        plaintext.extend_from_slice(&cookie_nonce);
        plaintext.extend_from_slice(&cookie);
        let welcome_nonce = random::<16>();
        let sealed = seal(
            &SalsaBox::new(&client_transient, &self.secret),
            &long_nonce(b"WELCOME-", &welcome_nonce),
            &plaintext,
        );
        let mut welcome = Vec::with_capacity(16 + sealed.len());
        welcome.extend_from_slice(&welcome_nonce);
        welcome.extend_from_slice(&sealed);
        write_raw_command(self.version, "WELCOME", &welcome, out);
        Ok(ServerState::WaitingForInitiate {
            client_transient,
            transient,
            cookie_key,
        })
    }

    /// Checks INITIATE: the cookie, the box, and the vouch for C.
    fn initiate(self, data: &[u8]) -> Result<CurveServerStep, HandshakeError> {
        let ServerState::WaitingForInitiate {
            client_transient,
            transient,
            cookie_key,
        } = self.state
        else {
            unreachable!("INITIATE is only handled while waiting for it");
        };
        if data.len() < COOKIE + 8 + TAG + KEY + 16 + 80 {
            return Err(malformed("INITIATE"));
        }
        let (cookie, rest) = data.split_at(COOKIE);
        let (cookie_nonce, cookie_box) = cookie.split_at(16);
        let cookie = open(
            &XSalsa20Poly1305::new(GenericArray::from_slice(&*cookie_key)),
            &long_nonce(b"COOKIE--", cookie_nonce),
            cookie_box,
        )?;
        if cookie[..KEY] != *client_transient.as_bytes() || cookie[KEY..] != transient.to_bytes() {
            return Err(HandshakeError::Cryptographic);
        }

        let (nonce, sealed) = rest.split_at(8);
        let nonce = u64::from_be_bytes(nonce.try_into().expect("8 octets"));
        let cipher = SalsaBox::new(&client_transient, &transient);
        let opened = open(&cipher, &short_nonce(INITIATE_PREFIX, nonce), sealed)?;
        if opened.len() < KEY + 16 + 80 {
            return Err(malformed("INITIATE"));
        }
        let client_key = PublicKey::from_bytes(opened[..KEY].try_into().expect("32 octets"));
        let vouch_nonce = &opened[KEY..KEY + 16];
        let vouch = &opened[KEY + 16..KEY + 16 + 80];
        let vouched = open(
            &SalsaBox::new(&box_key(&client_key), &transient),
            &long_nonce(b"VOUCH---", vouch_nonce),
            vouch,
        )?;
        // libzmq checks only C'; RFC 26 asks for S too.
        if vouched[..KEY] != *client_transient.as_bytes()
            || vouched[KEY..] != *self.public.as_bytes()
        {
            return Err(HandshakeError::Cryptographic);
        }
        let metadata = parse_metadata(&opened[KEY + 16 + 80..], "INITIATE")
            .map_err(HandshakeError::MalformedCommand)?;

        let request = ZapRequest {
            domain: self.config.domain.clone(),
            address: self.config.peer_address.clone(),
            identity: Frame::copy_from_slice(self.config.identity.as_slice()),
            credentials: ZapCredentials::Curve(client_key),
        };
        let pending = PendingCurveAuthentication {
            config: self.config,
            version: self.version,
            metadata,
            cipher,
            peer_nonce: nonce,
        };
        Ok(CurveServerStep::Authenticate(pending, request))
    }
}

impl PendingCurveAuthentication {
    /// Applies the authenticator's verdict: READY on success, otherwise
    /// ERROR or a silent close, as for PLAIN.
    pub fn reply(
        self,
        reply: ZapReply,
        out: &mut Vec<u8>,
    ) -> Result<(Established, CurveCodec), HandshakeError> {
        let authenticated = zap_verdict(reply, self.version, out)?;
        let socket_type = self.config.socket_type;
        let mut established = accept_peer(
            socket_type,
            self.version,
            self.metadata,
            Rejection::SendError,
            out,
        )?;
        let mut metadata = Zeroizing::new(Vec::new());
        encode_metadata(
            &local_metadata(socket_type, &self.config.identity),
            &mut metadata,
        );
        let sealed = seal(&self.cipher, &short_nonce(READY_PREFIX, 1), &metadata);
        let mut ready = Vec::with_capacity(8 + sealed.len());
        ready.extend_from_slice(&1u64.to_be_bytes());
        ready.extend_from_slice(&sealed);
        write_raw_command(self.version, "READY", &ready, out);
        established.authenticated = Some(authenticated);
        let codec = CurveCodec {
            cipher: self.cipher,
            send_prefix: SERVER_MESSAGE_PREFIX,
            receive_prefix: CLIENT_MESSAGE_PREFIX,
            next_nonce: 2,
            peer_nonce: self.peer_nonce,
            partial: Vec::new(),
        };
        Ok((established, codec))
    }
}

impl CurveCodec {
    /// Boxes each frame of `message` and appends them to `out`.
    pub fn encode_message(
        &mut self,
        message: &Message,
        encoder: Encoder,
        out: &mut Vec<u8>,
    ) -> Result<(), CurveError> {
        let last = message.len().get() - 1;
        for (i, frame) in message.iter().enumerate() {
            let flags = if i < last { MORE } else { 0 };
            self.encode_frame(flags, frame, encoder, out)?;
        }
        Ok(())
    }

    /// Boxes a command (name length, name, data) and appends it to `out`.
    pub fn encode_command(
        &mut self,
        body: &[u8],
        encoder: Encoder,
        out: &mut Vec<u8>,
    ) -> Result<(), CurveError> {
        self.encode_frame(COMMAND, body, encoder, out)
    }

    fn encode_frame(
        &mut self,
        flags: u8,
        payload: &[u8],
        encoder: Encoder,
        out: &mut Vec<u8>,
    ) -> Result<(), CurveError> {
        let nonce = self.next_nonce;
        if nonce == u64::MAX {
            return Err(CurveError::NonceExhausted);
        }
        self.next_nonce += 1;
        let mut plaintext = Zeroizing::new(Vec::with_capacity(1 + payload.len()));
        plaintext.push(flags);
        plaintext.extend_from_slice(payload);
        let sealed = seal(
            &self.cipher,
            &short_nonce(self.send_prefix, nonce),
            &plaintext,
        );
        let mut body = Vec::with_capacity(MESSAGE.len() + 8 + sealed.len());
        body.extend_from_slice(MESSAGE);
        body.extend_from_slice(&nonce.to_be_bytes());
        body.extend_from_slice(&sealed);
        encoder.encode(&Message::new(Frame::from(body)), out);
        Ok(())
    }

    /// Unboxes one item from the decoder. Returns a whole message once its
    /// last frame arrives, or a command the peer boxed.
    pub fn decode(&mut self, incoming: Incoming) -> Result<Option<Incoming>, CurveError> {
        let body = match incoming {
            Incoming::Command(body) => body,
            Incoming::Message(message) => match message.split_first() {
                (body, []) => body.clone(),
                // MESSAGE is never sent with MORE: each frame is boxed alone.
                _ => return Err(CurveError::NotMessage),
            },
        };
        let data = body.strip_prefix(MESSAGE).ok_or(CurveError::NotMessage)?;
        if data.len() < 8 + TAG + 1 {
            return Err(CurveError::Malformed);
        }
        let (nonce, sealed) = data.split_at(8);
        let nonce = u64::from_be_bytes(nonce.try_into().expect("8 octets"));
        if nonce <= self.peer_nonce {
            return Err(CurveError::StaleNonce);
        }
        let opened = open(
            &self.cipher,
            &short_nonce(self.receive_prefix, nonce),
            sealed,
        )
        .map_err(|_| CurveError::Cryptographic)?;
        self.peer_nonce = nonce;
        let (&flags, payload) = opened.split_first().ok_or(CurveError::Malformed)?;
        let payload = Frame::copy_from_slice(payload);
        if flags & COMMAND != 0 {
            if !self.partial.is_empty() {
                return Err(CurveError::Malformed);
            }
            return Ok(Some(Incoming::Command(payload)));
        }
        self.partial.push(payload);
        if flags & MORE != 0 {
            return Ok(None);
        }
        let frames = std::mem::take(&mut self.partial);
        Ok(Some(Incoming::Message(
            Message::try_from(frames).expect("at least the frame just pushed"),
        )))
    }
}

/// Seals `plaintext` as NaCl does: authenticator, then ciphertext.
fn seal(cipher: &impl AeadInPlace, nonce: &[u8; 24], plaintext: &[u8]) -> Vec<u8> {
    let mut sealed = Vec::with_capacity(TAG + plaintext.len());
    sealed.extend_from_slice(&[0; TAG]);
    sealed.extend_from_slice(plaintext);
    let tag = cipher
        .encrypt_in_place_detached(GenericArray::from_slice(nonce), b"", &mut sealed[TAG..])
        .expect("XSalsa20Poly1305 seals any length we send");
    sealed[..TAG].copy_from_slice(&tag);
    sealed
}

/// Opens a box sealed by [`seal`].
fn open(
    cipher: &impl AeadInPlace,
    nonce: &[u8; 24],
    sealed: &[u8],
) -> Result<Zeroizing<Vec<u8>>, HandshakeError> {
    let (tag, ciphertext) = sealed
        .split_at_checked(TAG)
        .ok_or(HandshakeError::Cryptographic)?;
    let mut plaintext = Zeroizing::new(ciphertext.to_vec());
    cipher
        .decrypt_in_place_detached(
            GenericArray::from_slice(nonce),
            b"",
            &mut plaintext,
            GenericArray::from_slice(tag),
        )
        .map_err(|_| HandshakeError::Cryptographic)?;
    Ok(plaintext)
}

/// A 16-octet prefix and a big-endian short nonce.
fn short_nonce(prefix: &[u8; 16], nonce: u64) -> [u8; 24] {
    let mut full = [0; 24];
    full[..16].copy_from_slice(prefix);
    full[16..].copy_from_slice(&nonce.to_be_bytes());
    full
}

/// An 8-octet prefix and 16 octets, random or received.
fn long_nonce(prefix: &[u8; 8], rest: &[u8]) -> [u8; 24] {
    let mut full = [0; 24];
    full[..8].copy_from_slice(prefix);
    full[8..].copy_from_slice(rest);
    full
}

fn random<const N: usize>() -> [u8; N] {
    let mut bytes = [0; N];
    OsRng.fill_bytes(&mut bytes);
    bytes
}

fn box_key(key: &PublicKey) -> crypto_box::PublicKey {
    crypto_box::PublicKey::from(*key.as_bytes())
}

fn public_key(bytes: &[u8]) -> crypto_box::PublicKey {
    let bytes: [u8; KEY] = bytes.try_into().expect("32 octets");
    crypto_box::PublicKey::from(bytes)
}

fn command_body(incoming: Incoming) -> Result<Frame, HandshakeError> {
    match incoming {
        Incoming::Command(body) => Ok(body),
        Incoming::Message(_) => Err(HandshakeError::UnexpectedMessage),
    }
}

/// A command body's name and data.
fn split_command(body: &[u8]) -> Result<(&[u8], &[u8]), HandshakeError> {
    let (&len, rest) = body
        .split_first()
        .ok_or(HandshakeError::MalformedCommand(CommandError::MissingName))?;
    rest.split_at_checked(usize::from(len))
        .ok_or(HandshakeError::MalformedCommand(CommandError::Truncated {
            command: "command name",
        }))
}

fn peer_error(body: &[u8]) -> HandshakeError {
    match Command::parse(body) {
        Ok(Command::Error(reason)) => HandshakeError::PeerError(reason),
        Ok(_) => malformed("ERROR"),
        Err(err) => HandshakeError::MalformedCommand(err),
    }
}

fn malformed(command: &'static str) -> HandshakeError {
    HandshakeError::MalformedCommand(CommandError::Truncated { command })
}

impl fmt::Debug for CurveClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CurveClient")
            .field("server_key", &self.server_key)
            .field("public", &self.public)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for CurveServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CurveServer")
            .field("config", &self.config)
            .field("public", &self.public)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for PendingCurveAuthentication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingCurveAuthentication")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for CurveCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CurveCodec")
            .field("next_nonce", &self.next_nonce)
            .field("peer_nonce", &self.peer_nonce)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for CurveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotMessage => "expected a CURVE MESSAGE command",
            Self::Malformed => "malformed CURVE MESSAGE",
            Self::StaleNonce => "CURVE nonce did not increase",
            Self::Cryptographic => "CURVE box did not open",
            Self::NonceExhausted => "CURVE nonces exhausted",
        })
    }
}

impl std::error::Error for CurveError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Decoder, DecoderConfig, ErrorReason, ZapFailure};

    const V3_1: PeerGreeting = PeerGreeting::Zmtp3 {
        major: 3,
        minor: 1,
        as_server: false,
    };

    /// The items written to `out`, in order.
    fn items(out: &[u8]) -> Vec<Incoming> {
        let mut decoder = Decoder::new(Version::V3_1, DecoderConfig::default());
        decoder.extend_from_slice(out);
        std::iter::from_fn(|| decoder.decode().unwrap()).collect()
    }

    /// The command bodies written to `out`, in order.
    fn commands(out: &[u8]) -> Vec<Frame> {
        items(out)
            .into_iter()
            .map(|item| match item {
                Incoming::Command(body) => body,
                Incoming::Message(message) => panic!("expected a command, got {message:?}"),
            })
            .collect()
    }

    fn only(out: &[u8]) -> Incoming {
        let mut bodies = commands(out);
        assert_eq!(bodies.len(), 1, "{bodies:?}");
        Incoming::Command(bodies.remove(0))
    }

    fn server_config(socket_type: SocketType) -> CurveServerConfig {
        CurveServerConfig {
            socket_type,
            identity: Identity::default(),
            domain: Frame::from("global"),
            peer_address: "127.0.0.1".into(),
        }
    }

    fn accepted() -> ZapReply {
        ZapReply::Ok {
            user_id: Frame::from("alice"),
            metadata: Metadata::new(),
        }
    }

    /// Runs a handshake up to the server's ZAP request.
    struct Run {
        client: CurveClient,
        pending: PendingCurveAuthentication,
        request: ZapRequest,
        client_keys: Keypair,
        wire: Vec<Vec<u8>>,
    }

    fn run_to_zap(client_type: SocketType, server_type: SocketType) -> Run {
        let server_keys = Keypair::generate();
        let client_keys = Keypair::generate();
        let mut wire = Vec::new();

        let mut out = Vec::new();
        let client = CurveClient::start(
            client_type,
            Identity::new("me").unwrap(),
            &client_keys,
            server_keys.public(),
            &V3_1,
            &mut out,
        )
        .unwrap();
        wire.push(out.clone());
        let server = CurveServer::start(server_config(server_type), &server_keys, &V3_1).unwrap();
        let mut welcome = Vec::new();
        let CurveServerStep::Continue(server) = server.receive(only(&out), &mut welcome).unwrap()
        else {
            panic!("expected WELCOME");
        };
        wire.push(welcome.clone());
        let mut initiate = Vec::new();
        let CurveClientStep::Continue(client) =
            client.receive(only(&welcome), &mut initiate).unwrap()
        else {
            panic!("expected INITIATE");
        };
        wire.push(initiate.clone());
        let CurveServerStep::Authenticate(pending, request) =
            server.receive(only(&initiate), &mut Vec::new()).unwrap()
        else {
            panic!("expected a ZAP request");
        };
        Run {
            client,
            pending,
            request,
            client_keys,
            wire,
        }
    }

    fn established() -> (CurveCodec, CurveCodec) {
        let run = run_to_zap(SocketType::Dealer, SocketType::Router);
        let mut ready = Vec::new();
        let (_, server) = run.pending.reply(accepted(), &mut ready).unwrap();
        let CurveClientStep::Established(_, client) =
            run.client.receive(only(&ready), &mut Vec::new()).unwrap()
        else {
            panic!("expected READY");
        };
        (client, server)
    }

    /// The MESSAGE bodies a codec writes for `message`: each must be a
    /// single-part data frame, as libzmq sends them.
    fn boxed(codec: &mut CurveCodec, message: &Message) -> Vec<Frame> {
        let mut out = Vec::new();
        codec
            .encode_message(message, Encoder::new(Version::V3_1), &mut out)
            .unwrap();
        items(&out)
            .into_iter()
            .map(|item| match item {
                Incoming::Message(message) => {
                    let (body, rest) = message.split_first();
                    assert!(rest.is_empty(), "MESSAGE is never multipart");
                    body.clone()
                }
                Incoming::Command(_) => panic!("MESSAGE must be a data frame"),
            })
            .collect()
    }

    fn unbox(codec: &mut CurveCodec, frames: Vec<Frame>) -> Vec<Incoming> {
        frames
            .into_iter()
            .filter_map(|frame| {
                codec
                    .decode(Incoming::Message(Message::new(frame)))
                    .unwrap()
            })
            .collect()
    }

    #[test]
    fn a_full_handshake_establishes_both_sides() {
        let run = run_to_zap(SocketType::Dealer, SocketType::Router);
        assert_eq!(
            run.request.credentials,
            ZapCredentials::Curve(run.client_keys.public())
        );
        assert_eq!(run.request.credentials.mechanism(), "CURVE");
        assert_eq!(run.request.domain, Frame::from("global"));
        let mut ready = Vec::new();
        let (server_side, _) = run.pending.reply(accepted(), &mut ready).unwrap();
        assert_eq!(server_side.peer_socket_type, Some(SocketType::Dealer));
        assert_eq!(server_side.peer_identity, Frame::from("me"));
        assert_eq!(
            server_side.authenticated.unwrap().user_id,
            Frame::from("alice")
        );
        let CurveClientStep::Established(client_side, _) =
            run.client.receive(only(&ready), &mut Vec::new()).unwrap()
        else {
            panic!("expected READY");
        };
        assert_eq!(client_side.peer_socket_type, Some(SocketType::Router));
    }

    #[test]
    fn handshake_commands_have_rfc_26_sizes() {
        let run = run_to_zap(SocketType::Dealer, SocketType::Router);
        let sizes: Vec<usize> = run.wire.iter().map(|out| commands(out)[0].len()).collect();
        assert_eq!(sizes[0], 200, "HELLO");
        assert_eq!(sizes[1], 168, "WELCOME");
        assert!(sizes[2] >= 257, "INITIATE is {}", sizes[2]);
        let mut ready = Vec::new();
        run.pending.reply(accepted(), &mut ready).unwrap();
        assert!(commands(&ready)[0].len() >= 30, "READY");
    }

    #[test]
    fn messages_and_commands_cross_both_ways() {
        let (mut client, mut server) = established();
        let message = Message::try_from(vec![
            Frame::from("a"),
            Frame::new(),
            Frame::from(vec![7; 300]),
        ])
        .unwrap();
        let frames = boxed(&mut client, &message);
        assert_eq!(frames.len(), 3, "one MESSAGE per frame");
        assert_eq!(frames[0].len(), 8 + 8 + TAG + 1 + 1);
        assert_eq!(unbox(&mut server, frames), [Incoming::Message(message)]);

        let reply = Message::from("pong");
        let frames = boxed(&mut server, &reply);
        assert_eq!(unbox(&mut client, frames), [Incoming::Message(reply)]);

        let mut out = Vec::new();
        server
            .encode_command(b"\x04PING\x00\x00", Encoder::new(Version::V3_1), &mut out)
            .unwrap();
        let frames = items(&out)
            .into_iter()
            .map(|item| match item {
                Incoming::Message(message) => message.split_first().0.clone(),
                Incoming::Command(_) => panic!("a boxed command is still a data frame"),
            })
            .collect();
        assert_eq!(
            unbox(&mut client, frames),
            [Incoming::Command(Frame::from(&b"\x04PING\x00\x00"[..]))]
        );
    }

    #[test]
    fn tampered_replayed_and_unboxed_frames_are_refused() {
        let (mut client, mut server) = established();
        let frames = boxed(&mut client, &Message::from("x"));
        let mut tampered = frames[0].to_vec();
        *tampered.last_mut().unwrap() ^= 1;
        assert_eq!(
            server.decode(Incoming::Command(Frame::from(tampered))),
            Err(CurveError::Cryptographic)
        );
        server.decode(Incoming::Command(frames[0].clone())).unwrap();
        assert_eq!(
            server.decode(Incoming::Command(frames[0].clone())),
            Err(CurveError::StaleNonce),
            "a replay"
        );
        assert_eq!(
            server.decode(Incoming::Message(Message::from("plain"))),
            Err(CurveError::NotMessage)
        );
        let multipart = Message::try_from(vec![frames[0].clone(), frames[0].clone()]).unwrap();
        assert_eq!(
            server.decode(Incoming::Message(multipart)),
            Err(CurveError::NotMessage)
        );
        assert_eq!(
            server.decode(Incoming::Command(Frame::from(&b"\x07MESSAGE\0"[..]))),
            Err(CurveError::Malformed)
        );
    }

    #[test]
    fn a_command_framed_message_is_accepted_too() {
        let (mut client, mut server) = established();
        let frames = boxed(&mut client, &Message::from("as RFC 25 frames it"));
        assert_eq!(
            server.decode(Incoming::Command(frames[0].clone())),
            Ok(Some(Incoming::Message(Message::from(
                "as RFC 25 frames it"
            ))))
        );
    }

    #[test]
    fn each_direction_has_its_own_nonce_prefix() {
        let (_, mut server) = established();
        // A frame the server sent, reflected back to it, must not open,
        // even with a nonce the server has not yet seen from the client.
        boxed(&mut server, &Message::from("first"));
        let frames = boxed(&mut server, &Message::from("reflected"));
        assert_eq!(
            server.decode(Incoming::Command(frames[0].clone())),
            Err(CurveError::Cryptographic)
        );
    }

    #[test]
    fn nonces_never_wrap() {
        let (mut client, _) = established();
        client.next_nonce = u64::MAX - 1;
        let encoder = Encoder::new(Version::V3_1);
        client
            .encode_message(&Message::from("last"), encoder, &mut Vec::new())
            .unwrap();
        assert_eq!(
            client.encode_message(&Message::from("one too many"), encoder, &mut Vec::new()),
            Err(CurveError::NonceExhausted)
        );
    }

    #[test]
    fn a_client_with_the_wrong_server_key_is_refused_at_hello() {
        let mut out = Vec::new();
        let _client = CurveClient::start(
            SocketType::Dealer,
            Identity::default(),
            &Keypair::generate(),
            Keypair::generate().public(),
            &V3_1,
            &mut out,
        )
        .unwrap();
        let server = CurveServer::start(
            server_config(SocketType::Router),
            &Keypair::generate(),
            &V3_1,
        )
        .unwrap();
        assert!(matches!(
            server.receive(only(&out), &mut Vec::new()),
            Err(HandshakeError::Cryptographic)
        ));
    }

    #[test]
    fn hello_must_be_version_1_0_and_200_octets() {
        let server_keys = Keypair::generate();
        let mut out = Vec::new();
        let _client = CurveClient::start(
            SocketType::Dealer,
            Identity::default(),
            &Keypair::generate(),
            server_keys.public(),
            &V3_1,
            &mut out,
        )
        .unwrap();
        let hello = commands(&out).remove(0);
        let start =
            || CurveServer::start(server_config(SocketType::Router), &server_keys, &V3_1).unwrap();
        let mut wrong_version = hello.to_vec();
        wrong_version[6] = 2;
        let short = hello[..199].to_vec();
        for body in [wrong_version, short] {
            assert!(matches!(
                start().receive(Incoming::Command(Frame::from(body)), &mut Vec::new()),
                Err(HandshakeError::MalformedCommand(_))
            ));
        }
    }

    #[test]
    fn a_tampered_cookie_is_refused() {
        let server_keys = Keypair::generate();
        let mut out = Vec::new();
        let client = CurveClient::start(
            SocketType::Dealer,
            Identity::default(),
            &Keypair::generate(),
            server_keys.public(),
            &V3_1,
            &mut out,
        )
        .unwrap();
        let server =
            CurveServer::start(server_config(SocketType::Router), &server_keys, &V3_1).unwrap();
        let mut welcome = Vec::new();
        let CurveServerStep::Continue(server) = server.receive(only(&out), &mut welcome).unwrap()
        else {
            panic!()
        };
        let mut initiate = Vec::new();
        let CurveClientStep::Continue(_) = client.receive(only(&welcome), &mut initiate).unwrap()
        else {
            panic!()
        };
        let mut body = commands(&initiate).remove(0).to_vec();
        body[9 + 20] ^= 1; // inside the cookie's box
        assert!(matches!(
            server.receive(Incoming::Command(Frame::from(body)), &mut Vec::new()),
            Err(HandshakeError::Cryptographic)
        ));
    }

    /// An INITIATE whose vouch claims `vouched_transient` and
    /// `vouched_server`, otherwise built as the client would.
    fn forged_initiate(
        client: &CurveClient,
        welcome: &[u8],
        vouched_transient: &crypto_box::PublicKey,
        vouched_server: &PublicKey,
    ) -> Incoming {
        let body = commands(welcome).remove(0);
        let data = &body[8..];
        let opened = open(
            &SalsaBox::new(&box_key(&client.server_key), &client.transient),
            &long_nonce(b"WELCOME-", &data[..16]),
            &data[16..],
        )
        .unwrap();
        let server_transient = public_key(&opened[..KEY]);
        let cookie = &opened[KEY..];
        let vouch_nonce = random::<16>();
        let mut vouched = Vec::new();
        vouched.extend_from_slice(vouched_transient.as_bytes());
        vouched.extend_from_slice(vouched_server.as_bytes());
        let vouch = seal(
            &SalsaBox::new(&server_transient, &client.secret),
            &long_nonce(b"VOUCH---", &vouch_nonce),
            &vouched,
        );
        let mut plaintext = Vec::new();
        plaintext.extend_from_slice(client.public.as_bytes());
        plaintext.extend_from_slice(&vouch_nonce);
        plaintext.extend_from_slice(&vouch);
        let sealed = seal(
            &SalsaBox::new(&server_transient, &client.transient),
            &short_nonce(INITIATE_PREFIX, 2),
            &plaintext,
        );
        let mut initiate = b"\x08INITIATE".to_vec();
        initiate.extend_from_slice(cookie);
        initiate.extend_from_slice(&2u64.to_be_bytes());
        initiate.extend_from_slice(&sealed);
        Incoming::Command(Frame::from(initiate))
    }

    #[test]
    fn a_vouch_must_name_this_connection_and_this_server() {
        let other_transient = crypto_box::SecretKey::generate(&mut OsRng).public_key();
        let other_server = Keypair::generate().public();
        for wrong_transient in [true, false] {
            let server_keys = Keypair::generate();
            let mut out = Vec::new();
            let client = CurveClient::start(
                SocketType::Dealer,
                Identity::default(),
                &Keypair::generate(),
                server_keys.public(),
                &V3_1,
                &mut out,
            )
            .unwrap();
            let server =
                CurveServer::start(server_config(SocketType::Router), &server_keys, &V3_1).unwrap();
            let mut welcome = Vec::new();
            let CurveServerStep::Continue(server) =
                server.receive(only(&out), &mut welcome).unwrap()
            else {
                panic!()
            };
            let (transient, server_key) = if wrong_transient {
                (other_transient.clone(), server_keys.public())
            } else {
                (client.transient.public_key(), other_server)
            };
            let initiate = forged_initiate(&client, &welcome, &transient, &server_key);
            assert!(
                matches!(
                    server.receive(initiate, &mut Vec::new()),
                    Err(HandshakeError::Cryptographic)
                ),
                "wrong transient: {wrong_transient}"
            );
        }
    }

    #[test]
    fn an_honest_forged_initiate_is_accepted() {
        // The forging helper itself builds a valid INITIATE when told the
        // truth, so the test above fails only for the vouch.
        let server_keys = Keypair::generate();
        let mut out = Vec::new();
        let client = CurveClient::start(
            SocketType::Dealer,
            Identity::default(),
            &Keypair::generate(),
            server_keys.public(),
            &V3_1,
            &mut out,
        )
        .unwrap();
        let server =
            CurveServer::start(server_config(SocketType::Router), &server_keys, &V3_1).unwrap();
        let mut welcome = Vec::new();
        let CurveServerStep::Continue(server) = server.receive(only(&out), &mut welcome).unwrap()
        else {
            panic!()
        };
        let initiate = forged_initiate(
            &client,
            &welcome,
            &client.transient.public_key(),
            &server_keys.public(),
        );
        assert!(matches!(
            server.receive(initiate, &mut Vec::new()),
            Ok(CurveServerStep::Authenticate(..))
        ));
    }

    #[test]
    fn a_denied_client_hears_the_zap_status() {
        let run = run_to_zap(SocketType::Dealer, SocketType::Router);
        let mut error = Vec::new();
        assert!(matches!(
            run.pending
                .reply(ZapReply::AuthenticationFailure, &mut error),
            Err(HandshakeError::Authentication(ZapFailure::Denied))
        ));
        assert!(matches!(
            run.client.receive(only(&error), &mut Vec::new()),
            Err(HandshakeError::PeerError(reason)) if reason == ErrorReason::from_static("400")
        ));
    }

    #[test]
    fn an_incompatible_peer_is_refused_after_authentication() {
        let run = run_to_zap(SocketType::Push, SocketType::Router);
        let mut out = Vec::new();
        assert!(matches!(
            run.pending.reply(accepted(), &mut out),
            Err(HandshakeError::IncompatiblePeer {
                peer: SocketType::Push
            })
        ));
        assert!(matches!(
            Command::parse(&commands(&out)[0]),
            Ok(Command::Error(_))
        ));
    }

    #[test]
    fn curve_needs_zmtp_3() {
        let legacy = PeerGreeting::Zmtp2 { socket_type: 5 };
        assert!(matches!(
            CurveServer::start(
                server_config(SocketType::Router),
                &Keypair::generate(),
                &legacy
            ),
            Err(HandshakeError::LegacyPeer)
        ));
    }
}
