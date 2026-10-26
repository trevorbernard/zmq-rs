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
//!
//! Boxes are `dryoc`'s, libsodium-compatible: its Salsa20 runs on AVX2 or
//! AVX-512 where the CPU has them, where RustCrypto's `salsa20` is scalar
//! only.

use std::fmt;

use bytes::{Buf, Bytes};
use dryoc::classic::crypto_box::{
    crypto_box_beforenm, crypto_box_detached_afternm_inplace, crypto_box_keypair,
    crypto_box_open_detached_afternm_inplace,
};
use zeroize::Zeroizing;

use crate::codec::counted_size;
use crate::command::{encode_metadata, parse_metadata};
use crate::handshake::{
    Rejection, ServerConfig, accept_peer, command_body, local_metadata, malformed, require_zmtp_3,
    unexpected, write_raw_command, zap_verdict,
};
use crate::{
    Command, CommandError, Encoder, Established, Frame, HandshakeError, Identity, Incoming,
    Keypair, Message, Metadata, PeerGreeting, PublicKey, SocketType, Version, ZapCredentials,
    ZapReply, ZapRequest,
};

const TAG: usize = 16;
const KEY: usize = 32;
/// A nonce's part on the wire: 8 octets of counter after a 16-octet
/// prefix, or 16 random octets after an 8-octet one.
const SHORT_NONCE: usize = 8;
const LONG_NONCE: usize = 16;
/// HELLO's padding, so that HELLO is as long as WELCOME (anti-amplification).
const HELLO_PADDING: usize = 72;
/// HELLO's signature: a box of 64 zero octets.
const SIGNATURE: usize = 64 + TAG;
/// HELLO: version 1.0, padding, C', a short nonce and the signature box.
const HELLO: usize = 2 + HELLO_PADDING + KEY + SHORT_NONCE + SIGNATURE;
/// Box[C' s'] under the cookie key, after its long nonce.
const COOKIE: usize = LONG_NONCE + KEY + KEY + TAG;
/// WELCOME: a long nonce and Box[S' cookie].
const WELCOME: usize = LONG_NONCE + KEY + COOKIE + TAG;
/// The vouch, Box[C' S].
const VOUCH: usize = KEY + KEY + TAG;
/// The least INITIATE's box holds: C, the vouch's long nonce and the vouch.
const INITIATE_BOXED: usize = KEY + LONG_NONCE + VOUCH;
/// The flags inside a MESSAGE box (RFC 26), not the wire's (RFC 37).
const BOX_MORE: u8 = 0x01;
const BOX_COMMAND: u8 = 0x02;

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
    secret: Zeroizing<Key>,
    transient: Transient,
    state: ClientState,
}

enum ClientState {
    /// `key` boxes between our transient key and the server's long-term
    /// key, HELLO and WELCOME alike.
    WaitingForWelcome { key: BoxKey },
    /// `key` boxes between the transient keys, READY and MESSAGEs alike.
    WaitingForReady { key: BoxKey },
}

type Key = [u8; KEY];

/// A key pair made for one connection.
struct Transient {
    public: Key,
    secret: Zeroizing<Key>,
}

impl Transient {
    fn generate() -> Self {
        let (public, secret) = crypto_box_keypair();
        Self {
            public,
            secret: Zeroizing::new(secret),
        }
    }
}

/// A box's precomputed key. Boxing with it is libsodium's
/// `crypto_secretbox`, so it serves public-key boxes and the server's
/// cookie alike.
struct BoxKey(Zeroizing<Key>);

impl BoxKey {
    /// For boxes between `public` and the holder of `secret`: libsodium's
    /// `crypto_box_beforenm`, which refuses a low-order `public`.
    fn between(public: &Key, secret: &Key) -> Result<Self, HandshakeError> {
        crypto_box_beforenm(public, secret)
            .map(|key| Self(Zeroizing::new(key)))
            .map_err(|_| HandshakeError::Cryptographic)
    }

    /// Secret boxes under `key` itself.
    fn secret(key: Key) -> Self {
        Self(Zeroizing::new(key))
    }
}

/// Where a client handshake goes next.
#[derive(Debug)]
#[must_use]
pub enum CurveClientStep {
    Continue(CurveClient),
    /// Done: exchange messages through the codec from now on.
    Established(Established, CurveCodec),
}

/// The server side of a CURVE handshake.
#[must_use]
pub struct CurveServer {
    config: ServerConfig,
    version: Version,
    public: PublicKey,
    secret: Zeroizing<Key>,
    state: ServerState,
}

enum ServerState {
    WaitingForHello,
    WaitingForInitiate {
        client_transient: Key,
        transient: Transient,
        cookie_key: BoxKey,
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
    config: ServerConfig,
    version: Version,
    metadata: Metadata,
    /// Boxes between the transient keys, READY and MESSAGEs alike.
    key: BoxKey,
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
    key: BoxKey,
    send_prefix: &'static [u8; 16],
    receive_prefix: &'static [u8; 16],
    /// The short nonce of the next frame we send.
    next_nonce: u64,
    /// The peer's last short nonce: its next must be greater.
    peer_nonce: u64,
    /// A multipart message whose last frame has yet to arrive.
    partial: Option<Message>,
    /// The size `partial` counts towards `max_message_size`, as the
    /// decoder counts it: each boxed frame reaches the decoder alone, so
    /// only the codec sees a multipart message whole.
    partial_size: u64,
    max_message_size: Option<u64>,
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
    /// A multipart message larger than the limit set with
    /// [`CurveCodec::set_max_message_size`].
    MessageTooLarge,
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
        let transient = Transient::generate();
        let key = BoxKey::between(server_key.as_bytes(), &transient.secret)?;
        let signature = seal(&key, &short_nonce(HELLO_PREFIX, 1), &[0; SIGNATURE - TAG]);
        let mut hello = Vec::with_capacity(HELLO);
        // Version 1.0, then anti-amplification padding.
        hello.extend_from_slice(&[1, 0]);
        hello.extend_from_slice(&[0; HELLO_PADDING]);
        hello.extend_from_slice(&transient.public);
        hello.extend_from_slice(&1u64.to_be_bytes());
        hello.extend_from_slice(&signature);
        write_raw_command(version, "HELLO", &hello, out);
        Ok(Self {
            socket_type,
            identity,
            version,
            server_key,
            public: keypair.public(),
            secret: Zeroizing::new(*keypair.secret().expose()),
            transient,
            state: ClientState::WaitingForWelcome { key },
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
            (ClientState::WaitingForWelcome { key }, b"WELCOME") => {
                let key = self.welcome(key, data, out)?;
                self.state = ClientState::WaitingForReady { key };
                Ok(CurveClientStep::Continue(self))
            }
            (ClientState::WaitingForReady { .. }, b"READY") => self.ready(data, out),
            (_, name) => Err(unexpected(name)),
        }
    }

    /// Opens WELCOME and answers with INITIATE. Returns the box key for
    /// the rest of the connection.
    fn welcome(
        &self,
        hello_key: &BoxKey,
        data: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<BoxKey, HandshakeError> {
        if data.len() != WELCOME {
            return Err(malformed("WELCOME"));
        }
        let (nonce, sealed) = data.split_at(LONG_NONCE);
        let opened = open(hello_key, &long_nonce(b"WELCOME-", nonce), sealed)?;
        let (server_transient, cookie) = opened.split_at(KEY);
        let server_transient: Key = server_transient.try_into().expect("32 octets");
        let key = BoxKey::between(&server_transient, &self.transient.secret)?;

        // The vouch proves C and C' belong together: Box[C',S](C->S').
        let vouch_nonce = random::<LONG_NONCE>();
        let mut vouched = Vec::with_capacity(2 * KEY);
        vouched.extend_from_slice(&self.transient.public);
        vouched.extend_from_slice(self.server_key.as_bytes());
        let vouch = seal(
            &BoxKey::between(&server_transient, &self.secret)?,
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
        let sealed = seal(&key, &short_nonce(INITIATE_PREFIX, 2), &plaintext);
        let mut initiate = Vec::with_capacity(cookie.len() + SHORT_NONCE + sealed.len());
        initiate.extend_from_slice(cookie);
        initiate.extend_from_slice(&2u64.to_be_bytes());
        initiate.extend_from_slice(&sealed);
        write_raw_command(self.version, "INITIATE", &initiate, out);
        Ok(key)
    }

    fn ready(self, data: &[u8], out: &mut Vec<u8>) -> Result<CurveClientStep, HandshakeError> {
        let ClientState::WaitingForReady { key } = self.state else {
            unreachable!("READY is only handled while waiting for it");
        };
        if data.len() < SHORT_NONCE + TAG {
            return Err(malformed("READY"));
        }
        let (nonce, sealed) = data.split_at(SHORT_NONCE);
        let nonce = u64::from_be_bytes(nonce.try_into().expect("8 octets"));
        let opened = open(&key, &short_nonce(READY_PREFIX, nonce), sealed)?;
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
            key,
            send_prefix: CLIENT_MESSAGE_PREFIX,
            receive_prefix: SERVER_MESSAGE_PREFIX,
            next_nonce: 3,
            peer_nonce: nonce,
            partial: None,
            partial_size: 0,
            max_message_size: None,
        };
        Ok(CurveClientStep::Established(established, codec))
    }
}

impl CurveServer {
    /// Starts the handshake with this server's long-term `keypair`. The
    /// client speaks first, so nothing is written.
    pub fn start(
        config: ServerConfig,
        keypair: &Keypair,
        peer: &PeerGreeting,
    ) -> Result<Self, HandshakeError> {
        let version = require_zmtp_3(peer)?;
        Ok(Self {
            config,
            version,
            public: keypair.public(),
            secret: Zeroizing::new(*keypair.secret().expose()),
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
            (_, name) => Err(unexpected(name)),
        }
    }

    /// Checks HELLO and answers with WELCOME.
    fn hello(&self, data: &[u8], out: &mut Vec<u8>) -> Result<ServerState, HandshakeError> {
        if data.len() != HELLO {
            return Err(malformed("HELLO"));
        }
        let (version, rest) = data.split_at(2);
        if version != [1, 0] {
            return Err(malformed("HELLO"));
        }
        let (client_transient, rest) = rest[HELLO_PADDING..].split_at(KEY);
        let (nonce, signature) = rest.split_at(SHORT_NONCE);
        let client_transient: Key = client_transient.try_into().expect("32 octets");
        let nonce = u64::from_be_bytes(nonce.try_into().expect("8 octets"));
        // The signature box only proves the client knows our key; its 64
        // zero octets are not checked, as libzmq does not.
        let long_term = BoxKey::between(&client_transient, &self.secret)?;
        open(&long_term, &short_nonce(HELLO_PREFIX, nonce), signature)?;

        // The cookie holds C' and s' under a key only we know, so the
        // client must send them back in INITIATE.
        let transient = Transient::generate();
        let cookie_key = BoxKey::secret(random::<KEY>());
        let cookie_nonce = random::<LONG_NONCE>();
        let mut cookie_plaintext = Zeroizing::new(Vec::with_capacity(2 * KEY));
        cookie_plaintext.extend_from_slice(&client_transient);
        cookie_plaintext.extend_from_slice(&*transient.secret);
        let cookie = seal(
            &cookie_key,
            &long_nonce(b"COOKIE--", &cookie_nonce),
            &cookie_plaintext,
        );

        let mut plaintext = Zeroizing::new(Vec::with_capacity(KEY + COOKIE));
        plaintext.extend_from_slice(&transient.public);
        plaintext.extend_from_slice(&cookie_nonce);
        plaintext.extend_from_slice(&cookie);
        let welcome_nonce = random::<LONG_NONCE>();
        let sealed = seal(
            &long_term,
            &long_nonce(b"WELCOME-", &welcome_nonce),
            &plaintext,
        );
        let mut welcome = Vec::with_capacity(WELCOME);
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
        if data.len() < COOKIE + SHORT_NONCE + INITIATE_BOXED + TAG {
            return Err(malformed("INITIATE"));
        }
        let (cookie, rest) = data.split_at(COOKIE);
        let (cookie_nonce, cookie_box) = cookie.split_at(LONG_NONCE);
        let cookie = open(
            &cookie_key,
            &long_nonce(b"COOKIE--", cookie_nonce),
            cookie_box,
        )?;
        if cookie[..KEY] != client_transient || cookie[KEY..] != *transient.secret {
            return Err(HandshakeError::Cryptographic);
        }

        let (nonce, sealed) = rest.split_at(SHORT_NONCE);
        let nonce = u64::from_be_bytes(nonce.try_into().expect("8 octets"));
        let key = BoxKey::between(&client_transient, &transient.secret)?;
        let opened = open(&key, &short_nonce(INITIATE_PREFIX, nonce), sealed)?;
        if opened.len() < INITIATE_BOXED {
            return Err(malformed("INITIATE"));
        }
        let (client_key, rest) = opened.split_at(KEY);
        let (vouch_nonce, rest) = rest.split_at(LONG_NONCE);
        let (vouch, metadata) = rest.split_at(VOUCH);
        let client_key = PublicKey::from_bytes(client_key.try_into().expect("32 octets"));
        let vouched = open(
            &BoxKey::between(client_key.as_bytes(), &transient.secret)?,
            &long_nonce(b"VOUCH---", vouch_nonce),
            vouch,
        )?;
        // libzmq checks only C'; RFC 26 asks for S too.
        if vouched[..KEY] != client_transient || vouched[KEY..] != *self.public.as_bytes() {
            return Err(HandshakeError::Cryptographic);
        }
        let metadata =
            parse_metadata(metadata, "INITIATE").map_err(HandshakeError::MalformedCommand)?;

        let request = self.config.zap_request(ZapCredentials::Curve(client_key));
        let pending = PendingCurveAuthentication {
            config: self.config,
            version: self.version,
            metadata,
            key,
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
        let sealed = seal(&self.key, &short_nonce(READY_PREFIX, 1), &metadata);
        let mut ready = Vec::with_capacity(8 + sealed.len());
        ready.extend_from_slice(&1u64.to_be_bytes());
        ready.extend_from_slice(&sealed);
        write_raw_command(self.version, "READY", &ready, out);
        established.authenticated = Some(authenticated);
        let codec = CurveCodec {
            key: self.key,
            send_prefix: SERVER_MESSAGE_PREFIX,
            receive_prefix: CLIENT_MESSAGE_PREFIX,
            next_nonce: 2,
            peer_nonce: self.peer_nonce,
            partial: None,
            partial_size: 0,
            max_message_size: None,
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
            let flags = if i < last { BOX_MORE } else { 0 };
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
        self.encode_frame(BOX_COMMAND, body, encoder, out)
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
        // Framed and sealed in place in `out`: MESSAGE, the short nonce,
        // the authenticator, then flags and payload, encrypted where they
        // were copied.
        let boxed = 1 + payload.len();
        encoder.encode_frame_header(MESSAGE.len() + SHORT_NONCE + TAG + boxed, out);
        out.extend_from_slice(MESSAGE);
        out.extend_from_slice(&nonce.to_be_bytes());
        let tag = out.len();
        out.extend_from_slice(&[0; TAG]);
        out.push(flags);
        out.extend_from_slice(payload);
        let mut mac = [0; TAG];
        crypto_box_detached_afternm_inplace(
            &mut out[tag + TAG..],
            &mut mac,
            &short_nonce(self.send_prefix, nonce),
            &self.key.0,
        );
        out[tag..tag + TAG].copy_from_slice(&mac);
        Ok(())
    }

    /// Fails a message once its frames total more than `max` octets,
    /// counted as [`DecoderConfig::max_message_size`] counts them. The
    /// decoder only sees boxed frames one at a time.
    ///
    /// [`DecoderConfig::max_message_size`]: crate::DecoderConfig::max_message_size
    pub fn set_max_message_size(&mut self, max: Option<u64>) {
        self.max_message_size = max;
    }

    /// Unboxes one item from the decoder. Returns a whole message once its
    /// last frame arrives, or a command the peer boxed.
    pub fn decode(&mut self, incoming: Incoming) -> Result<Option<Incoming>, CurveError> {
        let body = match incoming {
            Incoming::Command(body) => body,
            // MESSAGE is never sent with MORE: each frame is boxed alone.
            Incoming::Message(message) if message.len().get() > 1 => {
                return Err(CurveError::NotMessage);
            }
            // Taken, not cloned: cloning a frame that owns its buffer
            // allocates a shared header for it.
            Incoming::Message(message) => message.into_iter().next().expect("one frame"),
        };
        let data = body.strip_prefix(MESSAGE).ok_or(CurveError::NotMessage)?;
        if data.len() < SHORT_NONCE + TAG + 1 {
            return Err(CurveError::Malformed);
        }
        let (nonce, sealed) = data.split_at(SHORT_NONCE);
        let nonce = u64::from_be_bytes(nonce.try_into().expect("8 octets"));
        if nonce <= self.peer_nonce {
            return Err(CurveError::StaleNonce);
        }
        let (mac, ciphertext) = sealed.split_at(TAG);
        // Opened in place in the one copy that becomes the frame.
        let mut opened = ciphertext.to_vec();
        crypto_box_open_detached_afternm_inplace(
            &mut opened,
            mac.try_into().expect("16 octets"),
            &short_nonce(self.receive_prefix, nonce),
            &self.key.0,
        )
        .map_err(|_| CurveError::Cryptographic)?;
        self.peer_nonce = nonce;
        let flags = opened[0];
        // Advanced past the flags rather than sliced, which, like a clone,
        // would allocate a shared header.
        let mut opened = Bytes::from(opened);
        opened.advance(1);
        let payload = Frame::from_shared(opened);
        if flags & BOX_COMMAND != 0 {
            if self.partial.is_some() {
                return Err(CurveError::Malformed);
            }
            return Ok(Some(Incoming::Command(payload)));
        }
        self.partial_size = self
            .partial_size
            .saturating_add(counted_size(payload.len() as u64));
        if self
            .max_message_size
            .is_some_and(|max| self.partial_size > max)
        {
            return Err(CurveError::MessageTooLarge);
        }
        let message = match self.partial.take() {
            Some(mut message) => {
                message.push(payload);
                message
            }
            None => Message::new(payload),
        };
        if flags & BOX_MORE != 0 {
            self.partial = Some(message);
            return Ok(None);
        }
        self.partial_size = 0;
        Ok(Some(Incoming::Message(message)))
    }
}

/// Seals `plaintext` as NaCl does: authenticator, then ciphertext.
fn seal(key: &BoxKey, nonce: &[u8; 24], plaintext: &[u8]) -> Vec<u8> {
    let mut sealed = vec![0; TAG + plaintext.len()];
    let (tag, ciphertext) = sealed.split_at_mut(TAG);
    ciphertext.copy_from_slice(plaintext);
    let mut mac = [0; TAG];
    crypto_box_detached_afternm_inplace(ciphertext, &mut mac, nonce, &key.0);
    tag.copy_from_slice(&mac);
    sealed
}

/// Opens a box sealed by [`seal`].
fn open(
    key: &BoxKey,
    nonce: &[u8; 24],
    sealed: &[u8],
) -> Result<Zeroizing<Vec<u8>>, HandshakeError> {
    let (tag, ciphertext) = sealed
        .split_at_checked(TAG)
        .ok_or(HandshakeError::Cryptographic)?;
    let mut plaintext = Zeroizing::new(ciphertext.to_vec());
    crypto_box_open_detached_afternm_inplace(
        &mut plaintext,
        tag.try_into().expect("16 octets"),
        nonce,
        &key.0,
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
    dryoc::rng::copy_randombytes(&mut bytes);
    bytes
}

/// A command body's name and data.
fn split_command(body: &[u8]) -> Result<(&[u8], &[u8]), HandshakeError> {
    let (&len, rest) = body
        .split_first()
        .ok_or(HandshakeError::MalformedCommand(CommandError::MissingName))?;
    rest.split_at_checked(usize::from(len))
        .ok_or(HandshakeError::MalformedCommand(CommandError::Malformed {
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
            Self::MessageTooLarge => "CURVE message too large",
        })
    }
}

impl std::error::Error for CurveError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Decoder, DecoderConfig, ErrorReason, ZapFailure};

    #[test]
    fn command_sizes_are_rfc_26s() {
        assert_eq!(HELLO, 194);
        assert_eq!(WELCOME, 160);
        assert_eq!(COOKIE, 96);
        assert_eq!(VOUCH, 80);
    }

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

    fn server_config(socket_type: SocketType) -> ServerConfig {
        ServerConfig {
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
    fn a_multipart_message_over_the_limit_is_refused() {
        let (mut client, mut server) = established();
        server.set_max_message_size(Some(10));
        let at_limit = Message::try_from(vec![
            Frame::from("12345"),
            Frame::from("1234"),
            Frame::new(),
        ])
        .unwrap();
        for _ in 0..2 {
            let frames = boxed(&mut client, &at_limit);
            assert_eq!(
                unbox(&mut server, frames),
                [Incoming::Message(at_limit.clone())]
            );
        }
        let over = Message::try_from(vec![Frame::from("12345"), Frame::from("123456")]).unwrap();
        let mut frames = boxed(&mut client, &over).into_iter();
        let first = Incoming::Message(Message::new(frames.next().unwrap()));
        assert_eq!(server.decode(first), Ok(None));
        let last = Incoming::Message(Message::new(frames.next().unwrap()));
        assert_eq!(server.decode(last), Err(CurveError::MessageTooLarge));
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
        vouched_transient: &Key,
        vouched_server: &PublicKey,
    ) -> Incoming {
        let body = commands(welcome).remove(0);
        let data = &body[8..];
        let opened = open(
            &BoxKey::between(client.server_key.as_bytes(), &client.transient.secret).unwrap(),
            &long_nonce(b"WELCOME-", &data[..16]),
            &data[16..],
        )
        .unwrap();
        let server_transient: Key = opened[..KEY].try_into().unwrap();
        let cookie = &opened[KEY..];
        let vouch_nonce = random::<16>();
        let mut vouched = Vec::new();
        vouched.extend_from_slice(vouched_transient);
        vouched.extend_from_slice(vouched_server.as_bytes());
        let vouch = seal(
            &BoxKey::between(&server_transient, &client.secret).unwrap(),
            &long_nonce(b"VOUCH---", &vouch_nonce),
            &vouched,
        );
        let mut plaintext = Vec::new();
        plaintext.extend_from_slice(client.public.as_bytes());
        plaintext.extend_from_slice(&vouch_nonce);
        plaintext.extend_from_slice(&vouch);
        let sealed = seal(
            &BoxKey::between(&server_transient, &client.transient.secret).unwrap(),
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
        let other_transient = Transient::generate().public;
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
                (other_transient, server_keys.public())
            } else {
                (client.transient.public, other_server)
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
            &client.transient.public,
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
