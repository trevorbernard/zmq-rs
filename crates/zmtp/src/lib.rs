//! ZeroMQ Message Transport Protocol as a sans-IO state machine.
//!
//! Greeting and version negotiation (ZMTP 1.0, 2.0, 3.0, 3.1), framing codecs,
//! typed commands and metadata, and the NULL, PLAIN and CURVE security
//! mechanisms. No I/O: bytes in, events out.
//!
//! Specs: RFC 13, 15, 23, 24, 25, 26, 27, 32, 37. See `docs/rfc-compliance.md`.

mod codec;
mod command;
mod curve;
mod curve_keys;
mod frame;
mod greeting;
mod handshake;
mod identity;
mod mechanism;
mod membership;
mod message;
mod plain;
mod socket_type;
mod subscription;
pub mod z85;
mod zap;

pub use codec::{
    CommandsUnsupported, DecodeError, Decoder, DecoderConfig, Encoder, FrameWalker, Incoming,
    MAX_FRAME_HEADER, frame_header,
};
pub use command::{
    Command, CommandError, CommandName, ErrorReason, Group, InvalidGroup, InvalidName, Metadata,
    Ping, PingContext, PropertyName, TooLong, Ttl,
};
pub use curve::{
    CurveClient, CurveClientStep, CurveCodec, CurveError, CurveServer, CurveServerConfig,
    CurveServerStep, PendingCurveAuthentication,
};
pub use curve_keys::{KeyError, Keypair, PublicKey, SecretKey};
pub use frame::Frame;
pub use greeting::{Feed, Greeting, GreetingConfig, GreetingError, PeerGreeting, Replay, Version};
pub use handshake::{
    Established, HandshakeError, NullHandshake, NullZapConfig, PendingNullAuthentication,
};
pub use identity::{Identity, InvalidIdentity};
pub use mechanism::{LegacyPeers, Mechanism, Role};
pub use membership::Membership;
pub use message::{EmptyMessage, IntoIter, Iter, Message};
pub use plain::{
    ClientStep, PendingAuthentication, PlainClient, PlainServer, PlainServerConfig, ServerStep,
};
pub use socket_type::SocketType;
pub use subscription::Subscription;
pub use zap::{
    Authenticated, Credential, PlainCredentials, ZapCredentials, ZapFailure, ZapReply, ZapRequest,
};
