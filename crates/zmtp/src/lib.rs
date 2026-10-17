//! ZeroMQ Message Transport Protocol as a sans-IO state machine.
//!
//! Greeting and version negotiation (ZMTP 1.0, 2.0, 3.0, 3.1), framing codecs,
//! typed commands and metadata, and the NULL, PLAIN and CURVE security
//! mechanisms. No I/O: bytes in, events out.
//!
//! Specs: RFC 13, 15, 23, 24, 25, 26, 27, 32, 37. See `docs/rfc-compliance.md`.

mod codec;
mod command;
mod frame;
mod greeting;
mod handshake;
mod identity;
mod mechanism;
mod message;
mod socket_type;

pub use codec::{CommandsUnsupported, DecodeError, Decoder, DecoderConfig, Encoder, Incoming};
pub use command::{
    Command, CommandError, CommandName, ErrorReason, Group, InvalidGroup, InvalidName, Metadata,
    Ping, PingContext, PropertyName, TooLong, Ttl,
};
pub use frame::Frame;
pub use greeting::{Feed, Greeting, GreetingConfig, GreetingError, PeerGreeting, Replay, Version};
pub use handshake::{Established, HandshakeError, NullHandshake};
pub use identity::{Identity, InvalidIdentity};
pub use mechanism::{LegacyPeers, Mechanism, Role};
pub use message::{EmptyMessage, IntoIter, Iter, Message};
pub use socket_type::SocketType;
