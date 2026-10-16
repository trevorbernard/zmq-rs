//! ZeroMQ Message Transport Protocol as a sans-IO state machine.
//!
//! Greeting and version negotiation (ZMTP 1.0, 2.0, 3.0, 3.1), framing codecs,
//! typed commands and metadata, and the NULL, PLAIN and CURVE security
//! mechanisms. No I/O: bytes in, events out.
//!
//! Specs: RFC 13, 15, 23, 24, 25, 26, 27, 32, 37. See `docs/rfc-compliance.md`.

mod frame;
mod message;

pub use frame::Frame;
pub use message::{EmptyMessage, IntoIter, Iter, Message};
