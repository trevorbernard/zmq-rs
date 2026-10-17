//! Interoperability tests against the reference libzmq.
//!
//! [`libzmq`] drives a real libzmq socket; [`Peer`] talks to it using only
//! the `zmtp` crate. The tests live in `tests/`.

pub mod libzmq;
mod peer;

pub use peer::{ConnectError, Peer, Security};
