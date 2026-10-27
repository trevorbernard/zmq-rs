//! Interoperability tests against the reference libzmq.
//!
//! [`libzmq`] drives a real libzmq socket; [`Peer`] talks to it using only
//! the `zmtp` crate. The tests live in `tests/`.

use std::env;

pub mod libzmq;
mod peer;

pub use peer::{ConnectError, Peer, Security};

/// Set by the Nix dev shell and CI, which provide everything the tests
/// need: a test that would skip fails instead.
const STRICT: &str = "ZMQ_INTEROP_STRICT";

/// Says why a test cannot run here and lets it pass, or fails it when
/// [`STRICT`] is set.
pub fn skip(reason: &str) {
    assert!(
        env::var_os(STRICT).is_none(),
        "{reason}, and {STRICT} is set"
    );
    eprintln!("skipped: {reason}");
}
