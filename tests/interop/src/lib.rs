//! Interoperability tests against the reference libzmq.
//!
//! [`libzmq`] drives a real libzmq socket; [`Peer`] talks to it using only
//! the `zmtp` crate. The tests live in `tests/`.

use std::env;
use std::net::{SocketAddr, TcpListener};
use std::process;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU16, Ordering};

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

/// A port with nothing listening on it yet, for a test to bind later. It is
/// below the ephemeral range, so no outgoing connection or bind to port 0
/// can take it while the test waits, and starts from the process id, as
/// nextest gives each test its own process: a shared start would have tests
/// bind, and reach, each other's ports.
pub fn free_port() -> SocketAddr {
    static NEXT: LazyLock<AtomicU16> =
        LazyLock::new(|| AtomicU16::new(20000 + (process::id() % 12000) as u16));
    loop {
        let port = NEXT.fetch_add(1, Ordering::Relaxed);
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) {
            return listener.local_addr().unwrap();
        }
    }
}
