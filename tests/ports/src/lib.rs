//! TCP ports for tests to bind later, which nothing else takes meanwhile.
//!
//! A port got by binding port 0 and letting go is in the ephemeral range:
//! another test's bind to port 0 can be given it, so that test's socket
//! answers for this one, or this one's bind fails with `AddrInUse`. Use
//! these instead for any port a test names without holding it open: one
//! nobody listens on, one bound after connecting, or one bound again.

use std::env;
use std::fs::{self, File};
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::process;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{LazyLock, Mutex, PoisonError};

/// Below the ephemeral range, so that no bind to port 0 or outgoing
/// connection takes a port while a test waits to use it.
const FIRST: u16 = 20000;
const COUNT: u16 = 12000;

/// The file whose lock claims `port`.
fn claim_path(port: u16) -> std::path::PathBuf {
    env::temp_dir()
        .join("zmq-rs-test-ports")
        .join(port.to_string())
}

/// A loopback address whose port nothing is listening on. The port is this
/// process's until it exits, and no other caller is given it, in this
/// process or any other: the lock on its claim goes with the process, so a
/// crashed test leaves nothing to clean up.
pub fn free_port() -> SocketAddr {
    // nextest runs each test in a process of its own: from the process id,
    // they rarely try the same ports.
    static NEXT: LazyLock<AtomicU32> = LazyLock::new(|| AtomicU32::new(process::id()));
    static CLAIMS: Mutex<Vec<File>> = Mutex::new(Vec::new());

    for _ in 0..COUNT {
        let port = FIRST + (NEXT.fetch_add(1, Ordering::Relaxed) % u32::from(COUNT)) as u16;
        let path = claim_path(port);
        fs::create_dir_all(path.parent().expect("in a directory")).expect("a claims directory");
        let Ok(claim) = File::create(path) else {
            continue;
        };
        if claim.try_lock().is_ok() && TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).is_ok() {
            CLAIMS
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(claim);
            return SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        }
    }
    panic!("every port from {FIRST} on is claimed or in use");
}

/// [`free_port`] as a `tcp://` endpoint.
pub fn free_endpoint() -> String {
    format!("tcp://{}", free_port())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ports_are_in_range_bindable_and_distinct() {
        let [first, second] = [free_port(), free_port()];
        assert_ne!(first, second);
        for address in [first, second] {
            assert!((FIRST..FIRST + COUNT).contains(&address.port()));
            TcpListener::bind(address).unwrap();
        }
    }

    #[test]
    fn a_port_stays_claimed() {
        let port = free_port().port();
        let other = File::create(claim_path(port)).unwrap();
        assert!(other.try_lock().is_err(), "another process could take it");
    }

    #[test]
    fn an_endpoint_names_a_claimed_port() {
        let endpoint = free_endpoint();
        let port: u16 = endpoint.rsplit(':').next().unwrap().parse().unwrap();
        assert!(File::create(claim_path(port)).unwrap().try_lock().is_err());
    }
}
