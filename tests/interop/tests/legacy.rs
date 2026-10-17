//! Unversioned ZMTP 1.0 peers, which send no greeting: their identity frame
//! comes first.

use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq_core::{ConnectionConfig, Context};
use zmtp::{Frame, Message, SocketType, Version};

const FIVE: Duration = Duration::from_secs(5);

/// Runs `f` on another thread and fails the test if it takes longer than
/// `limit`, so a lost wakeup fails instead of hanging the suite.
fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
        .recv_timeout(limit)
        .unwrap_or_else(|_| panic!("timed out after {limit:?}"))
}

#[test]
fn an_unversioned_zmtp_1_0_peer_completes_the_handshake() {
    let context = Context::new().unwrap();
    let mut listener = context
        .bind(
            "127.0.0.1:0".parse().unwrap(),
            ConnectionConfig::new(SocketType::Pull),
        )
        .unwrap();
    let mut stream = TcpStream::connect(listener.local_addr()).unwrap();
    // ZMTP 1.0 frames: length (counting the flags octet), flags, body.
    stream.write_all(b"\x03\x00me").unwrap();
    stream.write_all(b"\x06\x00hello").unwrap();
    let (established, message) = within(FIVE, move || {
        let mut connection = listener.accept_blocking().unwrap();
        (connection.peer(), connection.rx.recv_blocking())
    });
    let established = established.expect("handshake completed");
    assert_eq!(established.version, Version::V1);
    assert_eq!(established.peer_identity, Frame::from("me"));
    assert_eq!(message, Some(Message::from("hello")));
}
