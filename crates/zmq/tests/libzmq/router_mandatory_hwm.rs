//! libzmq's `tests/test_router_mandatory_hwm.cpp`: a mandatory ROUTER
//! reports a full peer rather than dropping, over TCP with high-water
//! marks of one. zmq-rs's ROUTER is always mandatory, and `try_send`
//! reports `Full` where libzmq's `ZMQ_DONTWAIT` send fails with `EAGAIN`.

use std::num::NonZeroUsize;
use std::thread;
use std::time::Duration;

use zmq::{Context, Dealer, Hwm, Identity, Linger, Router, RoutingId, SendError, SocketOptions};
use zmq_test_ports::free_endpoint;

use crate::support::FIVE;

const ONE: Hwm = Hwm::Messages(NonZeroUsize::MIN);
const MESSAGES: usize = 100_000;

/// Sends 64 KiB messages to `to` until its queue is full, counting from
/// `sent`, and returns the new count.
fn send_until_full(router: &mut zmq::Socket<Router>, to: &RoutingId, mut sent: usize) -> usize {
    while sent < MESSAGES {
        match router.try_send(to, vec![0u8; 65536]) {
            Ok(()) => sent += 1,
            Err(SendError::Full(_)) => break,
            Err(SendError::UnknownPeer(_)) => panic!("X is connected"),
        }
    }
    sent
}

#[test]
fn router_mandatory_hwm() {
    let context = Context::new().unwrap();
    let mut router = context.socket_with::<Router>(SocketOptions {
        send_hwm: ONE,
        linger: Linger::For(Duration::from_millis(1)),
        ..SocketOptions::default()
    });
    let endpoint = router.bind(&free_endpoint()).unwrap().to_string();

    let mut dealer = context.socket_with::<Dealer>(SocketOptions {
        identity: Identity::new("X").unwrap(),
        recv_hwm: ONE,
        ..SocketOptions::default()
    });
    dealer.connect(&endpoint).unwrap();
    dealer.send("Hello");
    let (x, _) = router.recv_timeout(FIVE).expect("Hello from X");

    // This should stop after one message, but kernel buffering may let a
    // few more through.
    let sent = send_until_full(&mut router, &x, 0);
    assert!(sent < 10, "{sent} sent before the first stop");

    thread::sleep(Duration::from_secs(1));
    let sent = send_until_full(&mut router, &x, sent);
    assert!(sent < 20, "{sent} sent before the second stop");
}
