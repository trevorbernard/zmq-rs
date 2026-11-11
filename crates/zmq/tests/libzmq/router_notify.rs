//! libzmq's `tests/test_router_notify.cpp`: `ZMQ_ROUTER_NOTIFY`, an empty
//! message a ROUTER receives from a peer that connected or disconnected.
//! Its option-value test is left out, as the option is a struct here.

use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

use zmq::{Context, Dealer, Identity, Message, Router, RouterNotify, SocketOptions};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, within};

const SETTLE: Duration = Duration::from_millis(300);

fn named(identity: &'static str) -> SocketOptions {
    SocketOptions {
        identity: Identity::new(identity).unwrap(),
        ..SocketOptions::default()
    }
}

fn recv(router: zmq::Socket<Router>) -> (Vec<u8>, Message, zmq::Socket<Router>) {
    within(FIVE, move || {
        let mut router = router;
        let (from, message) = router.recv().unwrap();
        (from.as_bytes().to_vec(), message, router)
    })
}

fn router_notify(notify: RouterNotify) {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.set_notify(notify);
    let endpoint = router.bind(&free_endpoint()).unwrap().to_string();

    let mut dealer = context.socket_with::<Dealer>(named("X"));
    dealer.connect(&endpoint).unwrap();

    if notify.connect {
        let (from, message, back) = recv(router);
        router = back;
        assert_eq!(from, b"X");
        assert_eq!(message, Message::from(""));
    }

    dealer.send("Hello").unwrap();
    let (from, message, back) = recv(router);
    router = back;
    assert_eq!(from, b"X");
    assert_eq!(message, Message::from("Hello"));

    dealer.disconnect(&endpoint).unwrap();

    if notify.disconnect {
        let (from, message, back) = recv(router);
        router = back;
        assert_eq!(from, b"X");
        assert_eq!(message, Message::from(""));
    }
    assert_eq!(router.recv_timeout(SETTLE), Ok(None));
}

#[test]
fn router_notify_connect() {
    router_notify(RouterNotify {
        connect: true,
        disconnect: false,
    });
}

#[test]
fn router_notify_disconnect() {
    router_notify(RouterNotify {
        connect: false,
        disconnect: true,
    });
}

#[test]
fn router_notify_both() {
    router_notify(RouterNotify {
        connect: true,
        disconnect: true,
    });
}

/// libzmq sends this through a STREAM socket; a plain TCP stream does the
/// same.
#[test]
fn handshake_fail() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.set_notify(RouterNotify {
        connect: true,
        disconnect: true,
    });
    let endpoint = router.bind(&free_endpoint()).unwrap();

    let mut stream = TcpStream::connect(endpoint.tcp_addr().unwrap()).unwrap();
    stream.write_all(b"not-a-handshake").unwrap();
    drop(stream);

    // No notification is delivered.
    assert_eq!(router.recv_timeout(Duration::from_millis(200)), Ok(None));
}

/// A disconnection in the middle of a multipart message: the incomplete
/// message is discarded and the notification arrives as a message of its
/// own.
#[test]
fn error_during_multipart() {
    let context = Context::new().unwrap();
    let mut router = context.socket_with::<Router>(SocketOptions {
        // The handshake fails if this is too small.
        max_message_size: Some(64),
        ..SocketOptions::default()
    });
    router.set_notify(RouterNotify {
        connect: false,
        disconnect: true,
    });
    let endpoint = router.bind(&free_endpoint()).unwrap().to_string();

    let mut dealer = context.socket_with::<Dealer>(named("X"));
    dealer.connect(&endpoint).unwrap();

    // The second part makes the router drop the connection.
    let mut message = Message::from("Hello2");
    message.push("*".repeat(127));
    dealer.send(message).unwrap();

    let (from, message, _router) = recv(router);
    assert_eq!(from, b"X");
    assert_eq!(message, Message::from(""));
}
