//! libzmq's `tests/test_immediate.cpp`: without `ZMQ_IMMEDIATE` a PUSH
//! queues messages for a peer still connecting; with it, only completed
//! connections take messages, and a broken one stops taking them until it
//! is back.

use std::thread;
use std::time::Duration;

use zmq::{Context, Dealer, Linger, Message, Pull, Push, Socket, SocketOptions};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, SETTLE, settle};

fn options(immediate: bool) -> SocketOptions {
    SocketOptions {
        linger: Linger::For(Duration::ZERO),
        immediate,
        ..SocketOptions::default()
    }
}

/// Receives until nothing arrives for 250 ms, as libzmq's `ZMQ_RCVTIMEO`
/// loop does.
fn count_received(socket: &mut Socket<Pull>) -> usize {
    let mut seen = 0;
    while socket.recv_timeout(Duration::from_millis(250)).is_some() {
        seen += 1;
    }
    seen
}

/// PUSH load-balances over a connected pipe and one that will never
/// connect, as `connect` makes a pipe at once, so half of the messages
/// queue for the one that never connects.
#[test]
fn immediate_1() {
    let context = Context::new().unwrap();
    let mut to = context.socket_with::<Pull>(options(false));
    let endpoint = to.bind(&free_endpoint()).unwrap().to_string();

    let mut from = context.socket_with::<Push>(options(false));
    from.connect(&free_endpoint()).unwrap();
    from.connect(&endpoint).unwrap();
    settle();

    for _ in 0..10 {
        from.send("Hello");
    }
    assert_eq!(count_received(&mut to), 5);
}

/// With IMMEDIATE, the pipe to the peer that never connects is not made,
/// so every message goes to the one that does.
#[test]
fn immediate_2() {
    let context = Context::new().unwrap();
    let mut to = context.socket_with::<Pull>(options(false));
    let endpoint = to.bind(&free_endpoint()).unwrap().to_string();

    let mut from = context.socket_with::<Push>(options(true));
    from.connect(&free_endpoint()).unwrap();
    from.connect(&endpoint).unwrap();

    for _ in 0..10 {
        assert!(from.send_timeout("Hello", FIVE).is_ok());
    }
    assert_eq!(count_received(&mut to), 10);
}

/// A connection that breaks stops taking messages until it is back.
#[test]
fn immediate_3() {
    let context = Context::new().unwrap();
    let mut backend = context.socket_with::<Dealer>(options(false));
    let mut frontend = context.socket_with::<Dealer>(options(true));
    let endpoint = backend.bind(&free_endpoint()).unwrap().to_string();
    frontend.connect(&endpoint).unwrap();

    // Ping backend to frontend so we know when the connection is up
    assert!(backend.send_timeout("Hello", FIVE).is_ok());
    assert_eq!(frontend.recv_timeout(FIVE), Some(Message::from("Hello")));

    assert!(frontend.try_send("Hello").is_ok());

    drop(backend);
    // Give time to process the disconnect
    thread::sleep(SETTLE * 10);

    assert!(frontend.try_send("Hello").is_err(), "no connection");

    let mut backend = context.socket_with::<Dealer>(options(false));
    backend.bind(&endpoint).unwrap();
    assert!(backend.send_timeout("Hello", FIVE).is_ok());
    assert_eq!(frontend.recv_timeout(FIVE), Some(Message::from("Hello")));

    // After the reconnect, it should succeed
    assert!(frontend.try_send("Hello").is_ok());
}
