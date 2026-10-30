//! PAIR (RFC 31): two sockets, one connection, both directions.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Message, Pair};
use zmq_test_ports::free_endpoint;

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
fn a_pair_talks_both_ways_over_tcp() {
    let context = Context::new().unwrap();
    let mut left = context.socket::<Pair>();
    let bound = left.bind(&free_endpoint()).unwrap();
    let mut right = context.socket::<Pair>();
    right.connect(&bound.to_string()).unwrap();
    right.send("ping");
    let (received, mut left) = within(FIVE, move || (left.recv(), left));
    assert_eq!(received, Message::from("ping"));
    left.send("pong");
    assert_eq!(within(FIVE, move || right.recv()), Message::from("pong"));
}

#[test]
fn a_pair_talks_both_ways_over_inproc() {
    let context = Context::new().unwrap();
    let mut left = context.socket::<Pair>();
    left.bind("inproc://pair").unwrap();
    let mut right = context.socket::<Pair>();
    right.connect("inproc://pair").unwrap();
    left.send("to right");
    right.send("to left");
    assert_eq!(right.try_recv(), Some(Message::from("to right")));
    assert_eq!(left.try_recv(), Some(Message::from("to left")));
}

#[test]
fn sending_waits_until_a_peer_connects() {
    let context = Context::new().unwrap();
    let mut left = context.socket::<Pair>();
    left.bind("inproc://later").unwrap();
    assert_eq!(left.try_send("too early"), Err(Message::from("too early")));
    let sender = thread::spawn(move || left.send("waited"));
    thread::sleep(Duration::from_millis(50));
    let mut right = context.socket::<Pair>();
    right.connect("inproc://later").unwrap();
    assert_eq!(within(FIVE, move || right.recv()), Message::from("waited"));
    sender.join().unwrap();
}

#[test]
fn a_second_peer_is_turned_away() {
    let context = Context::new().unwrap();
    let mut left = context.socket::<Pair>();
    left.bind("inproc://taken").unwrap();
    let mut first = context.socket::<Pair>();
    first.connect("inproc://taken").unwrap();
    let mut second = context.socket::<Pair>();
    second.connect("inproc://taken").unwrap();
    left.send("for the first");
    assert_eq!(first.try_recv(), Some(Message::from("for the first")));
    assert_eq!(second.try_recv(), None);
    assert!(
        second.try_send("anyone?").is_err(),
        "its connection was closed"
    );
}

#[test]
fn the_next_peer_is_accepted_once_the_first_leaves() {
    let context = Context::new().unwrap();
    let mut left = context.socket::<Pair>();
    left.bind("inproc://succession").unwrap();
    let mut first = context.socket::<Pair>();
    first.connect("inproc://succession").unwrap();
    first.send("goodbye");
    drop(first);
    let mut second = context.socket::<Pair>();
    second.connect("inproc://succession").unwrap();
    second.send("hello");
    assert_eq!(left.try_recv(), Some(Message::from("goodbye")));
    assert_eq!(left.try_recv(), Some(Message::from("hello")));
    left.send("welcome");
    assert_eq!(second.try_recv(), Some(Message::from("welcome")));
}
