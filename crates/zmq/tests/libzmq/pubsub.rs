//! libzmq's `tests/test_pubsub.cpp`: a subscriber to everything receives
//! what a publisher sends.
//!
//! libzmq runs this case only over NORM, which zmq-rs does not have; it
//! runs here over the transports zmq-rs does.

use zmq::{Context, Message, Pub, Sub};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, settle};

fn test(address: &str) {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    let endpoint = publisher.bind(address).unwrap().to_string();
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect(&endpoint).unwrap();
    subscriber.subscribe("");

    // Wait a bit till the subscription gets to the publisher
    settle();

    publisher.send("test");
    assert_eq!(subscriber.recv_timeout(FIVE), Some(Message::from("test")));
}

#[test]
fn tcp() {
    test(&free_endpoint());
}

#[cfg(unix)]
#[test]
fn ipc() {
    test("ipc://*");
}

#[test]
fn inproc() {
    test("inproc://libzmq-pubsub");
}
