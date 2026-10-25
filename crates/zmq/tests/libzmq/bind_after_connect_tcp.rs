//! libzmq's `tests/test_bind_after_connect_tcp.cpp`: messages sent before
//! anything listens are delivered once something binds.

use std::net::TcpListener;

use zmq::{Context, Dealer, Message};

use crate::support::{FIVE, within};

/// A TCP endpoint on a port free now, standing in for libzmq's fixed
/// `ENDPOINT_3`.
fn free_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    format!("tcp://{}", listener.local_addr().unwrap())
}

#[test]
fn x() {
    let endpoint = free_endpoint();
    let context = Context::new().unwrap();
    let mut server = context.socket::<Dealer>();
    let mut client = context.socket::<Dealer>();
    client.connect(&endpoint).unwrap();
    client.send("foobar");
    client.send("baz");
    client.send("buzz");
    server.bind(&endpoint).unwrap();
    within(FIVE, move || {
        assert_eq!(server.recv(), Message::from("foobar"));
        assert_eq!(server.recv(), Message::from("baz"));
        assert_eq!(server.recv(), Message::from("buzz"));
    });
}
