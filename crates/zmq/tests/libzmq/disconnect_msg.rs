//! libzmq's `tests/test_disconnect_msg.cpp`: `ZMQ_DISCONNECT_MSG`, what a
//! socket receives from a peer it accepted when that peer leaves.

use zmq::{Client, Context, Frame, Server, Socket};
use zmq_test_ports::free_endpoint;

use crate::support::FIVE;

/// `leave` returns the client if it stays open, as libzmq's does after a
/// disconnect.
fn test(address: &str, leave: impl FnOnce(Socket<Client>, &str) -> Option<Socket<Client>>) {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>();
    server.set_disconnect_message("D");
    let endpoint = server.bind(address).unwrap().to_string();

    let mut client = context.socket::<Client>();
    client.set_hello_message("H");
    client.connect(&endpoint).unwrap();

    let (who, hello) = server.recv_timeout(FIVE).unwrap().expect("the hello");
    assert_eq!(hello, Frame::from("H"));

    let _client = leave(client, &endpoint);

    let (left, disconnect) = server
        .recv_timeout(FIVE)
        .unwrap()
        .expect("the disconnect message");
    assert_eq!(disconnect, Frame::from("D"));
    assert_eq!(left, who, "from the client that left");
}

#[test]
fn tcp() {
    test(&free_endpoint(), |_, _| None);
}

#[test]
fn inproc() {
    test("inproc://disconnect-msg", |_, _| None);
}

#[test]
fn inproc_disconnect() {
    test(
        "inproc://disconnect-msg-disconnect",
        |mut client, endpoint| {
            client.disconnect(endpoint).unwrap();
            Some(client)
        },
    );
}
