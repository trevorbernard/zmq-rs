//! libzmq's `tests/test_hello_msg.cpp`: `ZMQ_HELLO_MSG`, what a socket
//! sends each peer first.

use zmq::{Client, Context, Dealer, Frame, Message, Router, Server};
use zmq_test_ports::free_endpoint;

use crate::support::FIVE;

fn test(address: &str) {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.set_hello_message("H");
    let endpoint = router.bind(address).unwrap().to_string();

    let mut dealer = context.socket::<Dealer>();
    dealer.connect(&endpoint).unwrap();

    let hello = dealer.recv_timeout(FIVE).unwrap().expect("the hello");
    assert_eq!(hello, Message::from("H"));
}

#[test]
fn tcp() {
    test(&free_endpoint());
}

#[test]
fn inproc() {
    test("inproc://hello-msg");
}

#[test]
fn inproc_late_bind() {
    let address = "inproc://late-hello-msg";
    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>();
    server.set_hello_message("W");

    let mut client = context.socket::<Client>();
    client.set_hello_message("H");
    client.connect(address).unwrap();

    server.bind(address).unwrap();

    let hello = client
        .recv_timeout(FIVE)
        .unwrap()
        .expect("the server's hello");
    assert_eq!(hello, Frame::from("W"));

    let (_, hello) = server
        .recv_timeout(FIVE)
        .unwrap()
        .expect("the client's hello");
    assert_eq!(hello, Frame::from("H"));
}
