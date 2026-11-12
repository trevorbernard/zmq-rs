//! libzmq's `tests/test_hiccup_msg.cpp`: `ZMQ_HICCUP_MSG`, what a socket
//! receives when a connection it made breaks.

use zmq::{Client, Context, Frame, Server};
use zmq_test_ports::free_endpoint;

use crate::support::FIVE;

#[test]
fn test() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>();
    let address = server.bind(&free_endpoint()).unwrap().to_string();

    let mut client = context.socket::<Client>();
    client.set_hello_message("HELLO");
    client.set_hiccup_message("HICCUP");
    client.connect(&address).unwrap();

    let (_, hello) = server.recv_timeout(FIVE).unwrap().expect("the hello");
    assert_eq!(hello, Frame::from("HELLO"));

    drop(server);

    let hiccup = client.recv_timeout(FIVE).unwrap().expect("the hiccup");
    assert_eq!(hiccup, Frame::from("HICCUP"));
}
