//! libzmq's `tests/test_bind_after_connect_tcp.cpp`: messages sent before
//! anything listens are delivered once something binds.

use zmq::{Context, Dealer, Message};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, within};

#[test]
fn x() {
    let endpoint = free_endpoint();
    let context = Context::new().unwrap();
    let mut server = context.socket::<Dealer>();
    let mut client = context.socket::<Dealer>();
    client.connect(&endpoint).unwrap();
    client.send("foobar").unwrap();
    client.send("baz").unwrap();
    client.send("buzz").unwrap();
    server.bind(&endpoint).unwrap();
    within(FIVE, move || {
        assert_eq!(server.recv().unwrap(), Message::from("foobar"));
        assert_eq!(server.recv().unwrap(), Message::from("baz"));
        assert_eq!(server.recv().unwrap(), Message::from("buzz"));
    });
}
