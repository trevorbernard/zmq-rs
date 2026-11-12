//! `ZMQ_HELLO_MSG` on our side, as a libzmq peer sees it: an ordinary
//! first message.
#![cfg(feature = "draft")]

use zmq::{Context, Dealer, Router};
use zmq_interop::libzmq::{Kind, Socket};
use zmq_test_ports::free_endpoint;

#[test]
fn a_libzmq_dealer_receives_our_routers_hello_first() {
    let context = Context::new().unwrap();
    let mut ours = context.socket::<Router>();
    ours.set_hello_message("H");
    let endpoint = ours.bind(&free_endpoint()).unwrap();
    let theirs = Socket::new(Kind::Dealer);
    theirs.connect(endpoint);
    assert_eq!(theirs.recv().unwrap(), [b"H".to_vec()]);
}

#[test]
fn a_libzmq_router_receives_our_dealers_hello_first() {
    let theirs = Socket::new(Kind::Router);
    let addr = theirs.bind_loopback();
    let context = Context::new().unwrap();
    let mut ours = context.socket::<Dealer>();
    ours.set_hello_message("H");
    ours.connect(&format!("tcp://{addr}")).unwrap();
    ours.send("after").unwrap();
    let hello = theirs.recv().unwrap();
    assert_eq!(hello.len(), 2, "a routing id and the hello");
    assert_eq!(hello[1], b"H");
    assert_eq!(theirs.recv().unwrap()[1], b"after");
}
