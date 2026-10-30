//! libzmq's `tests/test_sub_forward.cpp`: an XPUB and XSUB passing a
//! subscription upstream and a message downstream, as a device does.

use zmq::{Context, Message, Pub, Sub, XPub, XSub};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, settle};

#[test]
fn test() {
    let context = Context::new().unwrap();
    // First, create an intermediate device
    let mut xpub = context.socket::<XPub>();
    let endpoint1 = xpub.bind(&free_endpoint()).unwrap().to_string();
    let mut xsub = context.socket::<XSub>();
    let endpoint2 = xsub.bind(&free_endpoint()).unwrap().to_string();

    let mut publisher = context.socket::<Pub>();
    publisher.connect(&endpoint2).unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect(&endpoint1).unwrap();
    subscriber.subscribe("");

    // Pass the subscription upstream through the device
    let subscription = xpub.recv_timeout(FIVE).expect("a subscription");
    xsub.forward(subscription);

    // Wait a bit till the subscription gets to the publisher
    settle();

    publisher.send("");

    // Pass the message downstream through the device
    let message = xsub.recv_timeout(FIVE).expect("a message");
    xpub.send(message);

    assert_eq!(subscriber.recv_timeout(FIVE), Some(Message::from("")));
}
