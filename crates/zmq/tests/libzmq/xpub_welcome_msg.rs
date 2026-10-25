//! libzmq's `tests/test_xpub_welcome_msg.cpp`: an XPUB's welcome message
//! reaches a subscriber subscribed to it.

use zmq::{Context, Message, Sub, XPub, XPubEvent};

use crate::support::FIVE;

#[test]
fn test() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<XPub>();
    publisher.bind("inproc://libzmq-xpub-welcome-msg").unwrap();
    publisher.set_welcome(Some(Message::from("W")));

    let mut subscriber = context.socket::<Sub>();
    // Subscribe to the welcome message
    subscriber.subscribe("W");
    subscriber
        .connect("inproc://libzmq-xpub-welcome-msg")
        .unwrap();

    // Receive the welcome subscription
    assert_eq!(
        publisher.recv_timeout(FIVE),
        Some(XPubEvent::Subscribe("W".into()))
    );
    // Receive the welcome message
    assert_eq!(subscriber.recv_timeout(FIVE), Some(Message::from("W")));
}
