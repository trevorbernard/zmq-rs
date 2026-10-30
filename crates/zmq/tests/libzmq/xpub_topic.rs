//! libzmq's `tests/test_xpub_topic.cpp`: an XPUB hears subscriptions and
//! cancels to topics whose subscribe command is 255 octets and longer.

use zmq::{Context, Socket, Sub, XPub, XPubEvent};
use zmq_test_ports::free_endpoint;

use crate::support::FIVE;

/// 245 octets: with the 10 of a SUBSCRIBE command, 255.
fn short_topic() -> Vec<u8> {
    topic(245)
}

/// 246 octets: with the 10 of a SUBSCRIBE command, 256.
fn long_topic() -> Vec<u8> {
    topic(246)
}

fn topic(len: usize) -> Vec<u8> {
    b"ABCDEFGHIJKLMNOP"
        .iter()
        .copied()
        .cycle()
        .take(len)
        .collect()
}

fn subscribe_cancel(xpub: &mut Socket<XPub>, sub: &mut Socket<Sub>, topic: &[u8]) {
    sub.subscribe(topic);
    assert_eq!(
        xpub.recv_timeout(FIVE),
        Some(XPubEvent::Subscribe(topic.to_vec().into()))
    );
    assert!(sub.unsubscribe(topic));
    assert_eq!(
        xpub.recv_timeout(FIVE),
        Some(XPubEvent::Cancel(topic.to_vec().into()))
    );
}

#[test]
fn xpub_subscribe_long_topic() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    let endpoint = xpub.bind(&free_endpoint()).unwrap().to_string();
    let mut sub = context.socket::<Sub>();
    sub.connect(&endpoint).unwrap();

    subscribe_cancel(&mut xpub, &mut sub, &short_topic());
    subscribe_cancel(&mut xpub, &mut sub, &long_topic());
}
