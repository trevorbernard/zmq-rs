//! libzmq's `tests/test_xpub_nodrop.cpp`: with `ZMQ_XPUB_NODROP`, an XPUB
//! never drops a message for a full subscriber, and refuses to send
//! instead.
//!
//! zmq-rs keeps both behaviors and lets the caller choose per send:
//! [`send_all`](zmq::Socket::send_all) waits for room, and
//! [`try_send_all`](zmq::Socket::try_send_all) hands the message back,
//! where libzmq sets an option and a send timeout.

use std::num::NonZeroUsize;
use std::time::Duration;

use zmq::{Context, Hwm, Message, SocketOptions, Sub, XPub, XPubEvent};

use crate::support::FIVE;

const HWM: usize = 2000;

#[test]
fn test() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket_with::<XPub>(SocketOptions {
        send_hwm: Hwm::Messages(NonZeroUsize::new(HWM).unwrap()),
        ..SocketOptions::default()
    });
    publisher.bind("inproc://libzmq-xpub-nodrop").unwrap();

    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("inproc://libzmq-xpub-nodrop").unwrap();
    subscriber.subscribe("");

    // We must wait for the subscription to be processed here, otherwise
    // some or all published messages might be lost
    assert_eq!(
        publisher.recv_timeout(FIVE),
        Some(XPubEvent::Subscribe("".into()))
    );

    let send_count = HWM - 1;
    for _ in 0..send_count {
        assert_eq!(publisher.send_all(Message::from("")), 1);
    }
    let mut recv_count = 0;
    if subscriber.recv_timeout(FIVE).is_some() {
        recv_count += 1;
        while subscriber
            .recv_timeout(Duration::from_millis(250))
            .is_some()
        {
            recv_count += 1;
        }
    }
    assert_eq!(recv_count, send_count);

    // Now test real blocking behavior: send until the subscriber is full,
    // when the send is refused rather than dropped
    let mut send_count = 0;
    while publisher.try_send_all(Message::from("")).is_ok() {
        send_count += 1;
    }
    let mut recv_count = 0;
    if send_count > 0 {
        subscriber.recv_timeout(FIVE).expect("a first message");
        recv_count += 1;
        while subscriber.try_recv().is_some() {
            recv_count += 1;
        }
    }
    assert_eq!(recv_count, send_count);
}
