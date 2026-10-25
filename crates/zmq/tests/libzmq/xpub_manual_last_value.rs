//! libzmq's `tests/test_xpub_manual_last_value.cpp`: with
//! `ZMQ_XPUB_MANUAL_LAST_VALUE`, a manual XPUB sends to the subscriber
//! whose subscription it just confirmed.
//!
//! In libzmq that is a socket option: the first send after a confirmed
//! subscription goes to its subscriber alone. In zmq-rs it is explicit:
//! [`SubscriptionRequest::send`](zmq::SubscriptionRequest::send) sends
//! to that subscriber, and a plain send reaches every subscriber, as
//! `docs/rfc-compliance.md` says of `ManualXPub`. The cases the two
//! libzmq tests share run as `xpub_manual` runs them.

use std::num::NonZeroUsize;

use zmq::{Context, Hwm, ManualXPub, Message, SocketOptions, Sub};

use crate::support::SETTLE;
use crate::xpub_manual::{cases, expect, expect_nothing, on_request, subscribed};

#[test]
fn basic() {
    cases::basic();
}

#[test]
fn unsubscribe_manual() {
    cases::unsubscribe_manual();
}

#[test]
fn xpub_proxy_unsubscribe_on_disconnect() {
    cases::xpub_proxy_unsubscribe_on_disconnect(true);
}

/// libzmq sends the first message after the second subscriber's
/// confirmation to it alone, so the first subscriber never gets its
/// message. Here that message is a plain send, which reaches it.
#[test]
fn missing_subscriptions() {
    cases::missing_subscriptions();
}

#[test]
fn unsubscribe_cleanup() {
    cases::unsubscribe_cleanup();
}

#[test]
fn manual_last_value() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket_with::<ManualXPub>(SocketOptions {
        send_hwm: Hwm::Messages(NonZeroUsize::new(2000).unwrap()),
        ..SocketOptions::default()
    });
    publisher
        .bind("inproc://libzmq-xpub-manual-last-value")
        .unwrap();
    let mut sub = context.socket::<Sub>();
    sub.connect("inproc://libzmq-xpub-manual-last-value")
        .unwrap();
    let mut sub2 = context.socket::<Sub>();
    sub2.connect("inproc://libzmq-xpub-manual-last-value")
        .unwrap();

    sub.subscribe("A");
    on_request(&mut publisher, subscribed("A"), |request| {
        assert!(request.subscribe("A"));
        assert!(request.send("A"));
    });
    expect(&mut sub, Message::from("A"));

    sub2.subscribe("A");
    on_request(&mut publisher, subscribed("A"), |request| {
        assert!(request.subscribe("A"));
        assert!(request.send("A"));
    });
    expect(&mut sub2, Message::from("A"));

    // sub won't get a message: it went to sub2's subscription alone
    expect_nothing(&mut sub, SETTLE);
}
