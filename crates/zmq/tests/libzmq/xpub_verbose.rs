//! libzmq's `tests/test_xpub_verbose.cpp`: which subscriptions and cancels
//! an XPUB reports with `ZMQ_XPUB_VERBOSE` and `ZMQ_XPUB_VERBOSER`, here
//! [`Verbosity::Subscriptions`] and [`Verbosity::Everything`].
//!
//! Where libzmq checks with a non-blocking receive that nothing was
//! reported, these wait [`SETTLE`] for it, so that a report on its way is
//! not missed.

use zmq::{Context, Message, Socket, Sub, Verbosity, XPub, XPubEvent};

use crate::support::{FIVE, SETTLE};

const TOPIC_A: &str = "A";
const TOPIC_B: &str = "B";

fn subscribed(topic: &'static str) -> Option<XPubEvent> {
    Some(XPubEvent::Subscribe(topic.into()))
}

fn cancelled(topic: &'static str) -> Option<XPubEvent> {
    Some(XPubEvent::Cancel(topic.into()))
}

fn expect(publisher: &mut Socket<XPub>, event: Option<XPubEvent>) {
    assert_eq!(publisher.recv_timeout(FIVE), event);
}

fn expect_nothing(publisher: &mut Socket<XPub>) {
    assert_eq!(publisher.recv_timeout(SETTLE), None);
}

fn expect_message(subscriber: &mut Socket<Sub>, topic: &'static str) {
    assert_eq!(subscriber.recv_timeout(FIVE), Some(Message::from(topic)));
}

fn xpub_with_subs<const N: usize>(endpoint: &str) -> (Socket<XPub>, [Socket<Sub>; N]) {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<XPub>();
    publisher.bind(endpoint).unwrap();
    let subscribers = std::array::from_fn(|_| {
        let mut subscriber = context.socket::<Sub>();
        subscriber.connect(endpoint).unwrap();
        subscriber
    });
    (publisher, subscribers)
}

/// sub0 subscribes to A; sub1's subscription to A, a duplicate, is
/// filtered out by the XPUB.
fn create_duplicate_subscription(
    publisher: &mut Socket<XPub>,
    sub0: &mut Socket<Sub>,
    sub1: &mut Socket<Sub>,
) {
    sub0.subscribe(TOPIC_A);
    expect(publisher, subscribed(TOPIC_A));
    sub1.subscribe(TOPIC_A);
    expect_nothing(publisher);
}

#[test]
fn xpub_verbose_one_sub() {
    let (mut publisher, [mut sub]) = xpub_with_subs("inproc://libzmq-xpub-verbose-1");

    sub.subscribe(TOPIC_A);
    expect(&mut publisher, subscribed(TOPIC_A));
    sub.subscribe(TOPIC_B);
    expect(&mut publisher, subscribed(TOPIC_B));

    // Subscribe again for A: it is duplicated, so it will be filtered out
    sub.subscribe(TOPIC_A);
    expect_nothing(&mut publisher);

    publisher.set_verbosity(Verbosity::Subscriptions);
    // This time with VERBOSE the duplicated subscription will be received
    sub.subscribe(TOPIC_A);
    expect(&mut publisher, subscribed(TOPIC_A));

    publisher.send(TOPIC_A);
    publisher.send(TOPIC_B);
    expect_message(&mut sub, TOPIC_A);
    expect_message(&mut sub, TOPIC_B);
}

#[test]
fn xpub_verbose_two_subs() {
    let (mut publisher, [mut sub0, mut sub1]) = xpub_with_subs("inproc://libzmq-xpub-verbose-2");
    create_duplicate_subscription(&mut publisher, &mut sub0, &mut sub1);

    sub0.subscribe(TOPIC_B);
    expect(&mut publisher, subscribed(TOPIC_B));

    publisher.set_verbosity(Verbosity::Subscriptions);
    // This time with VERBOSE the duplicated subscription will be received
    sub1.subscribe(TOPIC_A);
    expect(&mut publisher, subscribed(TOPIC_A));

    publisher.send(TOPIC_A);
    publisher.send(TOPIC_B);
    expect_message(&mut sub0, TOPIC_A);
    expect_message(&mut sub1, TOPIC_A);
    expect_message(&mut sub0, TOPIC_B);
}

#[test]
fn xpub_verboser_one_sub() {
    let (mut publisher, [mut sub]) = xpub_with_subs("inproc://libzmq-xpub-verboser-1");

    // Unsubscribe for A, which does not exist yet, so the SUB filters it
    assert!(!sub.unsubscribe(TOPIC_A));
    expect_nothing(&mut publisher);

    sub.subscribe(TOPIC_A);
    expect(&mut publisher, subscribed(TOPIC_A));

    // Subscribe again for A: the SUB counts it, and the XPUB filters out
    // the duplicate
    sub.subscribe(TOPIC_A);
    expect_nothing(&mut publisher);

    // The SUB sends a cancel only once its cancels match its earlier
    // subscriptions
    assert!(sub.unsubscribe(TOPIC_A));
    assert!(sub.unsubscribe(TOPIC_A));
    expect(&mut publisher, cancelled(TOPIC_A));
    expect_nothing(&mut publisher);

    // Unsubscribe for A, which does not exist anymore
    assert!(!sub.unsubscribe(TOPIC_A));
    expect_nothing(&mut publisher);

    publisher.set_verbosity(Verbosity::Everything);
    sub.subscribe(TOPIC_A);
    expect(&mut publisher, subscribed(TOPIC_A));

    // Make sure everything still works
    publisher.send(TOPIC_A);
    expect_message(&mut sub, TOPIC_A);

    assert!(sub.unsubscribe(TOPIC_A));
    expect(&mut publisher, cancelled(TOPIC_A));

    // The SUB sends a cancel only for what it had subscribed to, even
    // with VERBOSER
    assert!(!sub.unsubscribe(TOPIC_A));
    expect_nothing(&mut publisher);
}

#[test]
fn xpub_verboser_two_subs() {
    let (mut publisher, [mut sub0, mut sub1]) = xpub_with_subs("inproc://libzmq-xpub-verboser-2");
    create_duplicate_subscription(&mut publisher, &mut sub0, &mut sub1);

    // sub1 is still subscribed, so no notification
    assert!(sub0.unsubscribe(TOPIC_A));
    expect_nothing(&mut publisher);

    // Unsubscribe the second socket to trigger the notification, once
    assert!(sub1.unsubscribe(TOPIC_A));
    expect(&mut publisher, cancelled(TOPIC_A));
    expect_nothing(&mut publisher);

    publisher.set_verbosity(Verbosity::Everything);
    sub0.subscribe(TOPIC_A);
    sub1.subscribe(TOPIC_A);
    // VERBOSER is set, so subscriptions from both sockets are received
    expect(&mut publisher, subscribed(TOPIC_A));
    expect(&mut publisher, subscribed(TOPIC_A));

    // Make sure everything still works
    publisher.send(TOPIC_A);
    expect_message(&mut sub0, TOPIC_A);
    expect_message(&mut sub1, TOPIC_A);

    // VERBOSER reports each socket's cancel
    assert!(sub1.unsubscribe(TOPIC_A));
    expect(&mut publisher, cancelled(TOPIC_A));
    assert!(sub0.unsubscribe(TOPIC_A));
    expect(&mut publisher, cancelled(TOPIC_A));

    // Unmatched, so the SUB filters it even with VERBOSER
    assert!(!sub1.unsubscribe(TOPIC_A));
    expect_nothing(&mut publisher);
}
