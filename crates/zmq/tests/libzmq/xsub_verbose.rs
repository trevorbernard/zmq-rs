//! libzmq's `tests/test_xsub_verbose.cpp`: `ZMQ_XSUB_VERBOSE_UNSUBSCRIBE`,
//! with which an XSUB passes every unsubscription to its publishers, not
//! only the one that ends a topic.
//!
//! Where libzmq checks with a non-blocking receive that nothing was
//! reported, this waits [`SETTLE`] for it, so that a report on its way is
//! not missed.
//!
//! libzmq's XPUB reports an unsubscription of a topic the peer does not
//! hold, which is how its test sees the ones the option lets through. Ours
//! ignores those (`docs/rfc-compliance.md`, PUBSUB), so here the
//! option shows in the first of two unsubscriptions being reported, and
//! what goes on the wire is checked in `zmq-core`'s `pubsub` tests.

use zmq::{Context, Socket, Verbosity, XPub, XPubEvent, XSub};

use crate::support::{FIVE, SETTLE};

const TOPIC_A: &str = "A";

fn expect(publisher: &mut Socket<XPub>, event: XPubEvent) {
    assert_eq!(publisher.recv_timeout(FIVE).unwrap(), Some(event));
}

fn expect_nothing(publisher: &mut Socket<XPub>) {
    assert_eq!(publisher.recv_timeout(SETTLE).unwrap(), None);
}

#[test]
fn xsub_verbose_unsubscribe() {
    let subscribed = || XPubEvent::Subscribe(TOPIC_A.into());
    let cancelled = || XPubEvent::Cancel(TOPIC_A.into());

    let context = Context::new().unwrap();
    let mut publisher = context.socket::<XPub>();
    publisher.bind("inproc://soketto").unwrap();
    let mut subscriber = context.socket::<XSub>();
    subscriber.connect("inproc://soketto").unwrap();

    // To be told of every subscription and unsubscription.
    publisher.set_verbosity(Verbosity::Everything);

    // Not subscribed yet, so the XSUB filters it out.
    subscriber.unsubscribe(TOPIC_A);
    expect_nothing(&mut publisher);

    subscriber.subscribe(TOPIC_A);
    expect(&mut publisher, subscribed());
    subscriber.subscribe(TOPIC_A);
    expect(&mut publisher, subscribed());

    // The first unsubscription is filtered out.
    subscriber.unsubscribe(TOPIC_A);
    expect_nothing(&mut publisher);
    subscriber.unsubscribe(TOPIC_A);
    expect(&mut publisher, cancelled());

    subscriber.set_verbose_unsubscribe(true);

    // Not subscribed, but passed on all the same; libzmq's XPUB reports
    // it, ours ignores it.
    subscriber.unsubscribe(TOPIC_A);
    expect_nothing(&mut publisher);

    subscriber.subscribe(TOPIC_A);
    expect(&mut publisher, subscribed());
    subscriber.subscribe(TOPIC_A);
    expect(&mut publisher, subscribed());

    // The first unsubscription is passed on now, and ends the XPUB's one
    // subscription for this peer; libzmq's XPUB reports the second too.
    subscriber.unsubscribe(TOPIC_A);
    expect(&mut publisher, cancelled());
    subscriber.unsubscribe(TOPIC_A);
    expect_nothing(&mut publisher);
}
