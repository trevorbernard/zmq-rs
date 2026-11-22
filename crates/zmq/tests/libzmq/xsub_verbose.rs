//! libzmq's `tests/test_xsub_verbose.cpp`: `ZMQ_XSUB_VERBOSE_UNSUBSCRIBE`,
//! with which an XSUB passes every unsubscription to its publishers, not
//! only the one that ends a topic.
//!
//! Where libzmq checks with a non-blocking receive that nothing was
//! reported, this waits [`SETTLE`] for it, so that a report on its way is
//! not missed.

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

    // Not subscribed, but passed on all the same.
    subscriber.unsubscribe(TOPIC_A);
    expect(&mut publisher, cancelled());

    subscriber.subscribe(TOPIC_A);
    expect(&mut publisher, subscribed());
    subscriber.subscribe(TOPIC_A);
    expect(&mut publisher, subscribed());

    // Both unsubscriptions are passed on now.
    subscriber.unsubscribe(TOPIC_A);
    expect(&mut publisher, cancelled());
    subscriber.unsubscribe(TOPIC_A);
    expect(&mut publisher, cancelled());
}
