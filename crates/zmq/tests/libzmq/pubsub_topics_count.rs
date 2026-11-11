//! libzmq's `tests/test_pubsub_topics_count.cpp`: `ZMQ_TOPICS_COUNT`, the
//! number of distinct topics a subscriber holds, or a publisher's
//! subscribers hold between them.
//!
//! libzmq sleeps and then reads `ZMQ_EVENTS` so that the publisher applies
//! the subscriptions before it counts. Ours applies them as it counts, so
//! the publisher's count is waited for instead.

use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Pub, Socket, Sub};

use crate::support::FIVE;

fn expect_count(publisher: &mut Socket<Pub>, subscriber: &Socket<Sub>, count: usize) {
    assert_eq!(subscriber.topics_count(), count);
    let deadline = Instant::now() + FIVE;
    while publisher.topics_count() != count {
        assert!(
            Instant::now() < deadline,
            "the publisher counts {} topics, not {count}",
            publisher.topics_count()
        );
        thread::sleep(Duration::from_millis(1));
    }
}

fn pair(context: &Context) -> (Socket<Pub>, Socket<Sub>) {
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://soname").unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("inproc://soname").unwrap();
    (publisher, subscriber)
}

#[test]
fn independent_topic_prefixes() {
    let context = Context::new().unwrap();
    let (mut publisher, mut subscriber) = pair(&context);

    subscriber.subscribe("topicprefix1");
    subscriber.subscribe("topicprefix2");
    subscriber.subscribe("topicprefix3");
    expect_count(&mut publisher, &subscriber, 3);

    subscriber.unsubscribe("topicprefix3");
    expect_count(&mut publisher, &subscriber, 2);

    subscriber.unsubscribe("topicprefix1");
    subscriber.unsubscribe("topicprefix2");
    expect_count(&mut publisher, &subscriber, 0);
}

#[test]
fn nested_topic_prefixes() {
    let context = Context::new().unwrap();
    let (mut publisher, mut subscriber) = pair(&context);

    subscriber.subscribe("a");
    subscriber.subscribe("ab");
    subscriber.subscribe("abc");
    // Nested one in the other, they still count as three.
    expect_count(&mut publisher, &subscriber, 3);

    subscriber.subscribe("xyz");
    subscriber.subscribe("xy");
    subscriber.subscribe("x");
    expect_count(&mut publisher, &subscriber, 6);
}
