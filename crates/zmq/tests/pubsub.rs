//! PUB and SUB (RFC 29): topics, filtering, fan-out, and a publisher that
//! never blocks.

use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Hwm, Message, Pub, SocketOptions, Sub};

const FIVE: Duration = Duration::from_secs(5);

/// Runs `f` on another thread and fails the test if it takes longer than
/// `limit`, so a lost wakeup fails instead of hanging the suite.
fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
        .recv_timeout(limit)
        .unwrap_or_else(|_| panic!("timed out after {limit:?}"))
}

/// Publishes `message` until `subscribers` peers take it, as subscriptions
/// travel upstream after connecting. Returns how many attempts went to
/// fewer.
fn publish_until(publisher: &mut zmq::Socket<Pub>, message: &str, subscribers: usize) -> usize {
    let started = Instant::now();
    let mut missed = 0;
    while publisher.send(message.to_owned()) < subscribers {
        missed += 1;
        assert!(started.elapsed() < FIVE, "subscribers never arrived");
        thread::sleep(Duration::from_millis(5));
    }
    missed
}

#[test]
fn subscribers_receive_only_their_topics_over_tcp() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    let bound = publisher.bind("tcp://127.0.0.1:*").unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.subscribe("news");
    subscriber.connect(&bound.to_string()).unwrap();

    publish_until(&mut publisher, "news: first", 1);
    assert_eq!(publisher.send("sports"), 0, "nobody wants sports");
    assert_eq!(publisher.send("news: second"), 1);
    let received = within(FIVE, move || [subscriber.recv(), subscriber.recv()]);
    assert_eq!(
        received,
        [Message::from("news: first"), Message::from("news: second")]
    );
}

#[test]
fn subscribing_after_connecting_works_too() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://late-subscription").unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("inproc://late-subscription").unwrap();
    assert_eq!(publisher.send("before"), 0);
    subscriber.subscribe("");
    assert_eq!(publisher.send("after"), 1);
    assert_eq!(
        within(FIVE, move || subscriber.recv()),
        Message::from("after")
    );
}

#[test]
fn a_subscriber_that_subscribed_to_nothing_receives_nothing() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://silence").unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("inproc://silence").unwrap();
    assert_eq!(publisher.send("x"), 0);
    assert_eq!(subscriber.try_recv(), None);
}

#[test]
fn every_subscriber_gets_a_copy() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://fan-out").unwrap();
    let mut subscribers: Vec<_> = (0..3)
        .map(|_| {
            let mut subscriber = context.socket::<Sub>();
            subscriber.subscribe("");
            subscriber.connect("inproc://fan-out").unwrap();
            subscriber
        })
        .collect();
    assert_eq!(publisher.send("to all"), 3);
    for subscriber in &mut subscribers {
        assert_eq!(subscriber.try_recv(), Some(Message::from("to all")));
    }
}

#[test]
fn unsubscribing_stops_delivery() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://unsubscribe").unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("inproc://unsubscribe").unwrap();
    subscriber.subscribe("a");
    subscriber.subscribe("a");
    assert!(subscriber.unsubscribe("a"));
    assert_eq!(publisher.send("a1"), 1, "one subscription is left");
    assert_eq!(subscriber.try_recv(), Some(Message::from("a1")));
    assert!(subscriber.unsubscribe("a"));
    assert_eq!(publisher.send("a2"), 0);
    assert!(!subscriber.unsubscribe("a"), "no subscription left");
    assert_eq!(subscriber.try_recv(), None);
}

#[test]
fn messages_queued_before_unsubscribing_are_filtered_out() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://in-flight").unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("inproc://in-flight").unwrap();
    subscriber.subscribe("a");
    assert_eq!(publisher.send("a1"), 1);
    subscriber.unsubscribe("a");
    assert_eq!(subscriber.try_recv(), None);
}

#[test]
fn a_publisher_drops_for_slow_subscribers_instead_of_blocking() {
    let context = Context::new().unwrap();
    let one = Hwm::Messages(NonZeroUsize::MIN);
    let mut publisher = context.socket_with::<Pub>(SocketOptions {
        send_hwm: one,
        ..SocketOptions::default()
    });
    publisher.bind("inproc://slow").unwrap();
    let mut subscriber = context.socket_with::<Sub>(SocketOptions {
        recv_hwm: one,
        ..SocketOptions::default()
    });
    subscriber.subscribe("");
    subscriber.connect("inproc://slow").unwrap();
    let delivered: Vec<usize> = (0..5).map(|i| publisher.send(format!("{i}"))).collect();
    assert_eq!(delivered, [1, 1, 0, 0, 0]);
    assert_eq!(subscriber.try_recv(), Some(Message::from("0")));
    assert_eq!(subscriber.try_recv(), Some(Message::from("1")));
    assert_eq!(subscriber.try_recv(), None);
}

#[test]
fn subscriptions_are_never_dropped_however_many() {
    // A SUB's queue towards its peers is unbounded, whatever its HWM.
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://many-topics").unwrap();
    let mut subscriber = context.socket::<Sub>();
    for i in 0..3000 {
        subscriber.subscribe(format!("topic {i}"));
    }
    subscriber.connect("inproc://many-topics").unwrap();
    for i in 0..3000 {
        assert_eq!(publisher.send(format!("topic {i}")), 1, "topic {i}");
        assert!(subscriber.try_recv().is_some());
    }
}

#[test]
fn a_bound_subscriber_tells_publishers_that_connect() {
    let context = Context::new().unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.subscribe("x");
    let bound = subscriber.bind("tcp://127.0.0.1:*").unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.connect(&bound.to_string()).unwrap();
    let (done, received) = mpsc::channel();
    thread::spawn(move || done.send(subscriber.recv()));
    publish_until(&mut publisher, "x marks the spot", 1);
    assert_eq!(
        received.recv_timeout(FIVE).unwrap(),
        Message::from("x marks the spot")
    );
}

#[test]
fn unsubscribing_drops_a_message_a_poll_already_took() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://held").unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.subscribe("a");
    subscriber.connect("inproc://held").unwrap();
    assert_eq!(publisher.send("a1"), 1);
    assert_eq!(
        zmq::poll_timeout([subscriber.readable()], FIVE),
        Some([true])
    );
    assert!(subscriber.unsubscribe("a"));
    assert_eq!(subscriber.try_recv(), None);
}
