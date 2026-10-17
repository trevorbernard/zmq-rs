//! XPUB's and PUB's options: libzmq's `ZMQ_XPUB_WELCOME_MSG`,
//! `ZMQ_XPUB_VERBOSE`, `ZMQ_XPUB_VERBOSER`, `ZMQ_XPUB_NODROP` and
//! `ZMQ_INVERT_MATCHING`.

use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Hwm, Message, Pub, SocketOptions, Sub, Verbosity, XPub, XPubEvent};

const FIVE: Duration = Duration::from_secs(5);

fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
        .recv_timeout(limit)
        .unwrap_or_else(|_| panic!("timed out after {limit:?}"))
}

fn subscribed(topic: &str) -> XPubEvent {
    XPubEvent::Subscribe(topic.to_owned().into())
}

fn cancelled(topic: &str) -> XPubEvent {
    XPubEvent::Cancel(topic.to_owned().into())
}

/// Every event the XPUB has, once `count` have arrived and a moment has
/// passed for any more.
fn events(xpub: &mut zmq::Socket<XPub>, count: usize) -> Vec<XPubEvent> {
    let started = Instant::now();
    let mut events = Vec::new();
    while events.len() < count {
        assert!(started.elapsed() < FIVE, "only {events:?}");
        match xpub.try_recv() {
            Some(event) => events.push(event),
            None => thread::sleep(Duration::from_millis(1)),
        }
    }
    thread::sleep(Duration::from_millis(50));
    events.extend(std::iter::from_fn(|| xpub.try_recv()));
    events
}

fn subscriber(context: &Context, endpoint: &str, topic: &str) -> zmq::Socket<Sub> {
    let mut sub = context.socket::<Sub>();
    sub.subscribe(topic);
    sub.connect(endpoint).unwrap();
    sub
}

#[test]
fn an_xpub_welcomes_each_new_subscriber() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.set_welcome(Some(Message::from("WELCOME")));
    xpub.bind("inproc://welcome").unwrap();
    let mut sub = subscriber(&context, "inproc://welcome", "WELCOME");
    let (done, welcomed) = mpsc::channel();
    thread::spawn(move || done.send(sub.recv()));
    let started = Instant::now();
    let welcome = loop {
        assert!(started.elapsed() < FIVE, "never welcomed");
        // The XPUB greets peers as it takes them in.
        let _ = xpub.try_recv();
        if let Ok(welcome) = welcomed.try_recv() {
            break welcome;
        }
        thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(welcome, Message::from("WELCOME"));
}

#[test]
fn by_default_an_xpub_reports_first_subscriptions_and_last_cancels() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.bind("inproc://quiet").unwrap();
    let mut first = subscriber(&context, "inproc://quiet", "a");
    let mut second = subscriber(&context, "inproc://quiet", "a");
    assert_eq!(events(&mut xpub, 1), [subscribed("a")]);
    first.unsubscribe("a");
    second.unsubscribe("a");
    assert_eq!(events(&mut xpub, 1), [cancelled("a")]);
}

#[test]
fn a_verbose_xpub_reports_every_subscription() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.set_verbosity(Verbosity::Subscriptions);
    xpub.bind("inproc://verbose").unwrap();
    let mut first = subscriber(&context, "inproc://verbose", "a");
    let mut second = subscriber(&context, "inproc://verbose", "a");
    second.subscribe("a");
    assert_eq!(events(&mut xpub, 3), vec![subscribed("a"); 3]);
    first.unsubscribe("a");
    second.unsubscribe("a");
    second.unsubscribe("a");
    assert_eq!(events(&mut xpub, 1), [cancelled("a")]);
}

#[test]
fn a_verboser_xpub_reports_every_cancel_too() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.set_verbosity(Verbosity::Everything);
    xpub.bind("inproc://verboser").unwrap();
    let mut first = subscriber(&context, "inproc://verboser", "a");
    let second = subscriber(&context, "inproc://verboser", "a");
    assert_eq!(events(&mut xpub, 2), vec![subscribed("a"); 2]);
    first.unsubscribe("a");
    assert_eq!(events(&mut xpub, 1), [cancelled("a")]);
    drop(second);
    assert_eq!(events(&mut xpub, 1), [cancelled("a")]);
}

#[test]
fn a_verboser_xpub_reports_a_departing_peers_shared_topics() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.set_verbosity(Verbosity::Everything);
    xpub.bind("inproc://departing").unwrap();
    let _stays = subscriber(&context, "inproc://departing", "a");
    let leaves = subscriber(&context, "inproc://departing", "a");
    assert_eq!(events(&mut xpub, 2), vec![subscribed("a"); 2]);
    drop(leaves);
    assert_eq!(events(&mut xpub, 1), [cancelled("a")]);
}

#[test]
fn inverted_matching_publishes_what_does_not_match() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.set_invert_matching(true);
    publisher.bind("inproc://inverted").unwrap();
    let mut sub = context.socket::<Sub>();
    sub.set_invert_matching(true);
    sub.subscribe("a");
    sub.connect("inproc://inverted").unwrap();
    // Over inproc the subscription arrives with the peer.
    let started = Instant::now();
    while publisher.send("b0") == 0 {
        assert!(started.elapsed() < FIVE, "never connected");
        thread::yield_now();
    }
    assert_eq!(publisher.send("a1"), 0);
    assert_eq!(publisher.send("b1"), 1);
    assert_eq!(sub.recv(), Message::from("b0"));
    assert_eq!(sub.recv(), Message::from("b1"));
}

#[test]
fn an_inverted_xpub_skips_matching_subscribers() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.set_invert_matching(true);
    xpub.bind("inproc://inverted-x").unwrap();
    let _sub = subscriber(&context, "inproc://inverted-x", "a");
    assert_eq!(events(&mut xpub, 1), [subscribed("a")]);
    assert_eq!(xpub.send("a1"), 0);
    assert_eq!(xpub.send("b1"), 1);
}

fn one() -> Hwm {
    Hwm::Messages(NonZeroUsize::MIN)
}

/// A PUB with one slow subscriber to "a" and one to "b", both of which
/// have subscribed and queue at most two messages.
fn publisher_and_slow_subscribers(
    context: &Context,
    name: &str,
) -> (zmq::Socket<Pub>, zmq::Socket<Sub>, zmq::Socket<Sub>) {
    let mut publisher = context.socket_with::<Pub>(SocketOptions {
        send_hwm: one(),
        ..SocketOptions::default()
    });
    let endpoint = format!("inproc://{name}");
    publisher.bind(&endpoint).unwrap();
    let slow = |topic: &str| {
        let mut sub = context.socket_with::<Sub>(SocketOptions {
            recv_hwm: one(),
            ..SocketOptions::default()
        });
        sub.subscribe(topic);
        sub.connect(&endpoint).unwrap();
        sub
    };
    let (a, b) = (slow("a"), slow("b"));
    // Each then has one message queued.
    for topic in ["a", "b"] {
        let started = Instant::now();
        while publisher.send(topic) == 0 {
            assert!(started.elapsed() < FIVE, "never subscribed");
            thread::yield_now();
        }
    }
    (publisher, a, b)
}

#[test]
fn try_send_all_hands_back_what_a_full_subscriber_would_miss() {
    let context = Context::new().unwrap();
    let (mut publisher, _a, _b) = publisher_and_slow_subscribers(&context, "nodrop");
    assert_eq!(publisher.try_send_all("a"), Ok(1));
    assert_eq!(publisher.try_send_all("a"), Err(Message::from("a")));
    assert_eq!(publisher.try_send_all("b"), Ok(1), "only a is full");
    assert_eq!(publisher.try_send_all("c"), Ok(0));
}

#[test]
fn send_all_waits_for_every_matching_subscriber() {
    let context = Context::new().unwrap();
    let (mut publisher, mut a, _b) = publisher_and_slow_subscribers(&context, "nodrop-wait");
    publisher.try_send_all("a").unwrap();
    let sender = thread::spawn(move || publisher.send_all("a-last"));
    thread::sleep(Duration::from_millis(50));
    assert!(!sender.is_finished(), "waits for room");
    let received = within(FIVE, move || [a.recv(), a.recv(), a.recv()]);
    assert_eq!(received, ["a", "a", "a-last"].map(Message::from));
    assert_eq!(sender.join().unwrap(), 1);
}

#[test]
fn an_xpub_can_send_without_dropping_too() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket_with::<XPub>(SocketOptions {
        send_hwm: one(),
        ..SocketOptions::default()
    });
    xpub.bind("inproc://xnodrop").unwrap();
    let mut sub = context.socket_with::<Sub>(SocketOptions {
        recv_hwm: one(),
        ..SocketOptions::default()
    });
    sub.subscribe("a");
    sub.connect("inproc://xnodrop").unwrap();
    assert_eq!(events(&mut xpub, 1), [subscribed("a")]);
    assert_eq!(xpub.try_send_all("a"), Ok(1));
    assert_eq!(xpub.try_send_all("a"), Ok(1));
    assert_eq!(xpub.try_send_all("a"), Err(Message::from("a")));
    let _sub = within(FIVE, move || {
        sub.recv();
        sub
    });
    assert_eq!(xpub.send_all("a"), 1);
}
