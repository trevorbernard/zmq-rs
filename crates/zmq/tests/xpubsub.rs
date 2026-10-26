//! XPUB and XSUB (RFC 29): publishers that hear subscriptions, subscribers
//! that can talk upstream, and a proxy built from them.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Frame, Message, Pub, Sub, XPub, XPubEvent, XSub};

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

fn subscribed(topic: &'static str) -> XPubEvent {
    XPubEvent::Subscribe(Frame::from(topic))
}

fn cancelled(topic: &'static str) -> XPubEvent {
    XPubEvent::Cancel(Frame::from(topic))
}

#[test]
fn xpub_hears_subscriptions_once_per_topic() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.bind("inproc://hear").unwrap();
    let mut first = context.socket::<Sub>();
    first.connect("inproc://hear").unwrap();
    let mut second = context.socket::<Sub>();
    second.connect("inproc://hear").unwrap();
    first.subscribe("x");
    second.subscribe("x");
    assert_eq!(xpub.try_recv(), Some(subscribed("x")));
    assert_eq!(xpub.try_recv(), None, "the second subscriber is not news");
    first.unsubscribe("x");
    assert_eq!(xpub.try_recv(), None);
    second.unsubscribe("x");
    assert_eq!(xpub.try_recv(), Some(cancelled("x")));
}

#[test]
fn xpub_hears_a_departing_subscriber_cancel() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.bind("inproc://depart").unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("inproc://depart").unwrap();
    subscriber.subscribe("x");
    assert_eq!(xpub.try_recv(), Some(subscribed("x")));
    drop(subscriber);
    assert_eq!(within(FIVE, move || xpub.recv()), cancelled("x"));
}

#[test]
fn xpub_publishes_like_pub() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.bind("inproc://publish").unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("inproc://publish").unwrap();
    subscriber.subscribe("news");
    assert_eq!(xpub.send("sports"), 0);
    assert_eq!(xpub.send("news"), 1);
    assert_eq!(subscriber.try_recv(), Some(Message::from("news")));
}

#[test]
fn xsub_talks_upstream_and_filters_downstream() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.bind("inproc://upstream").unwrap();
    let mut xsub = context.socket::<XSub>();
    xsub.connect("inproc://upstream").unwrap();
    xsub.send("hello upstream");
    xsub.subscribe("a");
    assert_eq!(
        xpub.try_recv(),
        Some(XPubEvent::Message(Message::from("hello upstream")))
    );
    assert_eq!(xpub.try_recv(), Some(subscribed("a")));
    assert_eq!(xpub.send("a1"), 1);
    assert_eq!(xsub.try_recv(), Some(Message::from("a1")));
}

#[test]
fn xsub_sending_a_subscription_subscribes() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://raw-subscription").unwrap();
    let mut xsub = context.socket::<XSub>();
    xsub.connect("inproc://raw-subscription").unwrap();
    xsub.send(&b"\x01a"[..]);
    assert_eq!(publisher.send("a1"), 1);
    assert_eq!(
        xsub.try_recv(),
        Some(Message::from("a1")),
        "and filters by it"
    );
}

#[test]
fn a_proxy_forwards_subscriptions_up_and_messages_down() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    let upstream = publisher.bind("tcp://127.0.0.1:*").unwrap();
    let mut frontend = context.socket::<XSub>();
    frontend.connect(&upstream.to_string()).unwrap();
    let mut backend = context.socket::<XPub>();
    backend.bind("inproc://proxy").unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let proxy = thread::spawn({
        let stop = stop.clone();
        move || {
            while !stop.load(Ordering::Relaxed) {
                let mut idle = true;
                while let Some(event) = backend.try_recv() {
                    frontend.forward(event);
                    idle = false;
                }
                while let Some(message) = frontend.try_recv() {
                    backend.send(message);
                    idle = false;
                }
                if idle {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
    });

    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("inproc://proxy").unwrap();
    subscriber.subscribe("news");
    let started = Instant::now();
    while publisher.send("news: through the proxy") == 0 {
        assert!(
            started.elapsed() < FIVE,
            "the subscription never got upstream"
        );
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        publisher.send("sports"),
        0,
        "only wanted topics go upstream"
    );
    assert_eq!(
        within(FIVE, move || subscriber.recv()),
        Message::from("news: through the proxy")
    );
    stop.store(true, Ordering::Relaxed);
    proxy.join().unwrap();
}

#[test]
fn forwarding_a_cancel_unsubscribes() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://forward-cancel").unwrap();
    let mut xsub = context.socket::<XSub>();
    xsub.connect("inproc://forward-cancel").unwrap();
    xsub.forward(subscribed("x"));
    assert_eq!(publisher.send("x1"), 1);
    xsub.forward(cancelled("x"));
    assert_eq!(publisher.send("x2"), 0);
}

#[test]
fn xsub_subscriptions_are_never_dropped_however_many() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://xsub-many-topics").unwrap();
    let mut xsub = context.socket::<XSub>();
    for i in 0..3000 {
        xsub.subscribe(format!("topic {i}"));
    }
    xsub.connect("inproc://xsub-many-topics").unwrap();
    for i in 0..3000 {
        assert_eq!(publisher.send(format!("topic {i}")), 1, "topic {i}");
        assert!(xsub.try_recv().is_some());
    }
}

#[test]
fn a_flooding_subscriber_cannot_keep_xpub_from_returning() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.bind("inproc://flood").unwrap();
    let mut xsub = context.socket::<XSub>();
    xsub.connect("inproc://flood").unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let flooding = {
        let stop = stop.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                xsub.send(Message::from(&b"\x01a"[..]));
            }
        })
    };
    let heard = within(FIVE, move || {
        let first = xpub.recv();
        (first, xpub.recv_timeout(Duration::from_millis(200)))
    });
    stop.store(true, Ordering::Relaxed);
    flooding.join().unwrap();
    assert_eq!(heard, (subscribed("a"), None));
}

#[test]
fn xsub_drops_messages_at_the_hwm_but_never_subscriptions() {
    let marks = |n| zmq::SocketOptions {
        send_hwm: zmq::Hwm::Messages(std::num::NonZeroUsize::new(n).unwrap()),
        recv_hwm: zmq::Hwm::Messages(std::num::NonZeroUsize::new(n).unwrap()),
        ..zmq::SocketOptions::default()
    };
    let context = Context::new().unwrap();
    let mut xpub = context.socket_with::<XPub>(marks(1));
    xpub.bind("inproc://xsub-hwm").unwrap();
    let mut xsub = context.socket_with::<XSub>(marks(2));
    xsub.connect("inproc://xsub-hwm").unwrap();
    for i in 0..10 {
        xsub.send(format!("upstream {i}"));
    }
    xsub.send(Message::from(&b"\x01news"[..]));
    let events: Vec<XPubEvent> = std::iter::from_fn(|| xpub.try_recv()).collect();
    // An inproc pipe holds what both ends would: 2 + 1.
    let expected: Vec<XPubEvent> = (0..3)
        .map(|i| XPubEvent::Message(Message::from(format!("upstream {i}"))))
        .chain([subscribed("news")])
        .collect();
    assert_eq!(events, expected);
}
