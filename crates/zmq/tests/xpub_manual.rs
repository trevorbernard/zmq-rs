//! An XPUB that decides its peers' subscriptions, as libzmq's
//! `ZMQ_XPUB_MANUAL` and `ZMQ_XPUB_MANUAL_LAST_VALUE`.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, ManualXPub, Message, Sub, XPubEvent};

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

fn bound(context: &Context, name: &str) -> zmq::Socket<ManualXPub> {
    let mut xpub = context.socket::<ManualXPub>();
    xpub.bind(&format!("inproc://{name}")).unwrap();
    xpub
}

fn subscriber(context: &Context, name: &str, topic: &str) -> zmq::Socket<Sub> {
    let mut sub = context.socket::<Sub>();
    sub.subscribe(topic);
    sub.connect(&format!("inproc://{name}")).unwrap();
    sub
}

#[test]
fn a_subscription_takes_effect_once_accepted() {
    let context = Context::new().unwrap();
    let mut xpub = bound(&context, "accept");
    let mut sub = subscriber(&context, "accept", "a");
    let mut request = xpub.recv();
    assert_eq!(request.event(), &subscribed("a"));
    assert!(request.accept());
    drop(request);
    assert_eq!(xpub.send("a1"), 1);
    assert_eq!(within(FIVE, move || sub.recv()), Message::from("a1"));
}

#[test]
fn a_subscription_left_unaccepted_has_no_effect() {
    let context = Context::new().unwrap();
    let mut xpub = bound(&context, "ignored");
    let _sub = subscriber(&context, "ignored", "a");
    drop(xpub.recv());
    assert_eq!(xpub.send("a1"), 0);
}

#[test]
fn the_application_chooses_what_a_peer_subscribes_to() {
    let context = Context::new().unwrap();
    let mut xpub = bound(&context, "choose");
    let _sub = subscriber(&context, "choose", "");
    let mut request = xpub.recv();
    assert_eq!(request.event(), &subscribed(""));
    assert!(request.subscribe("allowed"));
    drop(request);
    assert_eq!(xpub.send("secret"), 0);
    assert_eq!(xpub.send("allowed: news"), 1);
}

#[test]
fn a_cancel_takes_effect_once_accepted() {
    let context = Context::new().unwrap();
    let mut xpub = bound(&context, "cancel");
    let mut sub = subscriber(&context, "cancel", "a");
    xpub.recv().accept();
    sub.unsubscribe("a");
    let mut xpub = within(FIVE, move || {
        assert_eq!(xpub.recv().event(), &cancelled("a"));
        xpub
    });
    assert_eq!(xpub.send("a1"), 1, "still subscribed until accepted");
    sub.subscribe("b");
    sub.unsubscribe("b");
    xpub.recv().accept();
    let mut cancel = xpub.recv();
    assert_eq!(cancel.event(), &cancelled("b"));
    assert!(cancel.accept());
    drop(cancel);
    assert_eq!(xpub.send("b1"), 0, "cancelled once accepted");
}

#[test]
fn every_subscription_is_a_request() {
    let context = Context::new().unwrap();
    let mut xpub = bound(&context, "every");
    let _first = subscriber(&context, "every", "a");
    let _second = subscriber(&context, "every", "a");
    assert_eq!(xpub.recv().event(), &subscribed("a"));
    assert_eq!(xpub.recv().event(), &subscribed("a"));
}

#[test]
fn a_request_can_answer_its_peer_alone() {
    let context = Context::new().unwrap();
    let mut xpub = bound(&context, "last-value");
    let mut first = subscriber(&context, "last-value", "a");
    xpub.recv().accept();
    let mut second = subscriber(&context, "last-value", "a");
    let mut request = xpub.recv();
    assert!(request.accept());
    assert!(!request.send("b: not wanted"));
    assert!(request.send("a: the latest"));
    drop(request);
    assert_eq!(
        within(FIVE, move || second.recv()),
        Message::from("a: the latest")
    );
    thread::sleep(Duration::from_millis(50));
    assert_eq!(first.try_recv(), None, "only the new subscriber");
}

#[test]
fn a_departed_peers_topics_are_cancelled_with_no_peer_to_answer() {
    let context = Context::new().unwrap();
    let mut xpub = bound(&context, "departed");
    let sub = subscriber(&context, "departed", "a");
    xpub.recv().accept();
    drop(sub);
    let mut request = xpub.recv();
    assert_eq!(request.event(), &cancelled("a"));
    assert!(!request.subscribe("b"), "nobody to subscribe");
    drop(request);
    assert_eq!(xpub.send("a1"), 0);
}
