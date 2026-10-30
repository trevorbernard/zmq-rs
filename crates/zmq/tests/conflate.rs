//! Keeping only the latest message, as libzmq's `ZMQ_CONFLATE`.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Message, Pub, Pull, Push, Sub};
use zmq_test_ports::free_endpoint;

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

#[test]
fn a_conflating_pull_keeps_only_the_latest_message() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.set_conflate(true);
    pull.bind("inproc://latest").unwrap();
    let mut push = context.socket::<Push>();
    push.connect("inproc://latest").unwrap();
    for i in 1..=10 {
        push.send(i.to_string());
    }
    assert_eq!(pull.recv(), Message::from("10"));
    assert_eq!(pull.try_recv(), None);
}

#[test]
fn a_conflating_push_queues_only_the_latest_message() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    push.set_conflate(true);
    push.connect("inproc://later").unwrap();
    for i in 1..=10 {
        push.try_send(i.to_string()).expect("never full");
    }
    let mut pull = context.socket::<Pull>();
    pull.bind("inproc://later").unwrap();
    assert_eq!(pull.recv(), Message::from("10"));
    thread::sleep(Duration::from_millis(50));
    assert_eq!(pull.try_recv(), None);
}

#[test]
fn a_conflating_pull_over_tcp_keeps_only_the_latest_message() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.set_conflate(true);
    let bound = pull.bind(&free_endpoint()).unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&bound.to_string()).unwrap();
    for i in 1..=100 {
        push.send(i.to_string());
    }
    // Time for all of them to arrive, each replacing the one before.
    thread::sleep(Duration::from_millis(200));
    assert_eq!(pull.try_recv(), Some(Message::from("100")));
    assert_eq!(pull.try_recv(), None);
}

#[test]
fn a_conflating_pull_that_connects_keeps_only_the_latest_message() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    let bound = push.bind(&free_endpoint()).unwrap();
    let mut pull = context.socket::<Pull>();
    pull.set_conflate(true);
    pull.connect(&bound.to_string()).unwrap();
    for i in 1..=100 {
        push.send(i.to_string());
    }
    thread::sleep(Duration::from_millis(200));
    assert_eq!(pull.try_recv(), Some(Message::from("100")));
    assert_eq!(pull.try_recv(), None);
}

#[test]
fn conflating_can_be_turned_off_again() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.set_conflate(true);
    pull.set_conflate(false);
    pull.bind("inproc://every").unwrap();
    let mut push = context.socket::<Push>();
    push.connect("inproc://every").unwrap();
    push.send("1");
    push.send("2");
    assert_eq!(pull.recv(), Message::from("1"));
    assert_eq!(pull.recv(), Message::from("2"));
}

#[test]
fn conflating_never_loses_a_subscription() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://topics").unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.set_conflate(true);
    subscriber.subscribe("a");
    subscriber.subscribe("b");
    subscriber.connect("inproc://topics").unwrap();
    let publisher = within(FIVE, move || {
        while publisher.send("b") == 0 {
            thread::yield_now();
        }
        publisher
    });
    let mut publisher = publisher;
    assert_eq!(publisher.send("a"), 1, "both subscriptions arrived");
    assert_eq!(subscriber.recv(), Message::from("a"));
}
