//! Queueing only on completed connections, as libzmq's `ZMQ_IMMEDIATE`.

use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Message, Pub, Pull, Push, Reconnect, SocketOptions, Sub, XPub, XPubEvent};

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

fn immediate() -> SocketOptions {
    SocketOptions {
        immediate: true,
        reconnect: Reconnect::Fixed {
            interval: Duration::from_millis(10),
        },
        ..SocketOptions::default()
    }
}

/// A TCP endpoint nothing listens on.
fn unused_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    format!("tcp://{}", listener.local_addr().unwrap())
}

/// Sends `message` until a peer takes it.
fn send_when_connected(push: &mut zmq::Socket<Push>, message: &'static str) {
    let started = Instant::now();
    while push.try_send(message).is_err() {
        assert!(started.elapsed() < FIVE, "never connected");
        thread::sleep(Duration::from_millis(5));
    }
}

/// Binds `endpoint` again, once the socket that had it has let it go.
fn rebind<T: zmq::SocketType>(socket: &mut zmq::Socket<T>, endpoint: &zmq::Endpoint) {
    let started = Instant::now();
    while socket.bind(&endpoint.to_string()).is_err() {
        assert!(started.elapsed() < FIVE, "{endpoint} stayed bound");
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn nothing_queues_before_the_connection_is_up() {
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(immediate());
    push.connect(&unused_endpoint()).unwrap();
    assert_eq!(push.try_send("early"), Err(Message::from("early")));
}

#[test]
fn messages_go_only_to_connections_that_are_up() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket_with::<Push>(immediate());
    push.connect(&unused_endpoint()).unwrap();
    push.connect(&bound.to_string()).unwrap();
    send_when_connected(&mut push, "0");
    for i in 1..10 {
        push.send(i.to_string());
    }
    let received = within(FIVE, move || {
        (0..10).map(|_| pull.recv()).collect::<Vec<_>>()
    });
    let expected: Vec<Message> = (0..10).map(|i| Message::from(i.to_string())).collect();
    assert_eq!(received, expected);
}

#[test]
fn a_broken_connection_stops_queueing_until_it_is_back() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket_with::<Push>(immediate());
    push.connect(&bound.to_string()).unwrap();
    send_when_connected(&mut push, "first");
    let pull = within(FIVE, move || {
        assert_eq!(pull.recv(), Message::from("first"));
        pull
    });
    drop(pull);

    let started = Instant::now();
    while push.try_send("into the void").is_ok() {
        assert!(started.elapsed() < FIVE, "the peer never went away");
        thread::sleep(Duration::from_millis(5));
    }

    let mut pull = context.socket::<Pull>();
    rebind(&mut pull, &bound);
    send_when_connected(&mut push, "again");
    let received = within(FIVE, move || pull.recv());
    assert_eq!(received, Message::from("again"));
}

#[test]
fn subscriptions_survive_a_reconnection() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    let bound = publisher.bind("tcp://127.0.0.1:*").unwrap();
    let mut subscriber = context.socket_with::<Sub>(immediate());
    subscriber.subscribe("news");
    subscriber.connect(&bound.to_string()).unwrap();
    let mut publish_until_heard = |publisher: &mut zmq::Socket<Pub>| {
        let started = Instant::now();
        loop {
            assert!(started.elapsed() < FIVE, "never subscribed");
            publisher.send("news");
            thread::sleep(Duration::from_millis(5));
            if subscriber.try_recv().is_some() {
                break;
            }
        }
    };
    publish_until_heard(&mut publisher);
    drop(publisher);
    let mut publisher = context.socket::<Pub>();
    rebind(&mut publisher, &bound);
    publish_until_heard(&mut publisher);
}

#[test]
fn a_dropped_socket_stops_reconnecting() {
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(immediate());
    let mut monitor = push.monitor();
    push.connect(&unused_endpoint()).unwrap();
    thread::sleep(Duration::from_millis(30));
    drop(push);
    // The monitor closes once the socket and all its connections are gone.
    within(FIVE, move || while monitor.recv_blocking().is_some() {});
}

#[test]
fn a_dropped_socket_still_delivers_what_it_queued() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket_with::<Push>(immediate());
    push.connect(&bound.to_string()).unwrap();
    send_when_connected(&mut push, "0");
    let reader = thread::spawn(move || (0..10_000).map(|_| pull.recv()).last());
    // Large enough that some are still queued when the socket goes.
    let filler = "x".repeat(10_000);
    for _ in 1..9_999 {
        push.send(filler.clone());
    }
    push.send("last");
    drop(push);
    let last = within(FIVE, move || reader.join().unwrap());
    assert_eq!(last, Some(Message::from("last")));
}

/// The XPUB's next event, using the SUB meanwhile: an `immediate`
/// connection reaches the SUB, which subscribes on it, only when the SUB is
/// used, as in libzmq.
fn next_event(xpub: &mut zmq::Socket<XPub>, subscriber: &mut zmq::Socket<Sub>) -> XPubEvent {
    let started = Instant::now();
    loop {
        assert!(started.elapsed() < FIVE, "no event");
        assert_eq!(subscriber.try_recv(), None);
        if let Some(event) = xpub.try_recv() {
            return event;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn a_reconnected_subscriber_subscribes_once() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    let bound = xpub.bind("tcp://127.0.0.1:*").unwrap();
    let mut subscriber = context.socket_with::<Sub>(immediate());
    subscriber.subscribe("a");
    subscriber.connect(&bound.to_string()).unwrap();
    let subscribed = XPubEvent::Subscribe("a".into());
    assert_eq!(next_event(&mut xpub, &mut subscriber), subscribed);
    drop(xpub);
    let mut xpub = context.socket::<XPub>();
    rebind(&mut xpub, &bound);
    assert_eq!(next_event(&mut xpub, &mut subscriber), subscribed);
    // The XPUB reports the last cancellation only if it heard one
    // subscription.
    subscriber.unsubscribe("a");
    assert_eq!(
        next_event(&mut xpub, &mut subscriber),
        XPubEvent::Cancel("a".into())
    );
}

#[test]
fn a_topic_dropped_while_disconnected_is_not_resubscribed() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    let bound = xpub.bind("tcp://127.0.0.1:*").unwrap();
    let mut subscriber = context.socket_with::<Sub>(immediate());
    subscriber.subscribe("old");
    subscriber.connect(&bound.to_string()).unwrap();
    assert_eq!(
        next_event(&mut xpub, &mut subscriber),
        XPubEvent::Subscribe("old".into())
    );
    drop(xpub);
    // Wait until the SUB has lost the connection.
    thread::sleep(Duration::from_millis(100));
    assert_eq!(subscriber.try_recv(), None);
    subscriber.unsubscribe("old");
    subscriber.subscribe("new");
    let mut xpub = context.socket::<XPub>();
    rebind(&mut xpub, &bound);
    assert_eq!(
        next_event(&mut xpub, &mut subscriber),
        XPubEvent::Subscribe("new".into())
    );
}
