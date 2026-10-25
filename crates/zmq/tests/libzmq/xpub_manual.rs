//! libzmq's `tests/test_xpub_manual.cpp`: with `ZMQ_XPUB_MANUAL`, here a
//! [`ManualXPub`], the application decides what each subscriber is
//! subscribed to.
//!
//! libzmq's `ZMQ_SUBSCRIBE` and `ZMQ_UNSUBSCRIBE` on a manual XPUB act on
//! the peer whose request was received last; here they are methods of
//! that request, a [`zmq::SubscriptionRequest`].

use std::time::Duration;

use zmq::{Context, ManualXPub, Message, Pub, Socket, Sub, XPub, XPubEvent, XSub};

use crate::support::{FIVE, SETTLE, settle};

pub(crate) fn subscription(topic: &[u8]) -> Message {
    Message::from([&[1][..], topic].concat())
}

pub(crate) fn cancel(topic: &[u8]) -> Message {
    Message::from([&[0][..], topic].concat())
}

pub(crate) fn subscribed(topic: &'static str) -> XPubEvent {
    XPubEvent::Subscribe(topic.into())
}

pub(crate) fn cancelled(topic: &'static str) -> XPubEvent {
    XPubEvent::Cancel(topic.into())
}

pub(crate) fn data(topic: &'static str, payload: &'static str) -> Message {
    let mut message = Message::new(topic);
    message.push(payload);
    message
}

/// Waits for the next request, which must be `event`, and applies `act`
/// to it.
pub(crate) fn on_request(
    publisher: &mut Socket<ManualXPub>,
    event: XPubEvent,
    act: impl FnOnce(&mut zmq::SubscriptionRequest<'_>),
) {
    let mut request = publisher.recv_timeout(FIVE).expect("a request");
    assert_eq!(request.event(), &event);
    act(&mut request);
}

pub(crate) fn expect<T: Receives>(socket: &mut Socket<T>, message: Message) {
    assert_eq!(T::recv(socket, FIVE), Some(message));
}

pub(crate) fn expect_nothing<T: Receives>(socket: &mut Socket<T>, wait: Duration) {
    assert_eq!(T::recv(socket, wait), None);
}

/// The sockets these tests receive messages on.
pub(crate) trait Receives: zmq::SocketType + Sized {
    fn recv(socket: &mut Socket<Self>, timeout: Duration) -> Option<Message>;
}

impl Receives for Sub {
    fn recv(socket: &mut Socket<Self>, timeout: Duration) -> Option<Message> {
        socket.recv_timeout(timeout)
    }
}

impl Receives for XSub {
    fn recv(socket: &mut Socket<Self>, timeout: Duration) -> Option<Message> {
        socket.recv_timeout(timeout)
    }
}

/// The cases this test shares with `test_xpub_manual_last_value.cpp`.
pub(crate) mod cases {
    use super::*;

    pub(crate) fn basic() {
        let context = Context::new().unwrap();
        let mut publisher = context.socket::<ManualXPub>();
        publisher.bind("inproc://libzmq-xpub-manual-basic").unwrap();
        let mut subscriber = context.socket::<XSub>();
        subscriber
            .connect("inproc://libzmq-xpub-manual-basic")
            .unwrap();

        subscriber.send(subscription(b"A"));
        // Subscribe the subscriber for B instead
        on_request(&mut publisher, subscribed("A"), |request| {
            assert!(request.subscribe("B"));
        });

        publisher.send("A");
        publisher.send("B");
        expect(&mut subscriber, Message::from("B"));
    }

    pub(crate) fn unsubscribe_manual() {
        let context = Context::new().unwrap();
        let mut publisher = context.socket::<ManualXPub>();
        publisher
            .bind("inproc://libzmq-xpub-manual-unsubscribe")
            .unwrap();
        let mut subscriber = context.socket::<XSub>();
        subscriber
            .connect("inproc://libzmq-xpub-manual-unsubscribe")
            .unwrap();

        subscriber.send(subscription(b"A"));
        subscriber.send(subscription(b"B"));
        on_request(&mut publisher, subscribed("A"), |request| {
            assert!(request.subscribe("XA"));
        });
        on_request(&mut publisher, subscribed("B"), |request| {
            assert!(request.subscribe("XB"));
        });

        subscriber.send(cancel(b"A"));
        on_request(&mut publisher, cancelled("A"), |request| {
            assert!(request.unsubscribe("XA"));
        });

        publisher.send("XA");
        publisher.send("XB");
        // The subscriber should receive XB only
        expect(&mut subscriber, Message::from("XB"));

        // Closing the subscriber cancels what it had asked for
        drop(subscriber);
        on_request(&mut publisher, cancelled("B"), |request| {
            // The peer is gone, so this does nothing
            assert!(!request.unsubscribe("XB"));
        });
    }

    pub(crate) fn xpub_proxy_unsubscribe_on_disconnect(last_value: bool) {
        let context = Context::new().unwrap();
        // Proxy frontend and backend
        let mut xsub_proxy = context.socket::<XSub>();
        let frontend = xsub_proxy.bind("tcp://127.0.0.1:*").unwrap().to_string();
        let mut xpub_proxy = context.socket::<ManualXPub>();
        let backend = xpub_proxy.bind("tcp://127.0.0.1:*").unwrap().to_string();

        let mut publisher = context.socket::<Pub>();
        publisher.connect(&frontend).unwrap();

        // The first subscriber subscribes; the proxy confirms it and passes
        // it upstream
        let mut sub1 = context.socket::<Sub>();
        sub1.connect(&backend).unwrap();
        sub1.subscribe("1");
        settle();
        on_request(&mut xpub_proxy, subscribed("1"), |request| {
            assert!(request.subscribe("1"));
        });
        xsub_proxy.send(subscription(b"1"));

        // And the second
        let mut sub2 = context.socket::<Sub>();
        sub2.connect(&backend).unwrap();
        sub2.subscribe("1");
        settle();
        on_request(&mut xpub_proxy, subscribed("1"), |request| {
            assert!(request.subscribe("1"));
            if last_value {
                // As ZMQ_XPUB_MANUAL_LAST_VALUE sends to the last subscriber
                assert!(request.send(data("1", "X")));
            }
        });
        xsub_proxy.send(subscription(b"1"));
        settle();

        // The publisher sends a message, which the proxy passes on
        publisher.send(data("1", "X"));
        settle();
        let message = xsub_proxy.recv_timeout(FIVE).expect("the message");
        assert_eq!(message, data("1", "X"));
        xpub_proxy.send(message);
        settle();

        // Each subscriber gets the message, and sub2 the last value too
        if last_value {
            expect(&mut sub2, data("1", "X"));
        }
        expect(&mut sub2, data("1", "X"));
        expect(&mut sub1, data("1", "X"));

        // Disconnect both subscribers; their cancels are passed upstream
        drop(sub1);
        drop(sub2);
        settle();
        for _ in 0..2 {
            on_request(&mut xpub_proxy, cancelled("1"), |request| {
                assert!(!request.unsubscribe("1"), "the peer is gone");
            });
            xsub_proxy.send(cancel(b"1"));
        }
        settle();

        // Nothing should come to the proxy any more
        publisher.send(data("1", "X"));
        settle();
        expect_nothing(&mut xsub_proxy, SETTLE);
    }

    /// Each subscriber subscribes in quick succession; the proxy confirms
    /// the first before the second arrives, so neither is lost.
    pub(crate) fn missing_subscriptions() {
        let context = Context::new().unwrap();
        let mut xsub_proxy = context.socket::<XSub>();
        let frontend = xsub_proxy.bind("tcp://127.0.0.1:*").unwrap().to_string();
        let mut xpub_proxy = context.socket::<ManualXPub>();
        let backend = xpub_proxy.bind("tcp://127.0.0.1:*").unwrap().to_string();

        let mut publisher = context.socket::<Pub>();
        publisher.connect(&frontend).unwrap();

        let mut sub1 = context.socket::<Sub>();
        sub1.connect(&backend).unwrap();
        sub1.subscribe("1");
        settle();
        on_request(&mut xpub_proxy, subscribed("1"), |request| {
            assert!(request.subscribe("1"));
        });
        xsub_proxy.send(subscription(b"1"));

        let mut sub2 = context.socket::<Sub>();
        sub2.connect(&backend).unwrap();
        sub2.subscribe("2");
        settle();
        on_request(&mut xpub_proxy, subscribed("2"), |request| {
            assert!(request.subscribe("2"));
        });
        xsub_proxy.send(subscription(b"2"));
        settle();

        // The publisher sends two messages, each with its own topic, which
        // the proxy passes on
        publisher.send(data("1", "X"));
        publisher.send(data("2", "X"));
        settle();
        for expected in [data("1", "X"), data("2", "X")] {
            let message = xsub_proxy.recv_timeout(FIVE).expect("a message");
            assert_eq!(message, expected);
            xpub_proxy.send(message);
        }
        settle();

        // Each subscriber should now get its message
        expect(&mut sub2, data("2", "X"));
        expect(&mut sub1, data("1", "X"));
    }

    pub(crate) fn unsubscribe_cleanup() {
        let context = Context::new().unwrap();
        let mut publisher = context.socket::<ManualXPub>();
        let endpoint = publisher.bind("tcp://127.0.0.1:*").unwrap().to_string();
        let mut subscriber = context.socket::<XSub>();
        subscriber.connect(&endpoint).unwrap();

        subscriber.send(subscription(b"A"));
        on_request(&mut publisher, subscribed("A"), |request| {
            assert!(request.subscribe("XA"));
        });
        publisher.send("XA");
        publisher.send("XB");
        expect(&mut subscriber, Message::from("XA"));
        expect_nothing(&mut subscriber, SETTLE);

        // Closing the socket results in a cancel
        drop(subscriber);
        on_request(&mut publisher, cancelled("A"), |request| {
            // This does nothing: the peer is gone
            assert!(!request.unsubscribe("XA"));
        });

        // Reconnect, and subscribe for B, overwritten to XB
        let mut subscriber = context.socket::<XSub>();
        subscriber.connect(&endpoint).unwrap();
        subscriber.send(subscription(b"B"));
        on_request(&mut publisher, subscribed("B"), |request| {
            assert!(request.subscribe("XB"));
        });
        publisher.send("XA");
        publisher.send("XB");
        expect(&mut subscriber, Message::from("XB"));
        expect_nothing(&mut subscriber, SETTLE);
    }
}

#[test]
fn basic() {
    cases::basic();
}

#[test]
fn unsubscribe_manual() {
    cases::unsubscribe_manual();
}

#[test]
fn xpub_proxy_unsubscribe_on_disconnect() {
    cases::xpub_proxy_unsubscribe_on_disconnect(false);
}

#[test]
fn missing_subscriptions() {
    cases::missing_subscriptions();
}

#[test]
fn unsubscribe_cleanup() {
    cases::unsubscribe_cleanup();
}

/// A message from an XSUB that is neither a subscription nor a cancel
/// reaches the XPUB as a message.
#[test]
fn user_message() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<XPub>();
    publisher.bind("inproc://libzmq-xpub-user-message").unwrap();
    let mut subscriber = context.socket::<XSub>();
    subscriber
        .connect("inproc://libzmq-xpub-user-message")
        .unwrap();

    let message = Message::from(&b"\x02A"[..]);
    subscriber.send(message.clone());
    assert_eq!(
        publisher.recv_timeout(FIVE),
        Some(XPubEvent::Message(message))
    );
}

// test_user_message_multi is not ported: it needs ZMQ_ONLY_FIRST_SUBSCRIBE,
// which zmq-rs does not have.
