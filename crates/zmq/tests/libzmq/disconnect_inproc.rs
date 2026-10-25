//! libzmq's `tests/test_disconnect_inproc.cpp`: an XPUB publishes to a SUB
//! connected over `inproc`, which then disconnects. The XPUB hears the
//! subscription and, once the SUB is gone, its cancel; the SUB receives
//! only what was published while it was connected.

use std::time::Duration;

use zmq::{Context, Message, Sub, XPub, XPubEvent};

use crate::support::{FIVE, settle, within};

#[test]
fn disconnect_inproc() {
    within(FIVE, || {
        let context = Context::new().unwrap();
        let mut publisher = context.socket::<XPub>();
        let mut subscriber = context.socket::<Sub>();
        subscriber.subscribe("foo");
        publisher.bind("inproc://someInProcDescriptor").unwrap();

        let mut publications_received = 0;
        let mut is_subscribed = false;
        for iteration in 0.. {
            let ready = zmq::poll_timeout(
                [subscriber.readable(), publisher.readable()],
                Duration::from_millis(100),
            );
            let [publication, subscription] = ready.unwrap_or_default();
            if subscription {
                match publisher.recv() {
                    XPubEvent::Subscribe(_) => {
                        assert!(!is_subscribed);
                        is_subscribed = true;
                    }
                    XPubEvent::Cancel(_) => {
                        assert!(is_subscribed);
                        is_subscribed = false;
                    }
                    XPubEvent::Message(message) => panic!("unexpected {message:?}"),
                }
            }
            if publication {
                subscriber.recv();
                publications_received += 1;
            }
            if iteration == 1 {
                subscriber.connect("inproc://someInProcDescriptor").unwrap();
                settle();
            }
            if iteration == 4 {
                subscriber
                    .disconnect("inproc://someInProcDescriptor")
                    .unwrap();
            }
            if iteration > 4 && ready.is_none() {
                break;
            }
            let mut message = Message::new("foo");
            message.push("this is foo!");
            publisher.send(message);
        }
        assert_eq!(publications_received, 3);
        assert!(!is_subscribed);
    });
}
