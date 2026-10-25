//! libzmq's `tests/test_proxy_hwm.cpp`: backpressure through an XSUB-to-XPUB
//! proxy whose XPUB has `ZMQ_XPUB_NODROP` set.
//!
//! ```text
//!   XPUB                      SUB
//!    |                         |
//!    \-----> XSUB -> XPUB -----/
//!            (the proxy)
//! ```
//!
//! Every queue holds `HWM` messages each way. The publisher floods without
//! blocking, sending only while every subscriber has room; the subscriber
//! is slow. Once the queues fill, the proxy must stop reading from its
//! XSUB rather than drop, so the publisher stops too, after between two
//! and four HWMs, and the subscriber receives every message sent. Asking
//! the proxy for statistics meanwhile must not block.
//!
//! libzmq stops `zmq_proxy` by terminating its context; here it is stopped
//! through its `ProxyControl`.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use zmq::{Context, Hwm, Message, Proxy, SocketOptions, Sub, XPub, XPubEvent, XSub};

use crate::support::within;

const HWM: usize = 10;
const NUM_BYTES_PER_MSG: usize = 50_000;
/// Far more than any queue holds: a publisher still sending after this
/// many was never held back.
const UNBLOCKED: u64 = 10_000;

fn lower_hwm() -> SocketOptions {
    let hwm = Hwm::Messages(NonZeroUsize::new(HWM).unwrap());
    SocketOptions {
        send_hwm: hwm,
        recv_hwm: hwm,
        ..SocketOptions::default()
    }
}

fn publisher_thread_main(context: Context) -> u64 {
    let mut publisher = context.socket_with::<XPub>(lower_hwm());
    publisher.connect("inproc://frontend").unwrap();

    // Wait for the one subscriber to subscribe before sending
    assert_eq!(publisher.recv(), XPubEvent::Subscribe(zmq::Frame::new()));

    let mut send_count = 0;
    let payload = vec![b'A'; NUM_BYTES_PER_MSG];
    while send_count < UNBLOCKED
        && publisher
            .try_send_all(Message::from(payload.clone()))
            .is_ok()
    {
        send_count += 1;
    }
    send_count
}

fn subscriber_thread_main(context: Context) -> u64 {
    let mut subscriber = context.socket_with::<Sub>(lower_hwm());
    subscriber.subscribe("");
    subscriber.connect("inproc://backend").unwrap();

    // The first message may take a while; after it, 100 ms with nothing
    // means the publisher has stopped.
    subscriber.recv();
    let mut received = 1;
    loop {
        thread::sleep(Duration::from_millis(100));
        if subscriber
            .recv_timeout(Duration::from_millis(100))
            .is_none()
        {
            return received;
        }
        received += 1;
    }
}

#[test]
#[ignore = "zmq-rs bug: the proxy sends to an XPUB backend with `send`, which drops for a full subscriber; it has no ZMQ_XPUB_NODROP mode, so it never stops reading from its XSUB and the publisher is never held back"]
fn proxy_hwm() {
    within(Duration::from_secs(60), || {
        let context = Context::new().unwrap();
        let mut frontend = context.socket_with::<XSub>(lower_hwm());
        frontend.bind("inproc://frontend").unwrap();
        let mut backend = context.socket_with::<XPub>(lower_hwm());
        backend.bind("inproc://backend").unwrap();

        let publisher = thread::spawn({
            let context = context.clone();
            move || publisher_thread_main(context)
        });
        let subscriber = thread::spawn({
            let context = context.clone();
            move || subscriber_thread_main(context)
        });

        let mut proxy = Proxy::new(&mut frontend, &mut backend);
        let control = proxy.control();
        let subscriber_done = Arc::new(AtomicBool::new(false));
        let asker = thread::spawn({
            let subscriber_done = subscriber_done.clone();
            let control = control.clone();
            move || {
                while !subscriber_done.load(Ordering::Relaxed) {
                    let _ = control.statistics();
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });
        let stopping = thread::spawn(move || {
            let sent = publisher.join().unwrap();
            let received = subscriber.join().unwrap();
            subscriber_done.store(true, Ordering::Relaxed);
            asker.join().unwrap();
            control.terminate();
            (sent, received)
        });
        proxy.run();
        let (sent, received) = stopping.join().unwrap();

        let hwm = HWM as u64;
        assert!(
            (2 * hwm..=4 * hwm).contains(&sent),
            "the publisher sent {sent} before blocking"
        );
        assert_eq!(received, sent, "the subscriber received every message");
    });
}
