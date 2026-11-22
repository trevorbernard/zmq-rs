//! XPUB and XSUB against libzmq's.

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Frame, Message, XPub, XPubEvent, XSub};
use zmq_interop::Peer;
use zmq_interop::libzmq::{Kind, Socket};
use zmq_test_ports::free_endpoint;
use zmtp::{Command, SocketType};

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

#[test]
fn our_xpub_hears_a_libzmq_sub() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    let bound = xpub.bind(free_endpoint()).unwrap();
    let subscriber = Socket::new(Kind::Sub);
    subscriber.subscribe(b"a");
    subscriber.subscribe(b"b");
    subscriber.connect(bound);
    let (first, second, mut xpub) = within(FIVE, move || {
        (xpub.recv().unwrap(), xpub.recv().unwrap(), xpub)
    });
    let mut heard = [first, second];
    heard.sort_by_key(|event| format!("{event:?}"));
    assert_eq!(
        heard,
        [
            XPubEvent::Subscribe(Frame::from("a")),
            XPubEvent::Subscribe(Frame::from("b"))
        ]
    );
    subscriber.unsubscribe(b"a");
    let (cancel, mut xpub) = within(FIVE, move || (xpub.recv().unwrap(), xpub));
    assert_eq!(cancel, XPubEvent::Cancel(Frame::from("a")));
    drop(subscriber);
    assert_eq!(
        within(FIVE, move || xpub.recv().unwrap()),
        XPubEvent::Cancel(Frame::from("b")),
        "a departing subscriber's topics are cancelled"
    );
}

#[test]
fn our_xpub_hears_what_a_libzmq_xsub_sends_upstream() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    let bound = xpub.bind(free_endpoint()).unwrap();
    let xsub = Socket::new(Kind::XSub);
    xsub.connect(bound);
    xsub.send(&[b"hello", b"upstream"]).unwrap();
    let expected = Message::try_from(vec![Frame::from("hello"), Frame::from("upstream")]).unwrap();
    assert_eq!(
        within(FIVE, move || xpub.recv().unwrap()),
        XPubEvent::Message(expected)
    );
}

#[test]
fn our_xsub_talks_to_a_libzmq_xpub() {
    let xpub = Socket::new(Kind::XPub);
    xpub.set_receive_timeout(FIVE);
    let addr = xpub.bind_loopback();
    let context = Context::new().unwrap();
    let mut xsub = context.socket::<XSub>();
    xsub.connect(format!("tcp://{addr}")).unwrap();
    xsub.subscribe("a");
    xsub.send("hello").unwrap();
    // libzmq hands XPUB applications subscriptions in the message form.
    let mut heard = [xpub.recv().unwrap(), xpub.recv().unwrap()];
    heard.sort();
    assert_eq!(heard, [vec![b"\x01a".to_vec()], vec![b"hello".to_vec()]]);

    let started = Instant::now();
    loop {
        xpub.send(&[b"b: unwanted"]).unwrap();
        xpub.send(&[b"a: wanted"]).unwrap();
        if let Some(message) = xsub.try_recv().unwrap() {
            assert_eq!(message, Message::from("a: wanted"));
            break;
        }
        assert!(started.elapsed() < FIVE, "nothing arrived");
        thread::sleep(Duration::from_millis(5));
    }
}

/// A subscriber that cancels `topic`, which it does not hold, after
/// subscribing to `held` if given: no socket of ours or libzmq's sends
/// such a cancel unasked.
fn cancel_unheld(
    addr: std::net::SocketAddr,
    held: Option<&'static str>,
    topic: &'static str,
) -> Peer {
    let mut peer = Peer::connect(addr, SocketType::Sub, b"").unwrap();
    if let Some(held) = held {
        peer.send_command(&Command::Subscribe(Frame::from(held)))
            .unwrap();
    }
    peer.send_command(&Command::Cancel(Frame::from(topic)))
        .unwrap();
    peer
}

#[test]
fn libzmq_reports_a_cancel_for_a_topic_its_peer_never_held() {
    let xpub = Socket::new(Kind::XPub);
    let addr = xpub.bind_loopback();
    let _first = cancel_unheld(addr, None, "never");
    assert_eq!(xpub.recv().unwrap(), [b"\0never"]);

    // Even one that another peer holds.
    let _holder = cancel_unheld(addr, Some("held"), "other");
    assert_eq!(xpub.recv().unwrap(), [b"\x01held"]);
    assert_eq!(xpub.recv().unwrap(), [b"\0other"]);
    let _second = cancel_unheld(addr, None, "held");
    assert_eq!(xpub.recv().unwrap(), [b"\0held"]);
}

#[test]
fn we_report_a_cancel_for_a_topic_our_peer_never_held() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    let addr = xpub.bind("tcp://127.0.0.1:*").unwrap().tcp_addr().unwrap();
    let cancel = |topic: &'static str| Some(XPubEvent::Cancel(Frame::from(topic)));
    let _first = cancel_unheld(addr, None, "never");
    assert_eq!(xpub.recv_timeout(FIVE).unwrap(), cancel("never"));

    let _holder = cancel_unheld(addr, Some("held"), "other");
    assert_eq!(
        xpub.recv_timeout(FIVE).unwrap(),
        Some(XPubEvent::Subscribe(Frame::from("held")))
    );
    assert_eq!(xpub.recv_timeout(FIVE).unwrap(), cancel("other"));
    let _second = cancel_unheld(addr, None, "held");
    assert_eq!(xpub.recv_timeout(FIVE).unwrap(), cancel("held"));
}
