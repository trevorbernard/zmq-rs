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
    let mut xpub = context.socket::<XPub>().unwrap();
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
    let mut xpub = context.socket::<XPub>().unwrap();
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
    let mut xsub = context.socket::<XSub>().unwrap();
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
    let mut xpub = context.socket::<XPub>().unwrap();
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

/// Messages as libzmq hands them over, each a list of frames.
type Received = Vec<Vec<Vec<u8>>>;

/// What `socket` receives until 300 ms pass without a message.
fn drain(socket: &Socket) -> Received {
    socket.set_receive_timeout(Duration::from_millis(300));
    std::iter::from_fn(|| socket.recv().ok()).collect()
}

/// What our `xpub` receives until 300 ms pass without an event.
fn our_events(xpub: &mut zmq::Socket<XPub>) -> Vec<XPubEvent> {
    std::iter::from_fn(|| xpub.recv_timeout(Duration::from_millis(300)).unwrap()).collect()
}

fn frames<const N: usize>(frames: [&'static [u8]; N]) -> Vec<Vec<u8>> {
    frames.map(<[u8]>::to_vec).to_vec()
}

fn message<const N: usize>(frames: [&'static str; N]) -> XPubEvent {
    XPubEvent::Message(Message::from(frames))
}

fn subscribe(topic: &'static str) -> XPubEvent {
    XPubEvent::Subscribe(Frame::from(topic))
}

/// What a libzmq XPUB hears when a libzmq XSUB sends it `sent` as one
/// message, and what of `A1` and `B1` that XSUB is then sent.
fn libzmq_xpub_hears(sent: &[&[u8]], only_first: bool) -> (Received, Received) {
    let xpub = Socket::new(Kind::XPub);
    if only_first {
        xpub.set_only_first_subscribe();
    }
    let xsub = Socket::new(Kind::XSub);
    xsub.connect(xpub.bind_loopback());
    xsub.send(sent).unwrap();
    let heard = drain(&xpub);
    xpub.send(&[b"A1"]).unwrap();
    xpub.send(&[b"B1"]).unwrap();
    (heard, drain(&xsub))
}

/// As [`libzmq_xpub_hears`], for an XPUB of ours.
fn our_xpub_hears(
    sent: &[&[u8]],
    configure: impl FnOnce(&mut zmq::Socket<XPub>),
) -> (Vec<XPubEvent>, Received) {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>().unwrap();
    configure(&mut xpub);
    let bound = xpub.bind(free_endpoint()).unwrap();
    let xsub = Socket::new(Kind::XSub);
    xsub.connect(bound);
    xsub.send(sent).unwrap();
    let heard = our_events(&mut xpub);
    xpub.send("A1").unwrap();
    xpub.send("B1").unwrap();
    (heard, drain(&xsub))
}

#[test]
fn libzmq_xpub_reads_a_subscription_from_any_frame() {
    assert_eq!(
        libzmq_xpub_hears(&[b"\x01A", b"\x01B", b"hello"], false),
        (
            vec![frames([b"\x01A"]), frames([b"\x01B"]), frames([b"hello"])],
            vec![frames([b"A1"]), frames([b"B1"])]
        )
    );
    // The subscription ends the message the frames before it began.
    assert_eq!(
        libzmq_xpub_hears(&[b"hello", b"\x01A", b"x"], false),
        (
            vec![frames([b"hello", b"\x01A"]), frames([b"x"])],
            vec![frames([b"A1"])]
        )
    );
}

#[test]
fn our_xpub_reads_a_subscription_from_any_frame() {
    assert_eq!(
        our_xpub_hears(&[b"\x01A", b"\x01B", b"hello"], |_| ()),
        (
            vec![subscribe("A"), subscribe("B"), message(["hello"])],
            vec![frames([b"A1"]), frames([b"B1"])]
        )
    );
    assert_eq!(
        our_xpub_hears(&[b"hello", b"\x01A", b"x"], |_| ()),
        (
            vec![message(["hello"]), subscribe("A"), message(["x"])],
            vec![frames([b"A1"])]
        )
    );
}

/// With `ZMQ_ONLY_FIRST_SUBSCRIBE`, the frames after the first are read
/// only when the first is a subscription.
#[test]
fn libzmq_xpub_reads_later_frames_only_after_a_first_that_subscribes() {
    if !zmq_interop::libzmq::has_drafts() {
        return;
    }
    assert_eq!(
        libzmq_xpub_hears(&[b"hello", b"\x01A", b"x"], true),
        (vec![frames([b"hello", b"\x01A", b"x"])], vec![])
    );
    assert_eq!(
        libzmq_xpub_hears(&[b"\x01A", b"hello", b"\x01B"], true),
        (
            vec![frames([b"\x01A"]), frames([b"hello", b"\x01B"])],
            vec![frames([b"A1"]), frames([b"B1"])]
        )
    );
}

#[cfg(feature = "draft")]
#[test]
fn our_xpub_reads_later_frames_only_after_a_first_that_subscribes() {
    let only_first = |xpub: &mut zmq::Socket<XPub>| xpub.set_only_first_subscribe(true);
    assert_eq!(
        our_xpub_hears(&[b"hello", b"\x01A", b"x"], only_first),
        (
            vec![XPubEvent::Message(Message::from([
                &b"hello"[..],
                &b"\x01A"[..],
                &b"x"[..]
            ]))],
            vec![]
        )
    );
    assert_eq!(
        our_xpub_hears(&[b"\x01A", b"hello", b"\x01B"], only_first),
        (
            vec![subscribe("A"), message(["hello"]), subscribe("B")],
            vec![frames([b"A1"]), frames([b"B1"])]
        )
    );
}

/// The subscriptions a libzmq XPUB hears from an XSUB that connects to it
/// after sending `sent` as one message: those the XSUB holds.
fn held_after(xsub: impl FnOnce(&str)) -> Received {
    let xpub = Socket::new(Kind::XPub);
    let addr = xpub.bind_loopback();
    xsub(&format!("tcp://{addr}"));
    let mut heard = drain(&xpub);
    heard.sort();
    heard
}

#[test]
fn libzmq_xsub_holds_a_subscription_sent_in_any_frame() {
    let xsub = Socket::new(Kind::XSub);
    xsub.send(&[b"\x01A", b"hello", b"\x01B", b"\x00A"])
        .unwrap();
    assert_eq!(
        held_after(|endpoint| xsub.connect_str(endpoint)),
        [frames([b"\x01B"])]
    );
}

#[test]
fn our_xsub_holds_a_subscription_sent_in_any_frame() {
    let context = Context::new().unwrap();
    let mut xsub = context.socket::<XSub>().unwrap();
    xsub.send(Message::from([
        &b"\x01A"[..],
        &b"hello"[..],
        &b"\x01B"[..],
        &b"\x00A"[..],
    ]))
    .unwrap();
    assert_eq!(
        held_after(|endpoint| xsub.connect(endpoint).unwrap()),
        [frames([b"\x01B"])]
    );
}

#[test]
fn libzmq_xsub_holds_later_frames_only_after_a_first_that_subscribes() {
    if !zmq_interop::libzmq::has_drafts() {
        return;
    }
    let xsub = Socket::new(Kind::XSub);
    xsub.set_only_first_subscribe();
    xsub.send(&[b"hello", b"\x01A"]).unwrap();
    xsub.send(&[b"\x01B", b"hello", b"\x01C"]).unwrap();
    assert_eq!(
        held_after(|endpoint| xsub.connect_str(endpoint)),
        [frames([b"\x01B"]), frames([b"\x01C"])]
    );
}

#[cfg(feature = "draft")]
#[test]
fn our_xsub_holds_later_frames_only_after_a_first_that_subscribes() {
    let context = Context::new().unwrap();
    let mut xsub = context.socket::<XSub>().unwrap();
    xsub.set_only_first_subscribe(true);
    xsub.send(Message::from([&b"hello"[..], &b"\x01A"[..]]))
        .unwrap();
    xsub.send(Message::from([&b"\x01B"[..], &b"hello"[..], &b"\x01C"[..]]))
        .unwrap();
    assert_eq!(
        held_after(|endpoint| xsub.connect(endpoint).unwrap()),
        [frames([b"\x01B"]), frames([b"\x01C"])]
    );
}

/// What a libzmq XPUB hears from an XSUB of ours, already connected, that
/// sends `sent`: what a libzmq XSUB would have put on the wire.
#[test]
fn our_xsub_sends_what_libzmqs_does_of_a_message_that_subscribes() {
    let sent: [&[u8]; 5] = [b"\x01A", b"hello", b"\x00Z", b"\x01B", b"tail"];
    let expected = vec![
        frames([b"\x01A"]),
        frames([b"hello", b"\x01B"]),
        frames([b"tail"]),
    ];

    let xpub = Socket::new(Kind::XPub);
    let xsub = Socket::new(Kind::XSub);
    xsub.connect(xpub.bind_loopback());
    xsub.send(&sent).unwrap();
    assert_eq!(drain(&xpub), expected, "libzmq");

    let xpub = Socket::new(Kind::XPub);
    let addr = xpub.bind_loopback();
    let context = Context::new().unwrap();
    let mut xsub = context.socket::<XSub>().unwrap();
    xsub.connect(format!("tcp://{addr}")).unwrap();
    xsub.send(Message::from(sent)).unwrap();
    assert_eq!(drain(&xpub), expected, "ours");
}
