//! XPUB and XSUB against libzmq's.

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Frame, Message, XPub, XPubEvent, XSub};
use zmq_interop::libzmq::{Kind, Socket};

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
    let bound = xpub.bind("tcp://127.0.0.1:*").unwrap();
    let subscriber = Socket::new(Kind::Sub);
    subscriber.subscribe(b"a");
    subscriber.subscribe(b"b");
    subscriber.connect(bound);
    let (first, second, mut xpub) = within(FIVE, move || (xpub.recv(), xpub.recv(), xpub));
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
    let (cancel, mut xpub) = within(FIVE, move || (xpub.recv(), xpub));
    assert_eq!(cancel, XPubEvent::Cancel(Frame::from("a")));
    drop(subscriber);
    assert_eq!(
        within(FIVE, move || xpub.recv()),
        XPubEvent::Cancel(Frame::from("b")),
        "a departing subscriber's topics are cancelled"
    );
}

#[test]
fn our_xpub_hears_what_a_libzmq_xsub_sends_upstream() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    let bound = xpub.bind("tcp://127.0.0.1:*").unwrap();
    let xsub = Socket::new(Kind::XSub);
    xsub.connect(bound);
    xsub.send(&[b"hello", b"upstream"]).unwrap();
    let expected = Message::try_from(vec![Frame::from("hello"), Frame::from("upstream")]).unwrap();
    assert_eq!(
        within(FIVE, move || xpub.recv()),
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
    xsub.connect(&format!("tcp://{addr}")).unwrap();
    xsub.subscribe("a");
    xsub.send("hello");
    // libzmq hands XPUB applications subscriptions in the message form.
    let mut heard = [xpub.recv().unwrap(), xpub.recv().unwrap()];
    heard.sort();
    assert_eq!(heard, [vec![b"\x01a".to_vec()], vec![b"hello".to_vec()]]);

    let started = Instant::now();
    loop {
        xpub.send(&[b"b: unwanted"]).unwrap();
        xpub.send(&[b"a: wanted"]).unwrap();
        if let Some(message) = xsub.try_recv() {
            assert_eq!(message, Message::from("a: wanted"));
            break;
        }
        assert!(started.elapsed() < FIVE, "nothing arrived");
        thread::sleep(Duration::from_millis(5));
    }
}
