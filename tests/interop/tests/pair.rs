//! PAIR against libzmq's.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Message, Pair};
use zmq_interop::libzmq::{Kind, Socket};
use zmq_test_ports::free_endpoint;

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
fn our_pair_talks_to_a_libzmq_pair() {
    let theirs = Socket::new(Kind::Pair);
    theirs.set_receive_timeout(FIVE);
    let addr = theirs.bind_loopback();
    let context = Context::new().unwrap();
    let mut ours = context.socket::<Pair>();
    ours.connect(&format!("tcp://{addr}")).unwrap();
    ours.send("ping");
    assert_eq!(theirs.recv().unwrap(), [b"ping"]);
    theirs.send(&[b"pong", b"two frames"]).unwrap();
    let received = within(FIVE, move || ours.recv());
    assert_eq!(received.iter().count(), 2);
    assert_eq!(received.get(0).unwrap().as_slice(), b"pong");
}

#[test]
fn a_libzmq_pair_talks_to_ours() {
    let context = Context::new().unwrap();
    let mut ours = context.socket::<Pair>();
    let bound = ours.bind(&free_endpoint()).unwrap();
    let theirs = Socket::new(Kind::Pair);
    theirs.set_receive_timeout(FIVE);
    theirs.connect(bound);
    theirs.send(&[b"hello"]).unwrap();
    let (received, mut ours) = within(FIVE, move || (ours.recv(), ours));
    assert_eq!(received, Message::from("hello"));
    ours.send("hi");
    assert_eq!(theirs.recv().unwrap(), [b"hi"]);
}

#[test]
fn a_second_libzmq_pair_waits_its_turn() {
    let context = Context::new().unwrap();
    let mut ours = context.socket::<Pair>();
    let bound = ours.bind(&free_endpoint()).unwrap();
    let first = Socket::new(Kind::Pair);
    first.connect(bound.clone());
    first.send(&[b"first"]).unwrap();
    let (received, ours) = within(FIVE, move || (ours.recv(), ours));
    assert_eq!(received, Message::from("first"));

    // Turned away for now. What it sends meanwhile is lost with the
    // connection we close, as with libzmq's own PAIR; libzmq reconnects and
    // gets in once the first has gone.
    let second = Socket::new(Kind::Pair);
    second.set_receive_timeout(FIVE);
    second.connect(bound);
    second.send(&[b"lost"]).unwrap();
    thread::sleep(Duration::from_millis(200));
    let mut ours = ours;
    assert_eq!(
        ours.try_recv(),
        None,
        "the second is not heard while the first is here"
    );
    drop(first);
    let started = std::time::Instant::now();
    let heard = loop {
        second.send(&[b"knock"]).unwrap();
        if let Some(message) = ours.try_recv() {
            break message;
        }
        assert!(started.elapsed() < FIVE, "the second never got in");
        thread::sleep(Duration::from_millis(20));
    };
    assert!(heard == Message::from("knock") || heard == Message::from("lost"));
    ours.send("welcome");
    assert_eq!(second.recv().unwrap(), [b"welcome"]);
}
