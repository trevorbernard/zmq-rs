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
    ours.connect(format!("tcp://{addr}")).unwrap();
    ours.send("ping").unwrap();
    assert_eq!(theirs.recv().unwrap(), [b"ping"]);
    theirs.send(&[b"pong", b"two frames"]).unwrap();
    let received = within(FIVE, move || ours.recv().unwrap());
    assert_eq!(received.iter().count(), 2);
    assert_eq!(received.get(0).unwrap().as_bytes(), b"pong");
}

#[test]
fn a_libzmq_pair_talks_to_ours() {
    let context = Context::new().unwrap();
    let mut ours = context.socket::<Pair>();
    let bound = ours.bind(free_endpoint()).unwrap();
    let theirs = Socket::new(Kind::Pair);
    theirs.set_receive_timeout(FIVE);
    theirs.connect(bound);
    theirs.send(&[b"hello"]).unwrap();
    let (received, mut ours) = within(FIVE, move || (ours.recv().unwrap(), ours));
    assert_eq!(received, Message::from("hello"));
    ours.send("hi").unwrap();
    assert_eq!(theirs.recv().unwrap(), [b"hi"]);
}

#[test]
fn a_second_libzmq_pair_waits_its_turn() {
    let context = Context::new().unwrap();
    let mut ours = context.socket::<Pair>();
    let bound = ours.bind(free_endpoint()).unwrap();
    let first = Socket::new(Kind::Pair);
    first.connect(bound.clone());
    first.send(&[b"first"]).unwrap();
    let (received, ours) = within(FIVE, move || (ours.recv().unwrap(), ours));
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
        ours.try_recv().unwrap(),
        None,
        "the second is not heard while the first is here"
    );
    drop(first);
    let started = std::time::Instant::now();
    let heard = loop {
        second.send(&[b"knock"]).unwrap();
        if let Some(message) = ours.try_recv().unwrap() {
            break message;
        }
        assert!(started.elapsed() < FIVE, "the second never got in");
        thread::sleep(Duration::from_millis(20));
    };
    assert!(heard == "knock" || heard == "lost");
    ours.send("welcome").unwrap();
    assert_eq!(second.recv().unwrap(), [b"welcome"]);
}

/// A hand-written PAIR that connects to `addr`, sends three messages and
/// leaves.
fn send_three_and_leave(addr: std::net::SocketAddr) {
    let mut peer = zmq_interop::Peer::connect(addr, zmtp::SocketType::Pair, b"").unwrap();
    for body in ["one", "two", "three"] {
        peer.send(&zmtp::Message::from(body)).unwrap();
    }
}

/// A second hand-written PAIR, which connects once the first has left and
/// sends one message.
fn then_a_second(addr: std::net::SocketAddr) -> zmq_interop::Peer {
    let mut peer = zmq_interop::Peer::connect(addr, zmtp::SocketType::Pair, b"").unwrap();
    peer.send(&zmtp::Message::from("four")).unwrap();
    peer
}

/// What `pair` receives until nothing comes for a while.
fn libzmq_receives(pair: &Socket) -> Vec<String> {
    pair.set_receive_timeout(Duration::from_millis(300));
    std::iter::from_fn(|| pair.recv().ok())
        .map(|message| String::from_utf8_lossy(&message[0]).into_owned())
        .collect()
}

#[test]
fn libzmq_delivers_what_a_departed_peer_sent() {
    let pair = Socket::new(Kind::Pair);
    let monitor = pair.monitor();
    let addr = pair.bind_loopback();
    send_three_and_leave(addr);
    monitor.values_of(zmq_interop::libzmq::EVENT_DISCONNECTED, 1);
    assert_eq!(libzmq_receives(&pair), ["one", "two", "three"]);
}

#[test]
fn we_deliver_what_a_departed_peer_sent() {
    let context = Context::new().unwrap();
    let mut pair = context.socket::<Pair>();
    let mut monitor = pair.monitor();
    let addr = pair.bind("tcp://127.0.0.1:*").unwrap().tcp_addr().unwrap();
    send_three_and_leave(addr);
    while monitor.recv_timeout(FIVE).unwrap().expect("an event").kind
        != zmq::EventKind::Disconnected
    {}
    for body in ["one", "two", "three"] {
        assert_eq!(pair.recv_timeout(FIVE).unwrap(), Some(Message::from(body)));
    }
}

/// Until the application has read what the first peer sent, libzmq's PAIR
/// still holds that peer's pipe, and turns a second peer away: its message
/// is lost. Ours lets the second in (`docs/libzmq-compatibility.md`).
#[test]
fn libzmq_turns_a_second_peer_away_while_the_firsts_messages_wait() {
    let pair = Socket::new(Kind::Pair);
    let monitor = pair.monitor();
    let addr = pair.bind_loopback();
    send_three_and_leave(addr);
    monitor.values_of(zmq_interop::libzmq::EVENT_DISCONNECTED, 1);
    let _second = then_a_second(addr);
    thread::sleep(Duration::from_millis(200));
    assert_eq!(libzmq_receives(&pair), ["one", "two", "three"]);
}
