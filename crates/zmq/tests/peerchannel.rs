//! PEER (RFC 51) and CHANNEL (RFC 52). Draft sockets.
#![cfg(feature = "draft")]

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Channel, Context, Frame, NoSuchPeer, Peer, PeerId, PeerSendError};

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
fn peers_address_each_other_by_id() {
    let context = Context::new().unwrap();
    let mut alice = context.socket::<Peer>();
    let bound = alice.bind("tcp://127.0.0.1:*").unwrap();
    let mut bob = context.socket::<Peer>();
    let to_alice = bob.connect_peer(&bound.to_string()).unwrap();
    bob.send(to_alice, "hello alice").unwrap();

    let (to_bob, greeting, mut alice) = within(FIVE, move || {
        let (from, greeting) = alice.recv();
        (from, greeting, alice)
    });
    assert_eq!(greeting, Frame::from("hello alice"));
    alice.send(to_bob, "hello bob").unwrap();
    assert_eq!(
        within(FIVE, move || bob.recv()),
        (to_alice, Frame::from("hello bob"))
    );
}

#[test]
fn every_connection_gets_its_own_nonzero_id() {
    let context = Context::new().unwrap();
    let mut hub = context.socket::<Peer>();
    hub.bind("inproc://hub-a").unwrap();
    let mut other = context.socket::<Peer>();
    other.bind("inproc://hub-b").unwrap();
    let mut caller = context.socket::<Peer>();
    let a = caller.connect_peer("inproc://hub-a").unwrap();
    let b = caller.connect_peer("inproc://hub-b").unwrap();
    assert_ne!(a, b);
    assert_ne!(a.get(), 0);
    assert_ne!(b.get(), 0);
    caller.send(b, "to b").unwrap();
    assert_eq!(
        other.try_recv().map(|(_, frame)| frame),
        Some(Frame::from("to b"))
    );
    assert_eq!(hub.try_recv(), None);
}

#[test]
fn sending_to_an_unknown_peer_fails() {
    let context = Context::new().unwrap();
    let mut peer = context.socket::<Peer>();
    let nobody = PeerId::new(7).unwrap();
    assert_eq!(peer.send(nobody, "hi"), Err(NoSuchPeer(Frame::from("hi"))));
    assert_eq!(
        peer.try_send(nobody, "hi"),
        Err(PeerSendError::NoSuchPeer(Frame::from("hi")))
    );
}

#[test]
fn a_disconnected_peer_is_forgotten() {
    let context = Context::new().unwrap();
    let mut far = context.socket::<Peer>();
    far.bind("inproc://far").unwrap();
    let mut near = context.socket::<Peer>();
    let id = near.connect_peer("inproc://far").unwrap();
    assert!(near.disconnect(id));
    assert!(!near.disconnect(id));
    assert_eq!(near.send(id, "gone"), Err(NoSuchPeer(Frame::from("gone"))));
}

#[test]
fn a_bad_endpoint_is_an_error() {
    let context = Context::new().unwrap();
    let mut peer = context.socket::<Peer>();
    assert!(matches!(
        peer.connect_peer("tcp://nonsense"),
        Err(zmq::Error::InvalidEndpoint(_))
    ));
}

#[test]
fn a_channel_talks_both_ways() {
    let context = Context::new().unwrap();
    let mut left = context.socket::<Channel>();
    let bound = left.bind("tcp://127.0.0.1:*").unwrap();
    let mut right = context.socket::<Channel>();
    right.connect(&bound.to_string()).unwrap();
    right.send("ping");
    let (heard, mut left) = within(FIVE, move || (left.recv(), left));
    assert_eq!(heard, Frame::from("ping"));
    left.send("pong");
    assert_eq!(within(FIVE, move || right.recv()), Frame::from("pong"));
}

#[test]
fn a_channel_has_one_peer() {
    let context = Context::new().unwrap();
    let mut hub = context.socket::<Channel>();
    hub.bind("inproc://one-peer").unwrap();
    let mut first = context.socket::<Channel>();
    first.connect("inproc://one-peer").unwrap();
    let mut second = context.socket::<Channel>();
    second.connect("inproc://one-peer").unwrap();
    hub.send("for the first");
    assert_eq!(first.try_recv(), Some(Frame::from("for the first")));
    assert_eq!(second.try_recv(), None);
    assert!(second.try_send("anyone?").is_err(), "turned away");
}

#[test]
fn a_channel_without_a_peer_hands_the_frame_back() {
    let context = Context::new().unwrap();
    let mut channel = context.socket::<Channel>();
    assert_eq!(channel.try_send("alone"), Err(Frame::from("alone")));
}
