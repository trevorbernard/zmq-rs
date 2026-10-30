//! PEER and CHANNEL against libzmq's, when libzmq was built with drafts,
//! and a CHANNEL dropping multipart messages.
#![cfg(feature = "draft")]

use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Channel, Context, Frame, Peer};
use zmq_interop::Security;
use zmq_interop::libzmq::{self, Kind, Socket};
use zmq_test_ports::{free_endpoint, free_port};
use zmtp::{Identity, Message, SocketType};

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
fn a_libzmq_peer_and_ours_address_each_other() {
    if !libzmq::has_drafts() {
        return;
    }
    let context = Context::new().unwrap();
    let mut ours = context.socket::<Peer>();
    let bound = ours.bind(&free_endpoint()).unwrap();
    let theirs = Socket::new(Kind::Peer);
    theirs.set_receive_timeout(FIVE);
    let to_ours = theirs.connect_peer(bound);
    theirs.send_routed(to_ours, b"hello from libzmq").unwrap();
    let (to_theirs, greeting, mut ours) = within(FIVE, move || {
        let (from, greeting) = ours.recv();
        (from, greeting, ours)
    });
    assert_eq!(greeting, Frame::from("hello from libzmq"));
    ours.send(to_theirs, "hello from rust").unwrap();
    assert_eq!(
        theirs.recv_routed().unwrap(),
        (to_ours, b"hello from rust".to_vec())
    );
}

#[test]
fn our_peer_connects_to_a_libzmq_peer() {
    if !libzmq::has_drafts() {
        return;
    }
    let theirs = Socket::new(Kind::Peer);
    theirs.set_receive_timeout(FIVE);
    let addr = theirs.bind_loopback();
    let context = Context::new().unwrap();
    let mut ours = context.socket::<Peer>();
    let to_theirs = ours.connect_peer(&format!("tcp://{addr}")).unwrap();
    ours.send(to_theirs, "queued before the connection is up")
        .unwrap();
    let (to_ours, body) = theirs.recv_routed().unwrap();
    assert_eq!(body, b"queued before the connection is up");
    theirs.send_routed(to_ours, b"reply").unwrap();
    assert_eq!(
        within(FIVE, move || ours.recv()),
        (to_theirs, Frame::from("reply"))
    );
}

#[test]
fn a_libzmq_channel_talks_to_ours() {
    if !libzmq::has_drafts() {
        return;
    }
    let context = Context::new().unwrap();
    let mut ours = context.socket::<Channel>();
    let bound = ours.bind(&free_endpoint()).unwrap();
    let theirs = Socket::new(Kind::Channel);
    theirs.set_receive_timeout(FIVE);
    theirs.connect(bound);
    theirs.send(&[b"ping"]).unwrap();
    let (heard, mut ours) = within(FIVE, move || (ours.recv(), ours));
    assert_eq!(heard, Frame::from("ping"));
    ours.send("pong");
    assert_eq!(theirs.recv().unwrap(), [b"pong"]);
}

#[test]
fn our_channel_drops_multipart_messages() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let mut channel = context.socket::<Channel>();
    channel
        .connect(&format!("tcp://{}", listener.local_addr().unwrap()))
        .unwrap();
    let mut peer = zmq_interop::Peer::accept(
        &listener,
        SocketType::Channel,
        Identity::default(),
        Security::Null,
    )
    .unwrap();
    let multipart = Message::try_from(vec![Frame::from("not"), Frame::from("allowed")]).unwrap();
    peer.send(&multipart).unwrap();
    peer.send(&Message::from("single")).unwrap();
    assert_eq!(within(FIVE, move || channel.recv()), Frame::from("single"));
}
