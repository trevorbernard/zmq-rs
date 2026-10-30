//! SCATTER and GATHER against libzmq's, when libzmq was built with drafts,
//! and a GATHER dropping multipart messages.
#![cfg(feature = "draft")]

use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Frame, Gather, Scatter};
use zmq_interop::libzmq::{self, Kind, Socket};
use zmq_interop::{Peer, Security};
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
fn a_libzmq_scatter_feeds_our_gather() {
    if !libzmq::has_drafts() {
        return;
    }
    let context = Context::new().unwrap();
    let mut gather = context.socket::<Gather>();
    let bound = gather.bind(&free_endpoint()).unwrap();
    let scatter = Socket::new(Kind::Scatter);
    scatter.connect(bound);
    for i in 0..3 {
        scatter.send(&[format!("{i}").as_bytes()]).unwrap();
    }
    let received = within(FIVE, move || [gather.recv(), gather.recv(), gather.recv()]);
    assert_eq!(received, ["0", "1", "2"].map(Frame::from));
}

#[test]
fn our_scatter_feeds_a_libzmq_gather() {
    if !libzmq::has_drafts() {
        return;
    }
    let gather = Socket::new(Kind::Gather);
    gather.set_receive_timeout(FIVE);
    let addr = gather.bind_loopback();
    let context = Context::new().unwrap();
    let mut scatter = context.socket::<Scatter>();
    scatter.connect(&format!("tcp://{addr}")).unwrap();
    scatter.send("to libzmq");
    assert_eq!(gather.recv().unwrap(), [b"to libzmq"]);
}

#[test]
fn our_gather_drops_multipart_messages() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let mut gather = context.socket::<Gather>();
    gather
        .connect(&format!("tcp://{}", listener.local_addr().unwrap()))
        .unwrap();
    let mut peer = Peer::accept(
        &listener,
        SocketType::Scatter,
        Identity::default(),
        Security::Null,
    )
    .unwrap();
    let multipart = Message::try_from(vec![Frame::from("not"), Frame::from("allowed")]).unwrap();
    peer.send(&multipart).unwrap();
    peer.send(&Message::from("single")).unwrap();
    assert_eq!(within(FIVE, move || gather.recv()), Frame::from("single"));
}
