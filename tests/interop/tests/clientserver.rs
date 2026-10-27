//! CLIENT and SERVER against libzmq's, when libzmq was built with its draft
//! sockets: without them, its CLIENT and SERVER abort on their first
//! handshake. Replies are tested from our SERVER.
#![cfg(feature = "draft")]

use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Client, Context, Frame, Server};
use zmq_interop::libzmq::{self, Kind, Socket};
use zmq_interop::{Peer, Security};
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
fn a_libzmq_client_talks_to_our_server() {
    if !libzmq::has_drafts() {
        return;
    }
    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>();
    let bound = server.bind("tcp://127.0.0.1:*").unwrap();
    let client = Socket::new(Kind::Client);
    client.set_receive_timeout(FIVE);
    client.connect(bound);
    client.send(&[b"ping"]).unwrap();
    let (id, request, mut server) = within(FIVE, move || {
        let (id, request) = server.recv();
        (id, request, server)
    });
    assert_eq!(request, Frame::from("ping"));
    server.send(id, "pong").unwrap();
    assert_eq!(client.recv().unwrap(), [b"pong"]);
}

#[test]
fn our_client_reaches_a_libzmq_server() {
    if !libzmq::has_drafts() {
        return;
    }
    let server = Socket::new(Kind::Server);
    server.set_receive_timeout(FIVE);
    let addr = server.bind_loopback();
    let context = Context::new().unwrap();
    let mut client = context.socket::<Client>();
    client.connect(&format!("tcp://{addr}")).unwrap();
    client.send("hello libzmq");
    assert_eq!(server.recv().unwrap(), [b"hello libzmq"]);
}

#[test]
fn our_client_drops_multipart_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let context = Context::new().unwrap();
    let mut client = context.socket::<Client>();
    client
        .connect(&format!("tcp://{}", listener.local_addr().unwrap()))
        .unwrap();
    let mut peer = Peer::accept(
        &listener,
        SocketType::Server,
        Identity::default(),
        Security::Null,
    )
    .unwrap();
    let multipart = Message::try_from(vec![Frame::from("not"), Frame::from("allowed")]).unwrap();
    peer.send(&multipart).unwrap();
    peer.send(&Message::from("single")).unwrap();
    assert_eq!(within(FIVE, move || client.recv()), Frame::from("single"));
}

#[test]
fn our_server_drops_multipart_messages() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>();
    let bound = server.bind("tcp://127.0.0.1:*").unwrap();
    let mut peer = Peer::connect(bound.tcp_addr().unwrap(), SocketType::Client, b"").unwrap();
    let multipart = Message::try_from(vec![Frame::from("not"), Frame::from("allowed")]).unwrap();
    peer.send(&multipart).unwrap();
    peer.send(&Message::from("single")).unwrap();
    let (_, frame) = within(FIVE, move || server.recv());
    assert_eq!(frame, Frame::from("single"));
}

#[test]
fn a_libzmq_server_replies_to_our_client() {
    if !libzmq::has_drafts() {
        return;
    }
    let server = Socket::new(Kind::Server);
    server.set_receive_timeout(FIVE);
    let addr = server.bind_loopback();
    let context = Context::new().unwrap();
    let mut client = context.socket::<Client>();
    client.connect(&format!("tcp://{addr}")).unwrap();
    client.send("question");
    let (id, body) = server.recv_routed().unwrap();
    assert_eq!(body, b"question");
    server.send_routed(id, b"answer").unwrap();
    assert_eq!(within(FIVE, move || client.recv()), Frame::from("answer"));
}
