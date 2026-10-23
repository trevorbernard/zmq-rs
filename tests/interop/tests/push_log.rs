//! PUSH sends through a byte log of ZMTP 3.x frames. These check what that
//! changes at the edges: messages larger than the log, peers whose framing
//! differs from the log's, and a connection that breaks part way through a
//! message.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Frame, Message, Pull, Push};
use zmq_core::ConnectionConfig;
use zmq_interop::{Peer, Security};
use zmtp::{Decoder, DecoderConfig, Identity, Incoming, SocketType, Version};

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

/// A header frame and a body of `len` octets that vary with position.
fn large(len: usize) -> Message {
    let body: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    Message::try_from(vec![Frame::from("large"), Frame::from(body)]).unwrap()
}

#[test]
fn messages_larger_than_the_log_arrive_whole_and_in_order() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&bound.to_string()).unwrap();
    let sent = [
        large(1024 * 1024),
        Message::from("small"),
        large(3 * 1024 * 1024 + 7),
        Message::from("last"),
    ];
    let expected = sent.clone();
    let sender = thread::spawn(move || {
        for message in sent {
            push.send(message);
        }
        push
    });
    let received = within(FIVE * 2, move || {
        (0..4).map(|_| pull.recv()).collect::<Vec<_>>()
    });
    assert_eq!(received, expected);
    drop(sender.join().unwrap());
}

#[test]
fn try_send_hands_back_a_message_larger_than_the_log() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&bound.to_string()).unwrap();
    let big = large(1024 * 1024);
    assert_eq!(push.try_send(big.clone()), Err(big));
    push.send("fits");
    assert_eq!(within(FIVE, move || pull.recv()), Message::from("fits"));
}

#[test]
fn a_zmtp_1_0_peer_gets_messages_reframed() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    let addr = push.bind("tcp://127.0.0.1:*").unwrap().tcp_addr().unwrap();
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(FIVE)).unwrap();
    // A ZMTP 1.0 greeting is the identity as a frame: here, empty.
    stream.write_all(&[0x01, 0x00]).unwrap();
    // Ours starts with a 1.0-compatible long frame header and our identity.
    let mut header = [0u8; 10];
    stream.read_exact(&mut header).unwrap();
    let identity_len = u64::from_be_bytes(header[1..9].try_into().unwrap()) - 1;
    let mut identity = vec![0u8; identity_len as usize];
    stream.read_exact(&mut identity).unwrap();

    let sent = [
        Message::try_from(vec![Frame::from("a"), Frame::from(vec![5; 300])]).unwrap(),
        Message::from("b"),
    ];
    for message in sent.clone() {
        push.send(message);
    }
    let mut decoder = Decoder::new(Version::V1, DecoderConfig::default());
    let mut received = Vec::new();
    while received.len() < sent.len() {
        if let Some(item) = decoder.decode().unwrap() {
            received.push(item);
            continue;
        }
        let read = decoder.fill(|buf| stream.read(buf)).unwrap();
        assert_ne!(read, 0, "closed");
    }
    let expected: Vec<Incoming> = sent.into_iter().map(Incoming::Message).collect();
    assert_eq!(received, expected);
}

#[test]
fn after_a_break_part_way_through_a_message_the_next_arrives_whole() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let context = zmq_core::Context::new().unwrap();
    let mut connection = context.connect(addr, ConnectionConfig::new(SocketType::Push));
    let first = Peer::accept(
        &listener,
        SocketType::Pull,
        Identity::default(),
        Security::Null,
    )
    .expect("first handshake");
    // Far more than the socket buffers hold, so the stream stalls part way
    // through while the first peer reads nothing.
    let huge = large(64 * 1024 * 1024);
    let sender = thread::spawn(move || {
        connection.tx.send_blocking(huge).unwrap();
        connection.tx.send_blocking(Message::from("after")).unwrap();
        connection
    });
    thread::sleep(Duration::from_millis(300));
    drop(first);
    let mut second = Peer::accept(
        &listener,
        SocketType::Pull,
        Identity::default(),
        Security::Null,
    )
    .expect("second handshake after reconnecting");
    let received = within(FIVE * 2, move || second.recv_message().unwrap());
    assert_eq!(received, Message::from("after"));
    drop(sender.join().unwrap());
}
