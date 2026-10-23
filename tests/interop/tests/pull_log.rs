//! PULL reads tcp and ipc connections straight into a byte log and decodes
//! them on its own thread. These check what that changes at the edges:
//! messages larger than the log, commands among the frames, lending
//! messages in place, and a connection that breaks part way through a
//! message.

use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Frame, Message, Pull, Push};
use zmq_interop::libzmq::{self, Kind};
use zmq_interop::{Peer, Security};
use zmtp::{Command, Encoder, Identity, Incoming, Ping, PingContext, SocketType, Ttl, Version};

const FIVE: Duration = Duration::from_secs(5);

/// More than PULL's receive log holds.
const LARGER_THAN_THE_LOG: usize = 5 * 1024 * 1024;

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

fn encode(message: &Message) -> Vec<u8> {
    let mut out = Vec::new();
    Encoder::new(Version::V3_1).encode(message, &mut out);
    out
}

#[test]
fn messages_from_libzmq_of_every_size_arrive_whole_and_in_order() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let theirs = libzmq::Context::new();
    let push = libzmq::Socket::in_context(&theirs, Kind::Push);
    push.connect(bound.clone());
    let sizes = [
        0,
        1,
        255,
        256,
        70_000,
        3 * 1024 * 1024,
        LARGER_THAN_THE_LOG,
        9,
    ];
    let sent: Vec<Message> = sizes.iter().map(|&size| large(size)).collect();
    for message in &sent {
        let frames: Vec<&[u8]> = message.iter().map(|frame| frame.as_slice()).collect();
        push.send(&frames).expect("libzmq send");
    }
    let received = within(FIVE * 2, move || {
        (0..sizes.len()).map(|_| pull.recv()).collect::<Vec<_>>()
    });
    assert_eq!(received, sent);
    drop(push);
}

#[test]
fn messages_are_lent_in_place() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&bound.to_string()).unwrap();
    let sent = [
        Message::try_from(vec![Frame::from("a"), Frame::from(vec![7; 300])]).unwrap(),
        large(LARGER_THAN_THE_LOG),
        Message::from("b"),
    ];
    for message in sent.clone() {
        push.send(message);
    }
    let received = within(FIVE * 2, move || {
        (0..3)
            .map(|_| pull.recv_with(|message| message.to_message()))
            .collect::<Vec<_>>()
    });
    assert_eq!(received, sent);
}

#[test]
fn pull_answers_pings_between_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.connect(&format!("tcp://{}", listener.local_addr().unwrap()))
        .unwrap();
    let mut push = Peer::accept(
        &listener,
        SocketType::Push,
        Identity::default(),
        Security::Null,
    )
    .expect("handshake");
    push.send(&Message::from("before")).unwrap();
    push.send_command(&Command::Ping(Ping {
        ttl: Ttl::ZERO,
        context: PingContext::default(),
    }))
    .unwrap();
    push.send(&Message::from("after")).unwrap();
    let received = within(FIVE, move || [pull.recv(), pull.recv()]);
    assert_eq!(received, [Message::from("before"), Message::from("after")]);
    let pong = loop {
        if let Incoming::Command(body) = push.recv().expect("an answer") {
            break Command::parse(&body).expect("a command");
        }
    };
    assert!(matches!(pong, Command::Pong(_)), "got {pong:?}");
}

/// Sends `cut` octets of an encoded message larger than `body`, then breaks
/// the connection; after reconnecting, the next message must arrive whole.
fn break_part_way(body: usize, cut: usize) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.connect(&format!("tcp://{}", listener.local_addr().unwrap()))
        .unwrap();
    let mut first = Peer::accept(
        &listener,
        SocketType::Push,
        Identity::default(),
        Security::Null,
    )
    .expect("first handshake");
    first.send(&Message::from("before")).unwrap();
    let encoded = encode(&large(body));
    first.send_raw(&encoded[..cut]).unwrap();
    // Let PULL's engine read it all before the connection breaks.
    thread::sleep(Duration::from_millis(200));
    drop(first);
    let mut second = Peer::accept(
        &listener,
        SocketType::Push,
        Identity::default(),
        Security::Null,
    )
    .expect("second handshake after reconnecting");
    second.send(&Message::from("after")).unwrap();
    let received = within(FIVE, move || [pull.recv(), pull.recv()]);
    assert_eq!(received, [Message::from("before"), Message::from("after")]);
}

#[test]
fn a_break_part_way_through_a_message_drops_it() {
    break_part_way(100_000, 50_000);
}

#[test]
fn a_break_part_way_through_a_message_larger_than_half_the_log_drops_it() {
    // Past half the log, the part already read is handed to the socket.
    break_part_way(LARGER_THAN_THE_LOG, 3 * 1024 * 1024);
}
