//! Subscriptions on the wire: SUBSCRIBE and CANCEL commands with ZMTP 3.1
//! peers, the message form with ZMTP 3.0 peers, and replay after a
//! reconnection. Sockets always see the message form.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq_core::{ConnectionConfig, Context, Reconnect};
use zmq_interop::{Peer, Security};
use zmq_test_ports::free_port;
use zmtp::{
    Command, Decoder, DecoderConfig, Encoder, Frame, Identity, Incoming, Message, Metadata,
    PropertyName, SocketType, Subscription, Version,
};

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

fn config(socket_type: SocketType, reconnect: Reconnect) -> ConnectionConfig {
    ConnectionConfig {
        reconnect,
        ..ConnectionConfig::new(socket_type)
    }
}

fn subscribe(topic: &'static str) -> Message {
    Subscription::Subscribe(Frame::from(topic)).to_message()
}

fn cancel(topic: &'static str) -> Message {
    Subscription::Cancel(Frame::from(topic)).to_message()
}

fn accept(listener: &TcpListener, socket_type: SocketType) -> Peer {
    Peer::accept(listener, socket_type, Identity::default(), Security::Null).unwrap()
}

/// The next command from `peer`, skipping heartbeats.
fn next_command(peer: &mut Peer) -> Command {
    loop {
        match peer.recv().unwrap() {
            Incoming::Command(body) => match Command::parse(&body).unwrap() {
                Command::Ping(_) | Command::Pong(_) => {}
                command => return command,
            },
            Incoming::Message(message) => panic!("expected a command, got {message:?}"),
        }
    }
}

#[test]
fn a_sub_sends_subscribe_and_cancel_commands_to_zmtp_3_1_peers() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let mut connection = context.connect(
        listener.local_addr().unwrap(),
        config(SocketType::Sub, Reconnect::Never),
    );
    connection.tx.send_blocking(subscribe("news")).unwrap();
    connection.tx.send_blocking(cancel("news")).unwrap();
    let mut peer = accept(&listener, SocketType::Pub);
    assert_eq!(
        next_command(&mut peer),
        Command::Subscribe(Frame::from("news"))
    );
    assert_eq!(
        next_command(&mut peer),
        Command::Cancel(Frame::from("news"))
    );
}

#[test]
fn a_pub_receives_subscribe_and_cancel_commands_as_messages() {
    let context = Context::new().unwrap();
    let mut listener = context
        .bind(free_port(), ConnectionConfig::new(SocketType::Pub))
        .unwrap();
    let mut peer = Peer::connect(listener.local_addr(), SocketType::Sub, b"").unwrap();
    peer.send_command(&Command::Subscribe(Frame::from("news")))
        .unwrap();
    peer.send_command(&Command::Cancel(Frame::from("news")))
        .unwrap();
    let received = within(FIVE, move || {
        let mut connection = listener.accept_blocking().unwrap();
        let first = connection.rx.recv_blocking();
        let second = connection.rx.recv_blocking();
        (first, second)
    });
    assert_eq!(received, (Some(subscribe("news")), Some(cancel("news"))));
}

#[test]
fn other_sockets_ignore_subscribe_commands() {
    let context = Context::new().unwrap();
    let mut listener = context
        .bind(free_port(), ConnectionConfig::new(SocketType::Dealer))
        .unwrap();
    let mut peer = Peer::connect(listener.local_addr(), SocketType::Dealer, b"").unwrap();
    peer.send_command(&Command::Subscribe(Frame::from("news")))
        .unwrap();
    peer.send(&Message::from("data")).unwrap();
    let received = within(FIVE, move || {
        let mut connection = listener.accept_blocking().unwrap();
        connection.rx.recv_blocking()
    });
    assert_eq!(received, Some(Message::from("data")));
}

#[test]
fn subscriptions_are_replayed_after_reconnecting() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let mut connection = context.connect(
        listener.local_addr().unwrap(),
        config(
            SocketType::Sub,
            Reconnect::Fixed {
                interval: Duration::from_millis(20),
            },
        ),
    );
    for message in [subscribe("a"), subscribe("b"), cancel("a")] {
        connection.tx.send_blocking(message).unwrap();
    }
    let mut first = accept(&listener, SocketType::Pub);
    for _ in 0..3 {
        next_command(&mut first);
    }
    drop(first);

    let mut second = within(FIVE, move || accept(&listener, SocketType::Pub));
    assert_eq!(
        next_command(&mut second),
        Command::Subscribe(Frame::from("b"))
    );
    // Nothing else was replayed: the next command is the next one sent.
    connection.tx.send_blocking(subscribe("c")).unwrap();
    assert_eq!(
        next_command(&mut second),
        Command::Subscribe(Frame::from("c"))
    );
}

#[test]
fn zmtp_3_0_peers_get_subscriptions_as_messages() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let mut connection = context.connect(
        listener.local_addr().unwrap(),
        config(SocketType::Sub, Reconnect::Never),
    );
    connection.tx.send_blocking(subscribe("news")).unwrap();

    let (mut stream, _) = listener.accept().unwrap();
    stream.set_read_timeout(Some(FIVE)).unwrap();
    // A ZMTP 3.0 greeting: signature, version 3.0, NULL, not a server.
    let mut greeting = [0u8; 64];
    greeting[0] = 0xff;
    greeting[9] = 0x7f;
    greeting[10] = 3;
    greeting[12..16].copy_from_slice(b"NULL");
    stream.write_all(&greeting).unwrap();
    let mut theirs = [0u8; 64];
    stream.read_exact(&mut theirs).unwrap();

    let encoder = Encoder::new(Version::V3_0);
    let mut metadata = Metadata::new();
    metadata
        .insert(PropertyName::new("Socket-Type").unwrap(), "PUB")
        .unwrap();
    let mut body = Vec::new();
    Command::Ready(metadata).encode(&mut body);
    let mut out = Vec::new();
    encoder.encode_command(&body, &mut out).unwrap();
    stream.write_all(&out).unwrap();

    let mut decoder = Decoder::new(Version::V3_0, DecoderConfig::default());
    let mut items = Vec::new();
    while items.len() < 2 {
        match decoder.decode().unwrap() {
            Some(item) => items.push(item),
            None => {
                let read = decoder.fill(|buf| stream.read(buf)).unwrap();
                assert_ne!(read, 0, "closed after {items:?}");
            }
        }
    }
    assert!(matches!(&items[0], Incoming::Command(body) if body.starts_with(b"\x05READY")));
    assert_eq!(items[1], Incoming::Message(subscribe("news")));
}

#[test]
fn other_sockets_send_subscription_shaped_messages_as_messages() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let mut connection = context.connect(
        listener.local_addr().unwrap(),
        config(SocketType::Dealer, Reconnect::Never),
    );
    connection.tx.send_blocking(subscribe("data")).unwrap();
    let mut peer = accept(&listener, SocketType::Dealer);
    assert_eq!(peer.recv_message().unwrap(), subscribe("data"));
}
