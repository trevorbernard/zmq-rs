//! libzmq's `tests/test_spec_pushpull.cpp`: PUSH and PULL against RFC 30.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::time::Duration;

use zmq::{Context, Frame, Hwm, Linger, Message, Pull, Push, SocketOptions};

use crate::reqrep_inproc::msg;
use crate::support::{FIVE, settle};

fn inproc(name: &str) -> String {
    format!("inproc://spec_pushpull_{name}")
}

const TCP: &str = "tcp://127.0.0.1:*";

/// A high-water mark of one message both ways, and no linger, as the
/// cases below set with `ZMQ_SNDHWM`, `ZMQ_RCVHWM` and `ZMQ_LINGER`.
fn tight() -> SocketOptions {
    let one = Hwm::Messages(NonZeroUsize::MIN);
    SocketOptions {
        send_hwm: one,
        recv_hwm: one,
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    }
}

/// PUSH: SHALL route outgoing messages to connected peers using a
/// round-robin strategy.
fn push_round_robin_out(bind_address: &str) {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    let connect_address = push.bind(bind_address).unwrap().to_string();
    let mut pulls: Vec<_> = (0..5)
        .map(|_| {
            let mut pull = context.socket::<Pull>();
            pull.connect(&connect_address).unwrap();
            pull
        })
        .collect();
    // Wait for connections.
    settle();

    // Send 2N messages
    for _ in &pulls {
        push.send("ABC");
    }
    for _ in &pulls {
        push.send("DEF");
    }

    // Expect every PULL got one of each
    for pull in &mut pulls {
        assert_eq!(pull.recv_timeout(FIVE), Some(msg(&["ABC"])));
        assert_eq!(pull.recv_timeout(FIVE), Some(msg(&["DEF"])));
    }
}

#[test]
fn push_round_robin_out_inproc() {
    push_round_robin_out(&inproc("round_robin_out"));
}

#[test]
fn push_round_robin_out_tcp() {
    push_round_robin_out(TCP);
}

/// PULL: SHALL receive incoming messages from its peers using a
/// fair-queuing strategy.
fn pull_fair_queue_in(bind_address: &str) {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let connect_address = pull.bind(bind_address).unwrap().to_string();
    let mut pushes: Vec<_> = (0..5)
        .map(|_| {
            let mut push = context.socket::<Push>();
            push.connect(&connect_address).unwrap();
            push
        })
        .collect();
    // Wait for connections.
    settle();

    // Send 2N messages: A to E first, then F to J, one of each per pusher
    let mut first_half = HashSet::new();
    let mut second_half = HashSet::new();
    for (peer, push) in (b'A'..).zip(&mut pushes) {
        let first = (peer as char).to_string();
        let second = ((peer + 5) as char).to_string();
        push.send(first.clone());
        push.send(second.clone());
        first_half.insert(msg(&[&first]));
        second_half.insert(msg(&[&second]));
    }
    // Wait for data.
    settle();

    // Expect to pull one from each first
    for _ in &pushes {
        let message = pull.recv_timeout(FIVE).expect("a message");
        assert!(first_half.remove(&message), "{message:?} out of turn");
    }
    // And then get the second batch
    for _ in &pushes {
        let message = pull.recv_timeout(FIVE).expect("a message");
        assert!(second_half.remove(&message), "{message:?} out of turn");
    }
}

#[test]
fn pull_fair_queue_in_inproc() {
    pull_fair_queue_in(&inproc("fair_queue_in"));
}

#[test]
fn pull_fair_queue_in_tcp() {
    pull_fair_queue_in(TCP);
}

/// PUSH: SHALL block on sending, or return a suitable error, when it has
/// no available peers. libzmq's `ZMQ_SNDTIMEO` is `send_timeout` here.
#[test]
fn push_block_on_send_no_peers() {
    let context = Context::new().unwrap();
    let mut sc = context.socket::<Push>();
    assert!(sc.try_send(Message::new("")).is_err());
    assert!(
        sc.send_timeout(Message::new(""), Duration::from_millis(250))
            .is_err()
    );
}

/// PUSH and PULL: SHALL create this queue when a peer connects to it. If
/// this peer disconnects, the socket SHALL destroy its queue and SHALL
/// discard any messages it contains.
///
/// libzmq leaves this case disabled, "until libzmq does this properly";
/// `docs/rfc-compliance.md` says zmq-rs does, so it runs here, over inproc
/// only: over tcp the I/O threads and the kernel buffer messages beyond
/// both high-water marks, so "both queues full" is not two messages.
#[test]
fn destroy_queue_on_disconnect_inproc() {
    let endpoint = inproc("destroy_queue");
    let context = Context::new().unwrap();
    let mut a = context.socket_with::<Push>(tight());
    a.bind(&endpoint).unwrap();
    let mut b = context.socket_with::<Pull>(tight());
    b.connect(&endpoint).unwrap();

    // Send two messages, one should be stuck in A's outgoing queue, the
    // other arrives at B.
    a.send("ABC");
    a.send("DEF");
    // Both queues should now be full, indicated by A blocking on send.
    assert!(a.try_send(Message::new("")).is_err());

    b.disconnect(&endpoint).unwrap();
    // Disconnect may take time
    settle();

    // Can't receive old data on B.
    assert_eq!(b.try_recv(), None);
    // Sending fails.
    assert!(a.try_send(Message::new("")).is_err());

    // Reconnect B
    b.connect(&endpoint).unwrap();
    // Still can't receive old data on B.
    assert_eq!(b.try_recv(), None);
    // two messages should be sendable before the queues are filled up.
    a.send("ABC");
    a.send("DEF");
    assert!(a.try_send(Message::new("")).is_err());
}

/// A message of `parts`, then a large frame of `fill`, as the case below
/// sends: large enough to overrun the TCP buffers.
fn with_large_frame(parts: &[&str], fill: u8) -> Message {
    const LEN: usize = 16 * 1024 * 1024;
    let mut message = msg(parts);
    message.push(Frame::from(vec![fill; LEN]));
    message
}

/// PUSH and PULL: SHALL either receive or drop multipart messages
/// atomically. libzmq sends frame by frame with `ZMQ_SNDMORE`, and the
/// case checks that a disconnect and reconnect between frames never
/// delivers part of a message. A zmq-rs message is sent whole, so the
/// same sequence checks that what arrives around a reconnect is whole.
fn push_multipart_atomic_drop(bind_address: &str, block: bool) {
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(tight());
    let connect_address = push.bind(bind_address).unwrap().to_string();
    let mut pull = context.socket_with::<Pull>(tight());
    pull.connect(&connect_address).unwrap();
    // Wait for connections.
    settle();

    // Normal case - exercise the queues
    push.send(with_large_frame(&["0", "0"], b'a'));
    assert_eq!(
        pull.recv_timeout(FIVE),
        Some(with_large_frame(&["0", "0"], b'a'))
    );

    // Fill the HWMs of sender and receiver, one message each
    push.send("1");
    push.send(with_large_frame(&["2", "2"], b'b'));

    // Disconnect, and let the pipes start to be deallocated
    pull.disconnect(&connect_address).unwrap();
    settle();

    // Reconnect and immediately push a large message into the pipe
    pull.connect(&connect_address).unwrap();
    let mut three = with_large_frame(&["3", "3"], b'c');
    three.push("3b");
    if block {
        push.send(three.clone());
    } else if let Err(back) = push.try_send(three.clone()) {
        // inproc won't fail, much faster to connect/disconnect pipes than
        // TCP; at this point the new pipe is there and it works
        push.send(back);
    }
    settle();

    push.send(with_large_frame(&["5", "5"], b'd'));

    // On very slow machines the message will not be lost, as it will be
    // sent when the new pipe is already in place.
    let first = pull.recv_timeout(FIVE).expect("a message");
    if first != with_large_frame(&["5", "5"], b'd') {
        assert_eq!(first, three);
        assert_eq!(
            pull.recv_timeout(FIVE),
            Some(with_large_frame(&["5", "5"], b'd'))
        );
    }
}

#[test]
fn push_multipart_atomic_drop_block_inproc() {
    push_multipart_atomic_drop(&inproc("atomic_drop_block"), true);
}

#[test]
fn push_multipart_atomic_drop_non_block_inproc() {
    push_multipart_atomic_drop(&inproc("atomic_drop_non_block"), false);
}

#[test]
fn push_multipart_atomic_drop_block_tcp() {
    push_multipart_atomic_drop(TCP, true);
}

#[test]
fn push_multipart_atomic_drop_non_block_tcp() {
    push_multipart_atomic_drop(TCP, false);
}
