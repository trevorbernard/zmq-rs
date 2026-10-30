//! libzmq's `tests/test_hwm_pubsub.cpp`: an XPUB queues at most its HWM
//! for a subscriber, and with `ZMQ_XPUB_NODROP`, here
//! [`try_send_all`](zmq::Socket::try_send_all), refuses rather than drops
//! once the subscriber is full.
//!
//! libzmq sets the XPUB's HWM after the subscriber connects; zmq-rs fixes
//! options when a socket is made, so the HWM is set then.

use std::num::NonZeroUsize;
use std::time::Duration;

use zmq::{Context, Hwm, Message, Pub, Socket, SocketOptions, Sub, XPub, XPubEvent};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, settle};

fn hwm(messages: usize) -> Hwm {
    Hwm::Messages(NonZeroUsize::new(messages).unwrap())
}

/// An XPUB with `send_hwm` bound at `endpoint` and a SUB connected to it,
/// once the SUB's subscription to everything has arrived.
fn xpub_and_sub(send_hwm: usize, endpoint: &str) -> (Socket<XPub>, Socket<Sub>) {
    let context = Context::new().unwrap();
    let mut pub_socket = context.socket_with::<XPub>(SocketOptions {
        send_hwm: hwm(send_hwm),
        ..SocketOptions::default()
    });
    let pub_endpoint = pub_socket.bind(endpoint).unwrap().to_string();
    let mut sub_socket = context.socket::<Sub>();
    sub_socket.connect(&pub_endpoint).unwrap();
    sub_socket.subscribe("");

    // Wait before sending till the one subscriber has subscribed
    assert_eq!(
        pub_socket.recv_timeout(FIVE),
        Some(XPubEvent::Subscribe("".into()))
    );
    (pub_socket, sub_socket)
}

fn defaults(send_hwm: usize, msg_cnt: usize, endpoint: &str) -> usize {
    let (mut pub_socket, mut sub_socket) = xpub_and_sub(send_hwm, endpoint);

    // Send until we reach the mute state
    let mut send_count = 0;
    while send_count < msg_cnt && pub_socket.send("test message") == 1 {
        send_count += 1;
    }
    assert_eq!(send_count, send_hwm);
    settle();

    let mut recv_count = 0;
    while sub_socket.try_recv().is_some() {
        recv_count += 1;
    }
    assert_eq!(recv_count, send_hwm);
    recv_count
}

/// Receives until a receive times out or the termination message comes.
/// Returns how many it received, counting the termination message, and
/// whether it came.
fn receive(socket: &mut Socket<Sub>) -> (usize, bool) {
    let mut recv_count = 0;
    while let Some(message) = socket.recv_timeout(Duration::from_millis(10)) {
        recv_count += 1;
        if message == Message::from("end") {
            return (recv_count, true);
        }
    }
    (recv_count, false)
}

/// Over tcp and ipc, `must_block` is false: zmq-rs's XPUB queue empties as
/// its I/O thread writes, so 6000 small messages fit in the subscriber's
/// queue and the kernel's buffers without a refusal. libzmq's writer
/// learns of reads only in batches, so its queue looks full at the HWM.
fn blocking(send_hwm: usize, msg_cnt: usize, endpoint: &str, must_block: bool) -> usize {
    let (mut pub_socket, mut sub_socket) = xpub_and_sub(send_hwm, endpoint);

    // Send until we block
    let mut send_count = 0;
    let mut recv_count = 0;
    let mut blocked_count = 0;
    while send_count < msg_cnt {
        if pub_socket.try_send_all(Message::from("")).is_ok() {
            send_count += 1;
        } else {
            blocked_count += 1;
            recv_count += receive(&mut sub_socket).0;
        }
    }
    // With fewer HWM than messages, we should block at least once
    assert!(
        !must_block || blocked_count > 0,
        "sent = {send_count}, received = {recv_count}"
    );

    // Dequeue the SUB again, to make sure the XPUB has room for the
    // termination message
    let (received, mut is_termination) = receive(&mut sub_socket);
    recv_count += received;
    pub_socket.send("end");
    // Now block on the SUB side till we get the termination message
    while !is_termination {
        let (received, terminated) = receive(&mut sub_socket);
        recv_count += received;
        is_termination = terminated;
    }
    // Remove the termination message from the count
    recv_count -= 1;
    assert_eq!(recv_count, send_count);
    recv_count
}

/// The HWM applies to the messages queued now, not to all ever sent:
/// with an HWM of 11024, send 9999, receive 9999, send 1100, receive 1100.
#[test]
fn reset_hwm() {
    let first_count = 9999;
    let second_count = 1100;
    let hwm_value = hwm(11024);
    let context = Context::new().unwrap();
    let mut pub_socket = context.socket_with::<Pub>(SocketOptions {
        send_hwm: hwm_value,
        ..SocketOptions::default()
    });
    let endpoint = pub_socket.bind(&free_endpoint()).unwrap().to_string();
    let mut sub_socket = context.socket_with::<Sub>(SocketOptions {
        recv_hwm: hwm_value,
        ..SocketOptions::default()
    });
    sub_socket.connect(&endpoint).unwrap();
    sub_socket.subscribe("");
    settle();

    for count in [first_count, second_count] {
        let mut send_count = 0;
        while send_count < count && pub_socket.send("") == 1 {
            send_count += 1;
        }
        assert_eq!(send_count, count);
        settle();

        let mut recv_count = 0;
        while sub_socket.try_recv().is_some() {
            recv_count += 1;
        }
        assert_eq!(recv_count, count);
        settle();
    }
}

macro_rules! regular_test_cases {
    ($name:ident, $endpoint:expr, $must_block:expr) => {
        mod $name {
            use super::*;

            /// Send 1000 messages on an HWM of 1000, receive 1000.
            #[test]
            fn defaults_large() {
                assert_eq!(defaults(1000, 1000, $endpoint), 1000);
            }

            /// Send 100 messages on an HWM of 100, receive 100.
            #[test]
            fn defaults_small() {
                assert_eq!(defaults(100, 100, $endpoint), 100);
            }

            /// Send 6000 messages on an HWM of 2000, refused above it
            /// rather than dropped, and receive them all.
            #[test]
            fn blocking() {
                assert_eq!(super::blocking(2000, 6000, $endpoint, $must_block), 6000);
            }
        }
    };
}

regular_test_cases!(tcp, &free_endpoint(), false);
regular_test_cases!(inproc, "inproc://libzmq-hwm-pubsub", true);
#[cfg(unix)]
regular_test_cases!(ipc, "ipc://*", false);
