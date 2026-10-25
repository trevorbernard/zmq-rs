//! libzmq's `tests/test_router_mandatory.cpp`: `ZMQ_ROUTER_MANDATORY`, a
//! ROUTER send failing for an unknown routing id, and the draft
//! `zmq_socket_get_peer_state`, whether a peer has room.
//!
//! zmq-rs's ROUTER is always mandatory: a send to an unknown routing id
//! fails and hands the message back (README, "Differences from libzmq").
//! Whether a peer has room is what `try_send` reports, as `Full`.

use std::num::NonZeroUsize;

use zmq::{
    Context, Dealer, Hwm, Identity, Message, Router, RouterSendError, RoutingId, Socket,
    SocketOptions,
};

use crate::support::FIVE;

fn dealer_called(
    context: &Context,
    identity: &'static str,
    options: SocketOptions,
) -> Socket<Dealer> {
    context.socket_with::<Dealer>(SocketOptions {
        identity: Identity::new(identity).unwrap(),
        ..options
    })
}

#[test]
fn basic() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    let endpoint = router.bind("tcp://127.0.0.1:*").unwrap().to_string();

    // libzmq's default silently drops a message for an unknown peer;
    // zmq-rs reports it, as libzmq does with mandatory routing.
    let unknown = RoutingId::new("UNKNOWN").unwrap();
    assert!(matches!(
        router.try_send(&unknown, "DATA"),
        Err(RouterSendError::UnknownRoutingId(_))
    ));
    assert!(router.send(&unknown, "DATA").is_err());

    let mut dealer = dealer_called(&context, "X", SocketOptions::default());
    dealer.connect(&endpoint).unwrap();
    dealer.send("Hello");
    let (from, _) = router.recv_timeout(FIVE).expect("Hello from X");
    assert_eq!(from.as_bytes(), b"X");

    // Now X is known, the send works.
    router.send(&from, "Hello").unwrap();
    assert_eq!(dealer.recv_timeout(FIVE), Some(Message::from("Hello")));
}

#[test]
fn get_peer_state() {
    let hwm = Hwm::Messages(NonZeroUsize::new(100).unwrap());
    let context = Context::new().unwrap();
    let mut router = context.socket_with::<Router>(SocketOptions {
        send_hwm: hwm,
        ..SocketOptions::default()
    });
    router.bind("inproc://test_get_peer_state").unwrap();
    let low_hwm = || SocketOptions {
        recv_hwm: hwm,
        ..SocketOptions::default()
    };
    let mut dealer1 = dealer_called(&context, "X", low_hwm());
    let mut dealer2 = dealer_called(&context, "Y", low_hwm());
    dealer1.connect("inproc://test_get_peer_state").unwrap();
    dealer2.connect("inproc://test_get_peer_state").unwrap();

    // A message from each dealer, to know both connections are ready.
    for dealer in [&mut dealer1, &mut dealer2] {
        dealer.send("Hello");
        router.recv_timeout(FIVE).expect("Hello from a dealer");
    }

    let x = RoutingId::new("X").unwrap();
    let y = RoutingId::new("Y").unwrap();
    let (mut dealer1_sent, mut dealer2_sent, mut dealer1_received) = (0, 0, 0);
    let mut dealer2_blocked = false;
    for _ in 0..10_000 {
        if router.try_send(&x, "Hello").is_ok() {
            dealer1_sent += 1;
        }
        match router.try_send(&y, "Hello") {
            Ok(()) => dealer2_sent += 1,
            Err(RouterSendError::Full(_)) => dealer2_blocked = true,
            Err(RouterSendError::UnknownRoutingId(_)) => panic!("Y is connected"),
        }
        // dealer2 is never read, so its queue fills.
        while let Some(message) = dealer1.try_recv() {
            assert_eq!(message, Message::from("Hello"));
            dealer1_received += 1;
        }
    }
    assert!(dealer2_blocked, "dealer2 never filled up");
    assert!(
        dealer2_sent < dealer1_sent,
        "{dealer2_sent} to Y, {dealer1_sent} to X"
    );
    assert!(dealer1_received > 0);
}

// `test_get_peer_state_corner_cases` passes a null socket, a non-ROUTER
// socket and an unknown routing id to the C function. The first two cannot
// be written in zmq-rs; the third is `basic`'s unknown routing id.
