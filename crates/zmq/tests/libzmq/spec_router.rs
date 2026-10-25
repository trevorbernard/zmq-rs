//! libzmq's `tests/test_spec_router.cpp`: ROUTER against RFC 28.

use std::collections::HashMap;

use zmq::{Context, Dealer, Identity, Message, Router, RouterSendError, RoutingId, SocketOptions};

use crate::reqrep_inproc::msg;
use crate::support::{FIVE, settle};

fn inproc(name: &str) -> String {
    format!("inproc://spec_router_{name}")
}

const TCP: &str = "tcp://127.0.0.1:*";

fn named(id: &str) -> SocketOptions {
    SocketOptions {
        identity: Identity::new(id.to_owned()).unwrap(),
        ..SocketOptions::default()
    }
}

/// SHALL receive incoming messages from its peers using a fair-queuing
/// strategy.
fn fair_queue_in(bind_address: &str) {
    let context = Context::new().unwrap();
    let mut receiver = context.socket::<Router>();
    let connect_address = receiver.bind(bind_address).unwrap().to_string();
    let names = ["A", "B", "C", "D", "E"];
    let mut senders: Vec<_> = names
        .iter()
        .map(|name| {
            let mut sender = context.socket_with::<Dealer>(named(name));
            sender.connect(&connect_address).unwrap();
            sender
        })
        .collect();
    settle();

    let a = RoutingId::new("A").unwrap();
    for _ in 0..2 {
        senders[0].send("M");
        assert_eq!(receiver.recv_timeout(FIVE), Some((a.clone(), msg(&["M"]))));
    }

    // send N requests, and expect one from each sender
    for sender in &mut senders {
        sender.send("M");
    }
    let mut pending: HashMap<RoutingId, usize> = names
        .iter()
        .map(|name| (RoutingId::new(*name).unwrap(), 1))
        .collect();
    for _ in names {
        let (from, message) = receiver.recv_timeout(FIVE).expect("every request");
        assert_eq!(message, msg(&["M"]));
        let left = pending.get_mut(&from).expect("a known sender");
        assert_eq!(*left, 1, "{from:?} sent twice");
        *left = 0;
    }
}

#[test]
fn fair_queue_in_inproc() {
    fair_queue_in(&inproc("fair_queue_in"));
}

#[test]
fn fair_queue_in_tcp() {
    fair_queue_in(TCP);
}

/// SHALL create a double queue when a peer connects to it. If this peer
/// disconnects, the ROUTER socket SHALL destroy its double queue and SHALL
/// discard any messages it contains.
///
/// libzmq leaves this case disabled, "until libzmq implements this
/// properly": it still delivers what the peer sent before leaving. zmq-rs
/// does the same (`docs/rfc-compliance.md`, "Socket patterns"), and checks
/// the rest: the queue towards the peer is gone.
fn destroy_queue_on_disconnect(bind_address: &str) {
    let context = Context::new().unwrap();
    // libzmq sets ZMQ_ROUTER_MANDATORY; a zmq-rs ROUTER always refuses to
    // send to an unknown peer.
    let mut a = context.socket::<Router>();
    let connect_address = a.bind(bind_address).unwrap().to_string();
    let mut b = context.socket_with::<Dealer>(named("B"));
    b.connect(&connect_address).unwrap();
    settle();

    // Send a message in both directions
    let to_b = RoutingId::new("B").unwrap();
    a.send(&to_b, "ABC").unwrap();
    b.send("DEF");
    b.disconnect(&connect_address).unwrap();
    // Disconnect may take time
    settle();

    // Sending to B fails. What B sent before leaving is still delivered,
    // as libzmq delivers it (docs/rfc-compliance.md, "Socket patterns").
    assert!(matches!(
        a.try_send(&to_b, ""),
        Err(RouterSendError::UnknownRoutingId(_))
    ));
    assert_eq!(
        a.try_recv(),
        Some((RoutingId::new("B").unwrap(), Message::from("DEF")))
    );
    assert_eq!(a.try_recv(), None);

    // After a reconnect of B, the messages should still be gone
    b.connect(&connect_address).unwrap();
    settle();
    assert_eq!(a.try_recv(), None);
    assert_eq!(b.try_recv(), None);
}

#[test]
fn destroy_queue_on_disconnect_inproc() {
    destroy_queue_on_disconnect(&inproc("destroy_queue"));
}

#[test]
fn destroy_queue_on_disconnect_tcp() {
    destroy_queue_on_disconnect(TCP);
}
