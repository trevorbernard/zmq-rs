//! libzmq's `tests/test_spec_dealer.cpp`: DEALER against RFC 28.

use std::time::Duration;

use zmq::{Context, Dealer, Message, Rep};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, msg, settle};

fn inproc(name: &str) -> String {
    format!("inproc://spec_dealer_{name}")
}

/// SHALL route outgoing messages to available peers using a round-robin
/// strategy.
fn round_robin_out(bind_address: &str) {
    let context = Context::new().unwrap();
    let mut dealer = context.socket::<Dealer>();
    let connect_address = dealer.bind(bind_address).unwrap().to_string();
    let mut reps: Vec<_> = (0..5)
        .map(|_| {
            let mut rep = context.socket::<Rep>();
            rep.connect(&connect_address).unwrap();
            rep
        })
        .collect();
    // Wait for connections.
    settle();

    // Send all requests
    for _ in &reps {
        dealer.send(msg(&["", "ABC"]));
    }

    // Expect every REP got one message
    for rep in &mut reps {
        let request = rep.recv_timeout(FIVE).expect("one request each");
        assert_eq!(request.body(), &msg(&["ABC"]));
    }
}

#[test]
fn round_robin_out_inproc() {
    round_robin_out(&inproc("round_robin_out"));
}

#[test]
fn round_robin_out_tcp() {
    round_robin_out(&free_endpoint());
}

/// SHALL receive incoming messages from its peers using a fair-queuing
/// strategy.
fn fair_queue_in(bind_address: &str) {
    let context = Context::new().unwrap();
    let mut receiver = context.socket::<Dealer>();
    let connect_address = receiver.bind(bind_address).unwrap().to_string();
    let mut senders: Vec<_> = (0..5)
        .map(|_| {
            let mut sender = context.socket::<Dealer>();
            sender.connect(&connect_address).unwrap();
            sender
        })
        .collect();

    for _ in 0..2 {
        senders[0].send("A");
        assert_eq!(receiver.recv_timeout(FIVE), Some(msg(&["A"])));
    }

    // send our requests
    for sender in &mut senders {
        sender.send("B");
    }
    // Wait for data.
    settle();

    // handle the requests
    for _ in &senders {
        assert_eq!(receiver.recv_timeout(FIVE), Some(msg(&["B"])));
    }
}

#[test]
fn fair_queue_in_inproc() {
    fair_queue_in(&inproc("fair_queue_in"));
}

#[test]
fn fair_queue_in_tcp() {
    fair_queue_in(&free_endpoint());
}

/// SHALL create a double queue when a peer connects to it. If this peer
/// disconnects, the DEALER socket SHALL destroy its double queue and SHALL
/// discard any messages it contains.
///
/// libzmq leaves this case disabled, "until libzmq does this properly": it
/// still delivers what the peer sent before leaving. zmq-rs does the same
/// (`docs/rfc-compliance.md`, "Socket patterns"), and checks the rest: the
/// queue towards the peer is gone.
fn destroy_queue_on_disconnect(bind_address: &str) {
    let context = Context::new().unwrap();
    let mut a = context.socket::<Dealer>();
    let connect_address = a.bind(bind_address).unwrap().to_string();
    let mut b = context.socket::<Dealer>();
    b.connect(&connect_address).unwrap();

    // Send a message in both directions
    a.send("ABC");
    b.send("DEF");
    b.disconnect(&connect_address).unwrap();
    // Disconnect may take time
    settle();

    // Sending to B fails. What B sent before leaving is still delivered,
    // as libzmq delivers it (docs/rfc-compliance.md, "Socket patterns").
    assert!(a.try_send(Message::new("")).is_err());
    assert_eq!(a.try_recv(), Some(Message::from("DEF")));
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
    destroy_queue_on_disconnect(&free_endpoint());
}

/// SHALL block on sending, or return a suitable error, when it has no
/// connected peers. libzmq's `ZMQ_SNDTIMEO` is `send_timeout` here.
#[test]
fn block_on_send_no_peers() {
    let context = Context::new().unwrap();
    let mut sc = context.socket::<Dealer>();
    assert!(sc.try_send(Message::new("")).is_err());
    assert!(
        sc.send_timeout(Message::new(""), Duration::from_millis(250))
            .is_err()
    );
}
