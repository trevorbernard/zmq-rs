//! libzmq's `tests/test_spec_rep.cpp`: REP against RFC 28.

use std::collections::HashSet;

use zmq::{Context, Dealer, Rep, Req};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, Requesting, msg, settle};

fn inproc(name: &str) -> String {
    format!("inproc://spec_rep_{name}")
}

/// SHALL receive incoming messages from its peers using a fair-queuing
/// strategy.
fn fair_queue_in(bind_address: &str) {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    let connect_address = rep.bind(bind_address).unwrap().to_string();
    let mut reqs: Vec<_> = (0..5)
        .map(|_| {
            let mut req = context.socket::<Req>();
            req.connect(&connect_address).unwrap();
            Some(req)
        })
        .collect();
    settle();

    for _ in 0..2 {
        let requesting = Requesting::start(reqs[0].take().unwrap(), msg(&["A"]));
        let request = rep.recv_timeout(FIVE).expect("the request");
        assert_eq!(request.body(), &msg(&["A"]));
        request.reply("A");
        let (req, reply) = requesting.finish();
        assert_eq!(reply, msg(&["A"]));
        reqs[0] = Some(req);
    }

    // libzmq keeps the rest behind `#ifdef SOMEONE_FIXES_THIS`, as it
    // expects the requests in the order they were sent and fails randomly.
    // Each is checked to arrive once, whatever the order, and its reply
    // to reach the requester that sent it.
    let names = ["A", "B", "C", "D", "E"];
    let requesting: Vec<_> = reqs
        .into_iter()
        .zip(names)
        .map(|(req, name)| Requesting::start(req.unwrap(), msg(&[name])))
        .collect();
    let mut seen = HashSet::new();
    for _ in names {
        let request = rep.recv_timeout(FIVE).expect("every request");
        let body = request.body().clone();
        assert!(seen.insert(body.clone()), "{body:?} arrived twice");
        request.reply(body);
    }
    for (requesting, name) in requesting.into_iter().zip(names) {
        let (_req, reply) = requesting.finish();
        assert_eq!(reply, msg(&[name]));
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

/// For an incoming message: SHALL remove and store the address envelope,
/// including the delimiter; SHALL pass the remaining data frames to its
/// calling application; SHALL wait for a single reply message from its
/// calling application; SHALL prepend the address envelope and delimiter;
/// SHALL deliver this message back to the originating peer.
fn envelope(bind_address: &str) {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    let connect_address = rep.bind(bind_address).unwrap().to_string();
    let mut dealer = context.socket::<Dealer>();
    dealer.connect(&connect_address).unwrap();

    // minimal envelope
    dealer.send(msg(&["", "A"]));
    let request = rep.recv_timeout(FIVE).expect("the request");
    assert_eq!(request.body(), &msg(&["A"]));
    request.reply("A");
    assert_eq!(dealer.recv_timeout(FIVE), Some(msg(&["", "A"])));

    // big envelope
    dealer.send(msg(&["X", "Y", "", "A"]));
    let request = rep.recv_timeout(FIVE).expect("the request");
    assert_eq!(request.body(), &msg(&["A"]));
    request.reply("A");
    assert_eq!(dealer.recv_timeout(FIVE), Some(msg(&["X", "Y", "", "A"])));
}

#[test]
fn envelope_inproc() {
    envelope(&inproc("envelope"));
}

#[test]
fn envelope_tcp() {
    envelope(&free_endpoint());
}
