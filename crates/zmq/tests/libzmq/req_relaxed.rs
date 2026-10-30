//! libzmq's `tests/test_req_relaxed.cpp`: `ZMQ_REQ_RELAXED` with
//! `ZMQ_REQ_CORRELATE`, a REQ that sends a new request without waiting for
//! the last one's reply, and drops that reply when it comes.
//!
//! zmq-rs's `request` sends and waits in one call, so "sending again before
//! the reply" is a `request_timeout` that gives up, which leaves the socket
//! usable and turns correlation on (README, "Differences from libzmq").
//! libzmq's checks of `ZMQ_EVENTS` have no counterpart: a REQ is not polled.

use std::thread::{self, JoinHandle};

use zmq::{Context, Frame, Message, Rep, Req, RequestError, Router, Socket};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, SETTLE, settle, within};

const SERVICES: usize = 5;

fn message(frames: &[&'static str]) -> Message {
    Message::try_from(
        frames
            .iter()
            .map(|&frame| Frame::from(frame))
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

/// Runs `ask` with `req` on another thread, handing the socket back with
/// the result from `finish`.
fn in_background<T: Send + 'static>(
    mut req: Socket<Req>,
    ask: impl FnOnce(&mut Socket<Req>) -> T + Send + 'static,
) -> JoinHandle<(Socket<Req>, T)> {
    thread::spawn(move || {
        let result = ask(&mut req);
        (req, result)
    })
}

fn finish<T: Send + 'static>(asking: JoinHandle<(Socket<Req>, T)>) -> (Socket<Req>, T) {
    within(FIVE, move || asking.join().unwrap())
}

/// A relaxed, correlating REQ bound to TCP, and five REPs connected to it
/// in turn, each connection settling before the next so that requests go
/// to them in order.
fn set_up(context: &Context) -> (Socket<Req>, [Socket<Rep>; SERVICES]) {
    let mut req = context.socket::<Req>();
    req.set_correlation(true);
    let endpoint = req.bind(&free_endpoint()).unwrap().to_string();
    let rep = [(); SERVICES].map(|()| {
        let mut rep = context.socket::<Rep>();
        rep.connect(&endpoint).unwrap();
        settle();
        rep
    });
    (req, rep)
}

/// Case 1: a second request before a reply arrives.
fn case_1(req: Socket<Req>, rep: &mut [Socket<Rep>; SERVICES]) -> Socket<Req> {
    let [rep0, rep1, rep2, ..] = rep;

    // Send a request, ensure it arrives, don't send a reply.
    let asking = in_background(req, |req| req.request_timeout(message(&["A", "B"]), SETTLE));
    let unanswered = rep0.recv_timeout(FIVE).expect("A B");
    assert_eq!(unanswered.body(), &message(&["A", "B"]));
    let (req, abandoned) = finish(asking);
    assert!(matches!(abandoned, Err(RequestError::TimedOut)));

    // Send another request.
    let asking = in_background(req, |req| req.request(message(&["C", "D"])));
    let expected = rep1.recv_timeout(FIVE).expect("C D");
    assert_eq!(expected.body(), &message(&["C", "D"]));

    // The reply to the first request is discarded; the expected one comes.
    unanswered.reply("WRONG");
    expected.reply("OK");
    let (req, reply) = finish(asking);
    assert_eq!(reply.unwrap(), Message::from("OK"));

    // Another standard request-reply cycle, just to check.
    let asking = in_background(req, |req| req.request("E"));
    let request = rep2.recv_timeout(FIVE).expect("E");
    assert_eq!(request.body(), &Message::from("E"));
    request.reply(message(&["F", "G"]));
    let (req, reply) = finish(asking);
    assert_eq!(reply.unwrap(), message(&["F", "G"]));
    req
}

/// Case 2: a second request when a reply is already in the REQ's pipe.
fn case_2(req: Socket<Req>, rep: &mut [Socket<Rep>; SERVICES]) -> Socket<Req> {
    let req = case_1(req, rep);
    let [_, _, _, rep3, rep4] = rep;

    // Send a request, ensure it arrives, and reply once the REQ has
    // stopped waiting, so the reply sits in its pipe.
    let asking = in_background(req, |req| req.request_timeout("H", SETTLE));
    let late = rep3.recv_timeout(FIVE).expect("H");
    assert_eq!(late.body(), &Message::from("H"));
    let (req, abandoned) = finish(asking);
    assert!(matches!(abandoned, Err(RequestError::TimedOut)));
    late.reply("BAD");
    settle();

    // Without receiving that reply, send another request.
    let asking = in_background(req, |req| req.request("I"));
    let request = rep4.recv_timeout(FIVE).expect("I");
    assert_eq!(request.body(), &Message::from("I"));
    request.reply("GOOD");
    let (req, reply) = finish(asking);
    assert_eq!(reply.unwrap(), Message::from("GOOD"));
    req
}

#[test]
fn case_1_second_request_before_a_reply() {
    let context = Context::new().unwrap();
    let (req, mut rep) = set_up(&context);
    case_1(req, &mut rep);
}

#[test]
fn case_2_second_request_with_a_reply_waiting() {
    let context = Context::new().unwrap();
    let (req, mut rep) = set_up(&context);
    case_2(req, &mut rep);
}

/// Case 3, libzmq's issue #1690: two requests in a row must not close the
/// pipe to the peer of the first, so rep0 is next to receive.
#[test]
fn case_3_abandoned_peers_stay_in_turn() {
    let context = Context::new().unwrap();
    let (req, mut rep) = set_up(&context);
    let req = case_2(req, &mut rep);
    let asking = in_background(req, |req| req.request("J"));
    let request = rep[0].recv_timeout(FIVE).expect("J at rep0");
    assert_eq!(request.body(), &Message::from("J"));
    request.reply("K");
    finish(asking).1.unwrap();
}

/// Case 4, libzmq's issue #1695: requests that pile up before a peer is
/// there are answered in turn, and only the last one's reply is taken.
#[test]
fn case_4_replies_to_earlier_requests_are_discarded() {
    let endpoint = free_endpoint();
    let context = Context::new().unwrap();
    let mut req = context.socket::<Req>();
    req.set_correlation(true);
    req.connect(&endpoint).unwrap();
    // The router is not bound yet.
    let mut router = context.socket::<Router>();

    // Two requests: the first is given up on, the second waited for.
    let asking = in_background(req, |req| {
        let abandoned = req.request_timeout("TO_BE_DISCARDED", SETTLE);
        assert!(matches!(abandoned, Err(RequestError::TimedOut)));
        req.request("TO_BE_ANSWERED")
    });

    // Bind the router, and send both requests back as they are.
    thread::sleep(2 * SETTLE);
    router.bind(&endpoint).unwrap();
    for _ in 0..2 {
        let (from, request) = router.recv_timeout(FIVE).expect("a request");
        router.send(&from, request).unwrap();
    }

    // Correlation takes the reply to the second request.
    let (_, reply) = finish(asking);
    assert_eq!(reply.unwrap(), Message::from("TO_BE_ANSWERED"));
}
