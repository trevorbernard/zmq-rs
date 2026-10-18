//! REQ and REP (RFC 28) through the typed `zmq` API, against libzmq.

use std::sync::mpsc;
use std::task::{Context as TaskContext, Waker};
use std::thread;
use std::time::Duration;

use zmq::{
    Context, Frame, Message, PeerGone, Reconnect, Rep, Req, RequestError, RouteOutcome,
    SocketOptions,
};
use zmq_interop::libzmq::{Kind, Socket};

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

/// Runs `f` on another thread, returning a receiver for its result.
fn spawn<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> mpsc::Receiver<T> {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
}

fn message(parts: &[&[u8]]) -> Message {
    let frames = parts
        .iter()
        .map(|p| Frame::copy_from_slice(p))
        .collect::<Vec<_>>();
    Message::try_from(frames).unwrap()
}

const FIVE: Duration = Duration::from_secs(5);

#[test]
fn our_req_asks_a_libzmq_rep() {
    let rep = Socket::new(Kind::Rep);
    let addr = rep.bind_loopback();
    let context = Context::new().unwrap();
    let mut req = context.socket::<Req>();
    req.connect(&format!("tcp://{addr}")).unwrap();
    let answer = spawn(move || req.request("question"));
    assert_eq!(rep.recv().unwrap(), [b"question".to_vec()]);
    rep.send(&[b"answer"]).unwrap();
    assert_eq!(
        answer.recv_timeout(FIVE).unwrap(),
        Ok(Message::from("answer"))
    );
}

#[test]
fn our_rep_answers_a_libzmq_req() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    let addr = rep.bind("tcp://127.0.0.1:*").unwrap();
    let req = Socket::new(Kind::Req);
    req.connect(addr);
    req.send(&[b"question"]).unwrap();
    let outcome = within(FIVE, move || {
        let request = rep.recv();
        assert_eq!(*request.body(), Message::from("question"));
        request.reply("answer")
    });
    assert_eq!(outcome, RouteOutcome::Sent);
    assert_eq!(req.recv().unwrap(), [b"answer".to_vec()]);
}

#[test]
fn our_req_and_rep_talk_to_each_other() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    let addr = rep.bind("tcp://127.0.0.1:*").unwrap();
    let server = spawn(move || {
        for _ in 0..3 {
            let request = rep.recv();
            let text = String::from_utf8(request.body().get(0).unwrap().to_vec()).unwrap();
            request.reply(text.to_uppercase());
        }
    });
    let mut req = context.socket::<Req>();
    req.connect(&addr.to_string()).unwrap();
    let answers = within(FIVE, move || {
        ["a", "b", "c"].map(|q| req.request(q).unwrap())
    });
    assert_eq!(answers, ["A", "B", "C"].map(Message::from));
    server.recv_timeout(FIVE).unwrap();
}

#[test]
fn a_router_sees_the_req_envelope() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let options = SocketOptions {
        identity: zmq::Identity::new("asker").unwrap(),
        ..SocketOptions::default()
    };
    let mut req = context.socket_with::<Req>(options);
    req.connect(&format!("tcp://{addr}")).unwrap();
    let answer = spawn(move || req.request("q"));
    assert_eq!(router.recv().unwrap(), [&b"asker"[..], b"", b"q"]);
    router.send(&[b"asker", b"", b"a"]).unwrap();
    assert_eq!(answer.recv_timeout(FIVE).unwrap(), Ok(Message::from("a")));
}

#[test]
fn rep_returns_the_whole_envelope_for_multi_hop_requests() {
    // A libzmq DEALER stands in for a broker that has already prepended
    // its client's address.
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    let addr = rep.bind("tcp://127.0.0.1:*").unwrap();
    let dealer = Socket::new(Kind::Dealer);
    dealer.connect(addr);
    dealer.send(&[b"client-7", b"", b"q1", b"q2"]).unwrap();
    within(FIVE, move || {
        let request = rep.recv();
        assert_eq!(*request.body(), message(&[b"q1", b"q2"]));
        request.reply(message(&[b"a1", b"a2"]));
    });
    assert_eq!(
        dealer.recv().unwrap(),
        [&b"client-7"[..], b"", b"a1", b"a2"]
    );
}

#[test]
fn req_round_robins_across_reps() {
    let reps: Vec<Socket> = (0..2).map(|_| Socket::new(Kind::Rep)).collect();
    let context = Context::new().unwrap();
    let mut req = context.socket::<Req>();
    for rep in &reps {
        req.connect(&format!("tcp://{}", rep.bind_loopback()))
            .unwrap();
    }
    let mut req = Some(req);
    for (i, rep) in reps.iter().enumerate() {
        let mut asker = req.take().unwrap();
        let answer = spawn(move || (asker.request(format!("q{i}")), asker));
        assert_eq!(
            rep.recv().unwrap(),
            [format!("q{i}").into_bytes()],
            "REP {i} is asked"
        );
        rep.send(&[format!("a{i}").as_bytes()]).unwrap();
        let (result, asker) = answer.recv_timeout(FIVE).unwrap();
        assert_eq!(result, Ok(Message::from(format!("a{i}"))));
        req = Some(asker);
    }
}

#[test]
fn a_peer_closing_before_replying_fails_the_request() {
    let rep = Socket::new(Kind::Rep);
    let addr = rep.bind_loopback();
    let context = Context::new().unwrap();
    let options = SocketOptions {
        reconnect: Reconnect::Never,
        ..SocketOptions::default()
    };
    let mut req = context.socket_with::<Req>(options);
    req.connect(&format!("tcp://{addr}")).unwrap();
    let answer = spawn(move || req.request("anyone?"));
    rep.recv().unwrap();
    drop(rep);
    assert_eq!(answer.recv_timeout(FIVE).unwrap(), Err(PeerGone));
}

#[test]
fn correlated_requests_interoperate_with_libzmq_rep() {
    let rep = Socket::new(Kind::Rep);
    let addr = rep.bind_loopback();
    let context = Context::new().unwrap();
    let mut req = context.socket::<Req>();
    req.set_correlation(true);
    req.connect(&format!("tcp://{addr}")).unwrap();
    let answer = spawn(move || req.request("q"));
    assert_eq!(rep.recv().unwrap(), [b"q".to_vec()]);
    rep.send(&[b"a"]).unwrap();
    assert_eq!(answer.recv_timeout(FIVE).unwrap(), Ok(Message::from("a")));
}

/// Sends a request and abandons it, as dropping a pending future does.
fn abandon_request(req: &mut zmq::Socket<Req>, body: &'static str) {
    let mut future = std::pin::pin!(req.request_async(body));
    let mut cx = TaskContext::from_waker(Waker::noop());
    assert!(future.as_mut().poll(&mut cx).is_pending(), "no reply yet");
}

/// A REP that answers `stale` to the first request and `fresh` to the second.
fn answer_stale_then_fresh(context: &Context) -> (String, mpsc::Receiver<()>) {
    let mut rep = context.socket::<Rep>();
    let addr = rep.bind("tcp://127.0.0.1:*").unwrap();
    let done = spawn(move || {
        rep.recv().reply("stale");
        rep.recv().reply("fresh");
    });
    (addr.to_string(), done)
}

#[test]
fn correlation_drops_the_reply_to_an_abandoned_request() {
    let context = Context::new().unwrap();
    let (endpoint, done) = answer_stale_then_fresh(&context);
    let mut req = context.socket::<Req>();
    req.set_correlation(true);
    req.connect(&endpoint).unwrap();
    abandon_request(&mut req, "first");
    let answer = within(FIVE, move || req.request("second"));
    assert_eq!(answer, Ok(Message::from("fresh")));
    done.recv_timeout(FIVE).unwrap();
}

#[test]
fn without_correlation_an_abandoned_reply_can_be_taken() {
    // The documented hazard of cancelling without correlation, as with
    // libzmq's ZMQ_REQ_RELAXED alone.
    let context = Context::new().unwrap();
    let (endpoint, done) = answer_stale_then_fresh(&context);
    let mut req = context.socket::<Req>();
    req.connect(&endpoint).unwrap();
    abandon_request(&mut req, "first");
    let answer = within(FIVE, move || req.request("second"));
    assert_eq!(answer, Ok(Message::from("stale")));
    done.recv_timeout(FIVE).unwrap();
}

#[test]
fn try_recv_does_not_block() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    assert!(rep.try_recv().is_none());
}

#[test]
fn a_timed_out_request_to_a_libzmq_rep_leaves_the_socket_usable() {
    let rep = Socket::new(Kind::Rep);
    let addr = rep.bind_loopback();
    let context = Context::new().unwrap();
    let mut req = context.socket::<Req>();
    req.connect(&format!("tcp://{addr}")).unwrap();
    let first = req.request_timeout("first", Duration::from_millis(100));
    assert_eq!(first, Err(RequestError::TimedOut));
    assert_eq!(rep.recv().unwrap(), [b"first".to_vec()]);
    rep.send(&[b"late"]).unwrap();
    let answer = spawn(move || req.request_timeout("second", FIVE));
    assert_eq!(rep.recv().unwrap(), [b"second".to_vec()]);
    rep.send(&[b"fresh"]).unwrap();
    assert_eq!(
        answer.recv_timeout(FIVE).unwrap(),
        Ok(Message::from("fresh"))
    );
}
