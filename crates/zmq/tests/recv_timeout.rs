//! Receiving with a timeout, as libzmq's `ZMQ_RCVTIMEO`: every socket type
//! that receives gives up after the timeout, returns at once when something
//! is waiting, and returns early when something arrives meanwhile.

use std::thread;
use std::time::{Duration, Instant};

use zmq::{
    Context, Dealer, EventKind, Frame, ManualXPub, Message, Pair, Pub, Pull, Push,
    RecvTimeoutError, Rep, Req, RequestError, Router, Sub, XPub, XSub,
};

const SHORT: Duration = Duration::from_millis(100);
/// How late a timed-out receive may return on a busy machine.
const SLACK: Duration = Duration::from_secs(2);
const FIVE: Duration = Duration::from_secs(5);

/// Checks `received` (one `recv_timeout` call, true if it got something)
/// on `receiver`, with `deliver` making one item arrive each time it runs.
fn check<R>(
    mut receiver: R,
    mut deliver: impl FnMut() + Send + 'static,
    received: fn(&mut R, Duration) -> bool,
) {
    let start = Instant::now();
    assert!(!received(&mut receiver, SHORT), "nothing was sent");
    let waited = start.elapsed();
    assert!(waited >= SHORT, "gave up after {waited:?}");
    assert!(waited < SHORT + SLACK, "gave up only after {waited:?}");

    let start = Instant::now();
    assert!(!received(&mut receiver, Duration::ZERO));
    assert!(start.elapsed() < SLACK, "a zero timeout waited");

    deliver();
    assert!(
        received(&mut receiver, Duration::ZERO),
        "a zero timeout still takes what is waiting"
    );
    deliver();
    let start = Instant::now();
    assert!(received(&mut receiver, FIVE));
    assert!(
        start.elapsed() < SLACK,
        "something waiting is returned at once"
    );
    deliver();
    assert!(
        received(&mut receiver, Duration::MAX),
        "an endless timeout waits forever, without overflowing"
    );

    let sender = thread::spawn(move || {
        thread::sleep(SHORT);
        deliver();
    });
    let start = Instant::now();
    assert!(
        received(&mut receiver, FIVE + FIVE),
        "woken by what arrived"
    );
    assert!(
        start.elapsed() < FIVE,
        "returned early, after {:?}",
        start.elapsed()
    );
    sender.join().unwrap();
}

#[test]
fn pair() {
    let context = Context::new().unwrap();
    let mut left = context.socket::<Pair>();
    left.bind("inproc://pair").unwrap();
    let mut right = context.socket::<Pair>();
    right.connect("inproc://pair").unwrap();
    check(
        left,
        move || right.send("x"),
        |s, t| s.recv_timeout(t).is_some(),
    );
}

#[test]
fn pull() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.bind("inproc://pull").unwrap();
    let mut push = context.socket::<Push>();
    push.connect("inproc://pull").unwrap();
    check(
        pull,
        move || push.send("x"),
        |s, t| s.recv_timeout(t).is_some(),
    );
}

#[test]
fn sub() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://sub").unwrap();
    let mut sub = context.socket::<Sub>();
    sub.subscribe("");
    sub.connect("inproc://sub").unwrap();
    check(
        sub,
        move || assert_eq!(publisher.send("x"), 1),
        |s, t| s.recv_timeout(t).is_some(),
    );
}

#[test]
fn xsub() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.bind("inproc://xsub").unwrap();
    let mut xsub = context.socket::<XSub>();
    xsub.subscribe("");
    xsub.connect("inproc://xsub").unwrap();
    check(
        xsub,
        move || assert_eq!(publisher.send("x"), 1),
        |s, t| s.recv_timeout(t).is_some(),
    );
}

#[test]
fn xpub() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.bind("inproc://xpub").unwrap();
    let mut sub = context.socket::<Sub>();
    sub.connect("inproc://xpub").unwrap();
    let mut topics = 0;
    check(
        xpub,
        move || {
            topics += 1;
            sub.subscribe(format!("topic {topics}"));
        },
        |s, t| s.recv_timeout(t).is_some(),
    );
}

#[test]
fn manual_xpub() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<ManualXPub>();
    xpub.bind("inproc://manual").unwrap();
    let mut sub = context.socket::<Sub>();
    sub.connect("inproc://manual").unwrap();
    let mut topics = 0;
    check(
        xpub,
        move || {
            topics += 1;
            sub.subscribe(format!("topic {topics}"));
        },
        |s, t| s.recv_timeout(t).is_some(),
    );
}

#[test]
fn dealer() {
    let context = Context::new().unwrap();
    let mut dealer = context.socket::<Dealer>();
    dealer.bind("inproc://dealer").unwrap();
    let mut other = context.socket::<Dealer>();
    other.connect("inproc://dealer").unwrap();
    check(
        dealer,
        move || other.send("x"),
        |s, t| s.recv_timeout(t).is_some(),
    );
}

#[test]
fn router() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.bind("inproc://router").unwrap();
    let mut dealer = context.socket::<Dealer>();
    dealer.connect("inproc://router").unwrap();
    check(
        router,
        move || dealer.send("x"),
        |s, t| s.recv_timeout(t).is_some(),
    );
}

#[test]
fn rep() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    rep.bind("inproc://rep").unwrap();
    let mut dealer = context.socket::<Dealer>();
    dealer.connect("inproc://rep").unwrap();
    let request = || Message::try_from(vec![Frame::new(), Frame::from("x")]).unwrap();
    check(
        rep,
        move || dealer.send(request()),
        |s, t| s.recv_timeout(t).is_some(),
    );
}

#[test]
fn a_request_with_no_peer_times_out() {
    let context = Context::new().unwrap();
    let mut req = context.socket::<Req>();
    let start = Instant::now();
    assert_eq!(
        req.request_timeout("anyone?", SHORT),
        Err(RequestError::TimedOut)
    );
    assert!(start.elapsed() >= SHORT);
}

#[test]
fn a_timed_out_request_leaves_the_socket_usable_and_its_reply_unseen() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    rep.bind("inproc://slow").unwrap();
    let server = thread::spawn(move || {
        let first = rep.recv();
        thread::sleep(SHORT * 3);
        first.reply("late");
        let second = rep.recv();
        let body = String::from_utf8_lossy(second.body().get(0).unwrap()).into_owned();
        second.reply(format!("for {body}"));
    });
    let mut req = context.socket::<Req>();
    req.connect("inproc://slow").unwrap();
    assert_eq!(
        req.request_timeout("first", SHORT),
        Err(RequestError::TimedOut)
    );
    assert_eq!(
        req.request_timeout("second", FIVE),
        Ok(Message::from("for second"))
    );
    server.join().unwrap();
}

#[test]
fn a_peer_closing_before_replying_still_fails_the_request() {
    let context = Context::new().unwrap();
    let mut rep = context.socket::<Rep>();
    rep.bind("inproc://closing").unwrap();
    let server = thread::spawn(move || {
        let request = rep.recv();
        drop(request);
        drop(rep);
    });
    let mut req = context.socket::<Req>();
    req.connect("inproc://closing").unwrap();
    assert_eq!(
        req.request_timeout("anyone?", FIVE),
        Err(RequestError::PeerGone)
    );
    server.join().unwrap();
}

#[test]
fn a_monitor_times_out_then_sees_events_then_the_end() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let mut monitor = pull.monitor();
    let start = Instant::now();
    assert_eq!(
        monitor.recv_timeout(SHORT).map(|e| e.kind),
        Err(RecvTimeoutError::Timeout)
    );
    assert!(start.elapsed() >= SHORT);
    pull.bind("tcp://127.0.0.1:*").unwrap();
    assert_eq!(
        monitor.recv_timeout(FIVE).map(|e| e.kind),
        Ok(EventKind::Listening)
    );
    drop(pull);
    let end = loop {
        match monitor.recv_timeout(FIVE) {
            Ok(_) => {}
            other => break other,
        }
    };
    assert_eq!(end.map(|e| e.kind), Err(RecvTimeoutError::Closed));
}

#[cfg(feature = "draft")]
mod draft {
    use super::*;
    use zmq::{Channel, Client, Dish, Gather, Group, Peer, Radio, Scatter, Server};

    #[test]
    fn server() {
        let context = Context::new().unwrap();
        let mut server = context.socket::<Server>();
        server.bind("inproc://server").unwrap();
        let mut client = context.socket::<Client>();
        client.connect("inproc://server").unwrap();
        check(
            server,
            move || client.send("x"),
            |s, t| s.recv_timeout(t).is_some(),
        );
    }

    #[test]
    fn client() {
        let context = Context::new().unwrap();
        let mut server = context.socket::<Server>();
        server.bind("inproc://client").unwrap();
        let mut client = context.socket::<Client>();
        client.connect("inproc://client").unwrap();
        client.send("hello");
        let (id, _) = server.recv_timeout(FIVE).expect("the client said hello");
        check(
            client,
            move || server.send(id, "x").unwrap(),
            |s, t| s.recv_timeout(t).is_some(),
        );
    }

    #[test]
    fn gather() {
        let context = Context::new().unwrap();
        let mut gather = context.socket::<Gather>();
        gather.bind("inproc://gather").unwrap();
        let mut scatter = context.socket::<Scatter>();
        scatter.connect("inproc://gather").unwrap();
        check(
            gather,
            move || scatter.send("x"),
            |s, t| s.recv_timeout(t).is_some(),
        );
    }

    #[test]
    fn channel() {
        let context = Context::new().unwrap();
        let mut left = context.socket::<Channel>();
        left.bind("inproc://channel").unwrap();
        let mut right = context.socket::<Channel>();
        right.connect("inproc://channel").unwrap();
        check(
            left,
            move || right.send("x"),
            |s, t| s.recv_timeout(t).is_some(),
        );
    }

    #[test]
    fn peer() {
        let context = Context::new().unwrap();
        let mut peer = context.socket::<Peer>();
        peer.bind("inproc://peer").unwrap();
        let mut other = context.socket::<Peer>();
        let id = other.connect_peer("inproc://peer").unwrap();
        check(
            peer,
            move || other.send(id, "x").unwrap(),
            |s, t| s.recv_timeout(t).is_some(),
        );
    }

    #[test]
    fn dish() {
        let context = Context::new().unwrap();
        let mut radio = context.socket::<Radio>();
        radio.bind("inproc://dish").unwrap();
        let mut dish = context.socket::<Dish>();
        dish.connect("inproc://dish").unwrap();
        let news = Group::new("news").unwrap();
        assert!(dish.join(&news));
        check(
            dish,
            move || assert_eq!(radio.send(&news, "x"), 1),
            |s, t| s.recv_timeout(t).is_some(),
        );
    }
}
