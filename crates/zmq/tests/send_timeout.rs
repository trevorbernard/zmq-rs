//! Sending with a timeout, as libzmq's `ZMQ_SNDTIMEO`: a send that would
//! block gives up after the timeout and hands the message back, sends at
//! once when there is room, and returns early once room appears.

use std::num::NonZeroUsize;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{
    Context, Dealer, Hwm, Identity, Message, Pair, Pull, Push, Router, RoutingId, SendError,
    SocketOptions, Sub, XPub,
};

const SHORT: Duration = Duration::from_millis(100);
/// How late a timed-out send may return on a busy machine.
const SLACK: Duration = Duration::from_secs(2);
const FIVE: Duration = Duration::from_secs(5);

/// Queues of one message each way, so a peer that does not read soon
/// leaves no room.
fn tiny() -> SocketOptions {
    let one = Hwm::Messages(NonZeroUsize::new(1).unwrap());
    SocketOptions {
        send_hwm: one,
        recv_hwm: one,
        ..SocketOptions::default()
    }
}

/// Checks `send_timeout` (one call, true if it sent) on `sender`. `fill`
/// sends without waiting until there is no room; `drain` reads everything
/// the peer has received.
fn check<S>(
    mut sender: S,
    try_send: fn(&mut S) -> bool,
    send_timeout: fn(&mut S, Duration) -> bool,
    mut drain: impl FnMut() + Send + 'static,
) {
    let start = Instant::now();
    assert!(send_timeout(&mut sender, FIVE), "there is room");
    assert!(start.elapsed() < SLACK, "sent at once");
    let mut sent = 0;
    while try_send(&mut sender) {
        sent += 1;
        assert!(sent < 10_000, "the queues never filled");
    }

    let start = Instant::now();
    assert!(!send_timeout(&mut sender, SHORT), "there is no room");
    let waited = start.elapsed();
    assert!(waited >= SHORT, "gave up after {waited:?}");
    assert!(waited < SHORT + SLACK, "gave up only after {waited:?}");

    let start = Instant::now();
    assert!(!send_timeout(&mut sender, Duration::ZERO));
    assert!(start.elapsed() < SLACK, "a zero timeout waited");

    let reader = thread::spawn(move || {
        thread::sleep(SHORT);
        drain();
        drain
    });
    let start = Instant::now();
    assert!(
        send_timeout(&mut sender, FIVE + FIVE),
        "woken by the room made"
    );
    assert!(
        start.elapsed() < FIVE,
        "returned early, after {:?}",
        start.elapsed()
    );
    let mut drain = reader.join().unwrap();
    drain();
    assert!(
        send_timeout(&mut sender, Duration::MAX),
        "an endless timeout waits forever, without overflowing"
    );
}

#[test]
fn push() {
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(tiny());
    pull.bind("inproc://push").unwrap();
    let mut push = context.socket_with::<Push>(tiny());
    push.connect("inproc://push").unwrap();
    check(
        push,
        |s| s.try_send("x").is_ok(),
        |s, t| s.send_timeout("x", t).is_ok(),
        move || while pull.try_recv().is_some() {},
    );
}

#[test]
fn pair() {
    let context = Context::new().unwrap();
    let mut left = context.socket_with::<Pair>(tiny());
    left.bind("inproc://pair").unwrap();
    let mut right = context.socket_with::<Pair>(tiny());
    right.connect("inproc://pair").unwrap();
    check(
        left,
        |s| s.try_send("x").is_ok(),
        |s, t| s.send_timeout("x", t).is_ok(),
        move || while right.try_recv().is_some() {},
    );
}

#[test]
fn dealer() {
    let context = Context::new().unwrap();
    let mut dealer = context.socket_with::<Dealer>(tiny());
    dealer.bind("inproc://dealer").unwrap();
    let mut other = context.socket_with::<Dealer>(tiny());
    other.connect("inproc://dealer").unwrap();
    check(
        dealer,
        |s| s.try_send("x").is_ok(),
        |s, t| s.send_timeout("x", t).is_ok(),
        move || while other.try_recv().is_some() {},
    );
}

fn dealer_id() -> RoutingId {
    RoutingId::new("dealer").unwrap()
}

#[test]
fn router() {
    let context = Context::new().unwrap();
    let mut router = context.socket_with::<Router>(tiny());
    router.bind("inproc://router").unwrap();
    let mut dealer = context.socket_with::<Dealer>(SocketOptions {
        identity: Identity::new("dealer").unwrap(),
        ..tiny()
    });
    dealer.connect("inproc://router").unwrap();
    dealer.send("hello");
    assert_eq!(
        router.recv_timeout(FIVE).map(|(id, _)| id),
        Some(dealer_id())
    );
    check(
        router,
        |s| s.try_send(&dealer_id(), "x").is_ok(),
        |s, t| s.send_timeout(&dealer_id(), "x", t).is_ok(),
        move || while dealer.try_recv().is_some() {},
    );
}

#[test]
fn router_reports_an_unknown_peer_at_once() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    let start = Instant::now();
    assert_eq!(
        router.send_timeout(&RoutingId::new("nobody").unwrap(), "x", FIVE),
        Err(SendError::UnknownPeer(Message::from("x")))
    );
    assert!(start.elapsed() < SLACK);
}

#[test]
fn router_reports_a_full_peer_after_the_timeout() {
    let context = Context::new().unwrap();
    let mut router = context.socket_with::<Router>(tiny());
    router.bind("inproc://full").unwrap();
    let mut dealer = context.socket_with::<Dealer>(SocketOptions {
        identity: Identity::new("dealer").unwrap(),
        ..tiny()
    });
    dealer.connect("inproc://full").unwrap();
    dealer.send("hello");
    assert!(router.recv_timeout(FIVE).is_some());
    while router.try_send(&dealer_id(), "fill").is_ok() {}
    assert_eq!(
        router.send_timeout(&dealer_id(), "x", SHORT),
        Err(SendError::Full(Message::from("x")))
    );
}

#[test]
fn a_timed_out_send_hands_the_message_back() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    assert_eq!(push.send_timeout("kept", SHORT), Err(Message::from("kept")));
}

#[test]
fn xpub_send_all() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket_with::<XPub>(tiny());
    xpub.bind("inproc://nodrop").unwrap();
    let mut sub = context.socket_with::<Sub>(tiny());
    sub.subscribe("");
    sub.connect("inproc://nodrop").unwrap();
    assert!(
        xpub.recv_timeout(FIVE).is_some(),
        "the subscription arrived"
    );
    check(
        xpub,
        |s| s.try_send_all("x").is_ok(),
        |s, t| s.send_all_timeout("x", t).is_ok(),
        move || while sub.try_recv().is_some() {},
    );
}

#[cfg(feature = "draft")]
mod draft {
    use super::*;
    use zmq::{Channel, Client, Frame, Gather, Peer, PeerId, Scatter, SendError, Server};

    #[test]
    fn client() {
        let context = Context::new().unwrap();
        let mut server = context.socket_with::<Server>(tiny());
        server.bind("inproc://client").unwrap();
        let mut client = context.socket_with::<Client>(tiny());
        client.connect("inproc://client").unwrap();
        check(
            client,
            |s| s.try_send("x").is_ok(),
            |s, t| s.send_timeout("x", t).is_ok(),
            move || while server.try_recv().is_some() {},
        );
    }

    #[test]
    fn scatter() {
        let context = Context::new().unwrap();
        let mut gather = context.socket_with::<Gather>(tiny());
        gather.bind("inproc://scatter").unwrap();
        let mut scatter = context.socket_with::<Scatter>(tiny());
        scatter.connect("inproc://scatter").unwrap();
        check(
            scatter,
            |s| s.try_send("x").is_ok(),
            |s, t| s.send_timeout("x", t).is_ok(),
            move || while gather.try_recv().is_some() {},
        );
    }

    #[test]
    fn channel() {
        let context = Context::new().unwrap();
        let mut left = context.socket_with::<Channel>(tiny());
        left.bind("inproc://channel").unwrap();
        let mut right = context.socket_with::<Channel>(tiny());
        right.connect("inproc://channel").unwrap();
        check(
            left,
            |s| s.try_send("x").is_ok(),
            |s, t| s.send_timeout("x", t).is_ok(),
            move || while right.try_recv().is_some() {},
        );
    }

    #[test]
    fn server() {
        let context = Context::new().unwrap();
        let mut server = context.socket_with::<Server>(tiny());
        server.bind("inproc://server").unwrap();
        let mut client = context.socket_with::<Client>(tiny());
        client.connect("inproc://server").unwrap();
        client.send("hello");
        let (id, _) = server.recv_timeout(FIVE).expect("the client said hello");
        check(
            (server, id),
            |(s, id)| s.try_send(*id, "x").is_ok(),
            |(s, id), t| s.send_timeout(*id, "x", t).is_ok(),
            move || while client.try_recv().is_some() {},
        );
    }

    #[test]
    fn peer() {
        let context = Context::new().unwrap();
        let mut peer = context.socket_with::<Peer>(tiny());
        peer.bind("inproc://peer").unwrap();
        let mut other = context.socket_with::<Peer>(tiny());
        let id = other.connect_peer("inproc://peer").unwrap();
        check(
            (other, id),
            |(s, id)| s.try_send(*id, "x").is_ok(),
            |(s, id), t| s.send_timeout(*id, "x", t).is_ok(),
            move || while peer.try_recv().is_some() {},
        );
    }

    #[test]
    fn peer_reports_a_full_peer_after_the_timeout() {
        let context = Context::new().unwrap();
        let mut peer = context.socket_with::<Peer>(tiny());
        peer.bind("inproc://full").unwrap();
        let mut other = context.socket_with::<Peer>(tiny());
        let id = other.connect_peer("inproc://full").unwrap();
        while other.try_send(id, "fill").is_ok() {}
        assert_eq!(
            other.send_timeout(id, "x", SHORT),
            Err(SendError::Full(Frame::from("x")))
        );
    }

    #[test]
    fn peer_reports_an_unknown_peer_at_once() {
        let context = Context::new().unwrap();
        let mut peer = context.socket::<Peer>();
        let nobody = PeerId::new(12345).unwrap();
        let start = Instant::now();
        assert_eq!(
            peer.send_timeout(nobody, "x", FIVE),
            Err(SendError::UnknownPeer(Frame::from("x")))
        );
        assert!(start.elapsed() < SLACK);
    }
}
