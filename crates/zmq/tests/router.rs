//! ROUTER's sending, handover and probing, as libzmq's
//! `ZMQ_ROUTER_MANDATORY`, `ZMQ_ROUTER_HANDOVER` and `ZMQ_PROBE_ROUTER`.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::task::{Context as TaskContext, Poll, Waker};
use std::thread;
use std::time::Duration;

use zmq::{
    Context, Dealer, Frame, Hwm, Identity, Message, Router, RouterSendError, RoutingId,
    SocketOptions, UnknownRoutingId,
};

const FIVE: Duration = Duration::from_secs(5);

fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
        .recv_timeout(limit)
        .unwrap_or_else(|_| panic!("timed out after {limit:?}"))
}

fn one() -> Hwm {
    Hwm::Messages(NonZeroUsize::MIN)
}

fn named(id: &str) -> SocketOptions {
    SocketOptions {
        identity: Identity::new(id.to_owned()).unwrap(),
        ..SocketOptions::default()
    }
}

fn id(text: &str) -> RoutingId {
    RoutingId::new(text.to_owned()).unwrap()
}

/// A ROUTER bound to `name`, and a DEALER called `d` connected to it that
/// queues at most one message from it.
fn router_and_slow_dealer(
    context: &Context,
    name: &str,
) -> (zmq::Socket<Router>, zmq::Socket<Dealer>) {
    let mut router = context.socket_with::<Router>(SocketOptions {
        send_hwm: one(),
        ..SocketOptions::default()
    });
    router.bind(&format!("inproc://{name}")).unwrap();
    let mut dealer = context.socket_with::<Dealer>(SocketOptions {
        recv_hwm: one(),
        ..named("d")
    });
    dealer.connect(&format!("inproc://{name}")).unwrap();
    (router, dealer)
}

#[test]
fn try_send_to_an_unknown_routing_id_hands_the_message_back() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    assert_eq!(
        router.try_send(&id("nobody"), "lost"),
        Err(RouterSendError::UnknownRoutingId(Message::from("lost")))
    );
}

#[test]
fn send_to_an_unknown_routing_id_fails_at_once() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    let result = within(FIVE, move || router.send(&id("nobody"), "lost"));
    assert_eq!(result, Err(UnknownRoutingId(Message::from("lost"))));
}

#[test]
fn try_send_to_a_full_peer_hands_the_message_back() {
    let context = Context::new().unwrap();
    let (mut router, _dealer) = router_and_slow_dealer(&context, "full");
    // The inproc pipe holds the ROUTER's send HWM plus the DEALER's receive
    // HWM.
    assert_eq!(router.try_send(&id("d"), "1"), Ok(()));
    assert_eq!(router.try_send(&id("d"), "2"), Ok(()));
    assert_eq!(
        router.try_send(&id("d"), "3"),
        Err(RouterSendError::Full(Message::from("3")))
    );
}

#[test]
fn send_waits_while_the_peer_is_full() {
    let context = Context::new().unwrap();
    let (mut router, mut dealer) = router_and_slow_dealer(&context, "wait");
    router.try_send(&id("d"), "1").unwrap();
    router.try_send(&id("d"), "2").unwrap();
    let (started, wait) = mpsc::channel();
    let sender = thread::spawn(move || {
        started.send(()).unwrap();
        router.send(&id("d"), "3")
    });
    wait.recv().unwrap();
    thread::sleep(Duration::from_millis(50));
    assert!(!sender.is_finished(), "the third message waits for room");
    let received = within(FIVE, move || [dealer.recv(), dealer.recv(), dealer.recv()]);
    assert_eq!(received, ["1", "2", "3"].map(Message::from));
    assert_eq!(sender.join().unwrap(), Ok(()));
}

#[test]
fn send_async_reaches_a_known_peer() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.bind("inproc://async").unwrap();
    let mut dealer = context.socket_with::<Dealer>(named("d"));
    dealer.connect("inproc://async").unwrap();
    let sent = std::pin::pin!(router.send_async(&id("d"), "hi"))
        .poll(&mut TaskContext::from_waker(Waker::noop()));
    assert_eq!(sent, Poll::Ready(Ok(())));
    assert_eq!(within(FIVE, move || dealer.recv()), Message::from("hi"));
}

#[test]
fn a_duplicate_identity_is_ignored_without_handover() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.bind("inproc://dup").unwrap();
    let mut first = context.socket_with::<Dealer>(named("same"));
    first.connect("inproc://dup").unwrap();
    first.send("first");
    let (from, _, mut router) = within(FIVE, move || {
        let (from, message) = router.recv();
        (from, message, router)
    });
    assert_eq!(from, id("same"));
    let mut second = context.socket_with::<Dealer>(named("same"));
    second.connect("inproc://dup").unwrap();
    second.send("second");
    thread::sleep(Duration::from_millis(50));
    assert_eq!(router.try_recv(), None, "the newcomer is ignored");
    router.send(&id("same"), "to the first").unwrap();
    assert_eq!(
        within(FIVE, move || first.recv()),
        Message::from("to the first")
    );
}

#[test]
fn with_handover_a_duplicate_identity_takes_over() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.set_handover(true);
    router.bind("inproc://handover").unwrap();
    let mut first = context.socket_with::<Dealer>(named("same"));
    first.connect("inproc://handover").unwrap();
    first.send("first");
    let mut router = within(FIVE, move || {
        assert_eq!(router.recv(), (id("same"), Message::from("first")));
        router
    });
    let mut second = context.socket_with::<Dealer>(named("same"));
    second.connect("inproc://handover").unwrap();
    second.send("second");
    let mut router = within(FIVE, move || {
        assert_eq!(router.recv(), (id("same"), Message::from("second")));
        router
    });
    router.send(&id("same"), "to the second").unwrap();
    assert_eq!(
        within(FIVE, move || second.recv()),
        Message::from("to the second")
    );
    assert_eq!(first.try_recv(), None, "the first lost its routing id");
    assert!(
        first.try_send("still there?").is_err(),
        "and was disconnected"
    );
}

#[test]
fn a_probing_router_greets_each_new_peer_with_an_empty_message() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.set_probe(true);
    router.bind("inproc://probe").unwrap();
    let mut dealer = context.socket::<Dealer>();
    dealer.connect("inproc://probe").unwrap();
    // The ROUTER sees the peer once it next looks at its peers.
    let _ = router.try_recv();
    assert_eq!(
        within(FIVE, move || dealer.recv()),
        Message::new(Frame::new())
    );
}

#[test]
fn a_probing_dealer_greets_its_router_so_the_router_learns_its_id() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.bind("inproc://dealer-probe").unwrap();
    let mut dealer = context.socket_with::<Dealer>(named("prober"));
    dealer.set_probe(true);
    dealer.connect("inproc://dealer-probe").unwrap();
    assert_eq!(
        within(FIVE, move || router.recv()),
        (id("prober"), Message::new(Frame::new()))
    );
}

#[test]
fn a_router_probes_the_peers_it_connects_to() {
    let context = Context::new().unwrap();
    let mut listener = context.socket_with::<Router>(named("listener"));
    listener.bind("inproc://router-to-router").unwrap();
    let mut router = context.socket::<Router>();
    router.set_probe(true);
    router.connect("inproc://router-to-router").unwrap();
    let (from, message) = within(FIVE, move || listener.recv());
    assert_eq!(from.as_bytes()[0], 0, "a generated id");
    assert_eq!(message, Message::new(Frame::new()));
}
