//! A second connect to an endpoint, compared with libzmq: a DEALER, SUB,
//! PUB or REQ ignores it, as libzmq's `is_single_connect` has them do.
//! Each case runs with libzmq connecting and with ours, and expects the
//! same of both.

use std::collections::HashSet;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Dealer, Pub, Push, Req, Router, SocketType, Sub, XPub, XSub};
use zmq_interop::libzmq::{self, Kind, Socket};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);

/// A listener that speaks no ZMTP, to count the connections made to it,
/// and its endpoint.
fn listen() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("tcp://{}", listener.local_addr().unwrap());
    (listener, endpoint)
}

/// Accepts the next connection to `listener`, failing after five seconds.
fn accept(listener: &TcpListener) -> TcpStream {
    let started = Instant::now();
    loop {
        if let Ok((stream, _)) = listener.accept() {
            return stream;
        }
        assert!(started.elapsed() < FIVE, "no connection was made");
        thread::sleep(Duration::from_millis(5));
    }
}

/// How many connections `connect` makes to an endpoint it is given twice.
/// It is then given another, and a connection to that one says the ones
/// asked for before it were made.
fn connections(mut connect: impl FnMut(&str)) -> usize {
    let (target, endpoint) = listen();
    let (after, later) = listen();
    connect(&endpoint);
    connect(&endpoint);
    connect(&later);
    let _held = accept(&after);
    thread::sleep(Duration::from_millis(100));
    let held: Vec<_> = std::iter::from_fn(|| target.accept().ok()).collect();
    held.len()
}

fn libzmq_connections(kind: Kind) -> usize {
    let socket = Socket::new(kind);
    connections(|endpoint| socket.connect_str(endpoint))
}

fn our_connections<T: SocketType>() -> usize {
    let context = Context::new().unwrap();
    let mut socket = context.socket::<T>().unwrap();
    connections(|endpoint| socket.connect(endpoint).unwrap())
}

#[test]
fn a_dealer_sub_pub_or_req_connects_to_an_endpoint_once() {
    for kind in [Kind::Dealer, Kind::Sub, Kind::Pub, Kind::Req] {
        assert_eq!(libzmq_connections(kind), 1, "libzmq's {kind:?}");
    }
    assert_eq!(our_connections::<Dealer>(), 1, "our DEALER");
    assert_eq!(our_connections::<Sub>(), 1, "our SUB");
    assert_eq!(our_connections::<Pub>(), 1, "our PUB");
    assert_eq!(our_connections::<Req>(), 1, "our REQ");
}

#[test]
fn the_other_types_connect_to_an_endpoint_each_time() {
    for kind in [Kind::Push, Kind::XSub, Kind::XPub, Kind::Router] {
        assert_eq!(libzmq_connections(kind), 2, "libzmq's {kind:?}");
    }
    assert_eq!(our_connections::<Push>(), 2, "our PUSH");
    assert_eq!(our_connections::<XSub>(), 2, "our XSUB");
    assert_eq!(our_connections::<XPub>(), 2, "our XPUB");
    assert_eq!(our_connections::<Router>(), 2, "our ROUTER");
}

/// The endpoint is compared as written: the same address reached from a
/// named source is another endpoint.
#[test]
fn an_endpoint_written_another_way_is_connected_to_again() {
    fn two_ways(mut connect: impl FnMut(&str)) {
        let (target, endpoint) = listen();
        let sourced = endpoint.replace("tcp://", "tcp://127.0.0.1:0;");
        connect(&endpoint);
        connect(&sourced);
        let _held = [accept(&target), accept(&target)];
    }

    let dealer = Socket::new(Kind::Dealer);
    two_ways(|endpoint| dealer.connect_str(endpoint));

    let context = Context::new().unwrap();
    let mut dealer = context.socket::<Dealer>().unwrap();
    two_ways(|endpoint| dealer.connect(endpoint).unwrap());
}

#[test]
fn a_disconnected_endpoint_is_connected_to_again() {
    fn again(mut connect: impl FnMut(&str), mut disconnect: impl FnMut(&str)) {
        let (target, endpoint) = listen();
        connect(&endpoint);
        let _first = accept(&target);
        disconnect(&endpoint);
        connect(&endpoint);
        let _second = accept(&target);
    }

    let dealer = Socket::new(Kind::Dealer);
    again(
        |endpoint| dealer.connect_str(endpoint),
        |endpoint| dealer.disconnect(endpoint),
    );

    let context = Context::new().unwrap();
    let dealer = std::cell::RefCell::new(context.socket::<Dealer>().unwrap());
    again(
        |endpoint| dealer.borrow_mut().connect(endpoint).unwrap(),
        |endpoint| dealer.borrow_mut().disconnect(endpoint).unwrap(),
    );
}

/// libzmq connects over `inproc` before it looks for the endpoint, so a
/// DEALER connected twice has two pipes, which a ROUTER tells apart.
#[test]
fn an_inproc_endpoint_is_connected_to_each_time() {
    let context = libzmq::Context::new();
    let router = Socket::in_context(&context, Kind::Router);
    router.bind("inproc://twice");
    let dealer = Socket::in_context(&context, Kind::Dealer);
    dealer.connect_str("inproc://twice");
    dealer.connect_str("inproc://twice");
    dealer.send(&[b"one"]).unwrap();
    dealer.send(&[b"two"]).unwrap();
    let from: HashSet<_> = (0..2).map(|_| router.recv().unwrap().remove(0)).collect();
    assert_eq!(from.len(), 2, "libzmq");

    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>().unwrap();
    router.bind("inproc://twice").unwrap();
    let mut dealer = context.socket::<Dealer>().unwrap();
    dealer.connect("inproc://twice").unwrap();
    dealer.connect("inproc://twice").unwrap();
    dealer.send("one").unwrap();
    dealer.send("two").unwrap();
    let from: HashSet<_> = (0..2)
        .map(|_| router.recv_timeout(FIVE).unwrap().unwrap().0)
        .collect();
    assert_eq!(from.len(), 2, "ours");
}

/// libzmq keeps a socket's listeners with what it connected to, so a
/// connect to an endpoint the socket is bound to is ignored too. A DEALER
/// connected to itself would send itself two of every three messages.
#[test]
fn a_dealer_does_not_connect_to_where_it_is_bound() {
    let endpoint = free_endpoint();
    let dealer = Socket::new(Kind::Dealer);
    dealer.bind(&endpoint);
    dealer.connect_str(&endpoint);
    let peer = Socket::new(Kind::Dealer);
    peer.connect_str(&endpoint);
    peer.send(&[b"here"]).unwrap();
    assert_eq!(dealer.recv().unwrap(), [b"here"]);
    for _ in 0..3 {
        dealer.send(&[b"out"]).unwrap();
    }
    for _ in 0..3 {
        assert_eq!(peer.recv().unwrap(), [b"out"], "libzmq");
    }

    let endpoint = free_endpoint();
    let context = Context::new().unwrap();
    let mut dealer = context.socket::<Dealer>().unwrap();
    dealer.bind(&endpoint).unwrap();
    dealer.connect(&endpoint).unwrap();
    let mut peer = context.socket::<Dealer>().unwrap();
    peer.connect(&endpoint).unwrap();
    peer.send("here").unwrap();
    assert_eq!(dealer.recv_timeout(FIVE).unwrap().unwrap(), "here");
    for _ in 0..3 {
        dealer.send("out").unwrap();
    }
    for _ in 0..3 {
        assert_eq!(peer.recv_timeout(FIVE).unwrap().unwrap(), "out", "ours");
    }
}
