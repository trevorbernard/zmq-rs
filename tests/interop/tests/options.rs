//! Socket options, compared with libzmq: what each library puts on the
//! wire for a hand-written peer to read, or what it does with the other
//! library as its peer. Each case runs with libzmq holding the option and
//! with ours holding it, and expects the same of both.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{
    Context, Dealer, Message, Pub, Pull, Push, Req, Router, RoutingId, SocketOptions, SocksProxy,
    Sub, Verbosity, XPub, XPubEvent,
};
use zmq_interop::libzmq::{Kind, Socket, option};
use zmq_interop::{Peer, Security};
use zmq_test_ports::free_port;
use zmtp::SocketType;

const FIVE: Duration = Duration::from_secs(5);
const QUIET: Duration = Duration::from_millis(200);

/// Calls `attempt` until it says it is done, failing after five seconds.
/// An attempt calls both sockets, as either library takes up a new
/// connection only when its socket is next called.
fn until(what: &str, mut attempt: impl FnMut() -> bool) {
    let started = Instant::now();
    while !attempt() {
        assert!(started.elapsed() < FIVE, "{what}");
        thread::sleep(Duration::from_millis(5));
    }
}

fn listen() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

/// A hand-written peer of type `socket_type` for whoever connects next.
fn accept(listener: &TcpListener, socket_type: SocketType) -> Peer {
    Peer::accept(
        listener,
        socket_type,
        zmtp::Identity::default(),
        Security::Null,
    )
    .unwrap()
}

fn id(text: &str) -> RoutingId {
    RoutingId::new(text.to_owned()).unwrap()
}

fn identity(id: &str) -> SocketOptions {
    SocketOptions::new().identity(zmq::Identity::new(id.to_owned()).unwrap())
}

fn frames(message: &Message) -> Vec<Vec<u8>> {
    message.iter().map(|frame| frame.to_vec()).collect()
}

/// A REQ that correlates sends an id of four octets before the delimiter,
/// and one that is relaxed may ask again before it is answered; it then
/// takes the answer to its last request alone.
#[test]
fn a_relaxed_req_that_correlates_takes_the_answer_to_its_last_request() {
    /// Reads two requests from a REQ, checks their shape, and answers the
    /// first, which is stale, and then the second.
    fn serve(peer: &mut Peer) {
        let requests = [peer.recv_message().unwrap(), peer.recv_message().unwrap()];
        for (request, body) in requests.iter().zip([&b"first"[..], b"second"]) {
            let request = frames(request);
            assert_eq!(request.len(), 3, "{request:?}");
            assert_eq!(request[0].len(), 4, "{request:?}");
            assert_eq!(request[1..], [&b""[..], body], "{request:?}");
        }
        assert_ne!(requests[0].iter().next(), requests[1].iter().next());
        for (request, answer) in requests.iter().zip(["stale", "fresh"]) {
            let id = request.iter().next().unwrap().clone();
            let reply = vec![id, zmtp::Frame::new(), zmtp::Frame::from(answer)];
            peer.send(&Message::try_from(reply).unwrap()).unwrap();
        }
    }

    let (listener, addr) = listen();
    let req = Socket::new(Kind::Req);
    req.set(option::REQ_CORRELATE, 1);
    req.set(option::REQ_RELAXED, 1);
    req.connect(addr);
    let mut peer = accept(&listener, SocketType::Router);
    req.send(&[b"first"]).unwrap();
    req.send(&[b"second"]).unwrap();
    serve(&mut peer);
    assert_eq!(req.recv().unwrap(), [b"fresh"]);

    let (listener, addr) = listen();
    let context = Context::new().unwrap();
    let mut req = context.socket::<Req>().unwrap();
    req.connect(format!("tcp://{addr}")).unwrap();
    let mut peer = accept(&listener, SocketType::Router);
    assert!(req.request_timeout("first", QUIET).is_err());
    let (done, answer) = mpsc::channel();
    thread::spawn(move || done.send(req.request_timeout("second", FIVE)));
    serve(&mut peer);
    assert_eq!(
        answer.recv_timeout(FIVE).unwrap(),
        Ok(Message::from("fresh"))
    );
}

/// What a client sends a SOCKS5 proxy: its greeting, and its request to
/// connect. The proxy says it has connected, and its end of the stream is
/// returned, to be kept until the client is done with it.
fn asked_of_a_proxy(connect: impl FnOnce(String)) -> (Vec<u8>, Vec<u8>, TcpStream) {
    let (listener, addr) = listen();
    connect(addr.to_string());
    let (mut client, _) = listener.accept().unwrap();
    client.set_read_timeout(Some(FIVE)).unwrap();
    let mut greeting = vec![0; 2];
    client.read_exact(&mut greeting).unwrap();
    let methods = usize::from(greeting[1]);
    greeting.resize(2 + methods, 0);
    client.read_exact(&mut greeting[2..]).unwrap();
    // No authentication.
    client.write_all(&[5, 0]).unwrap();
    let mut request = vec![0; 5];
    client.read_exact(&mut request).unwrap();
    let rest = match request[3] {
        1 => 3 + 2,
        4 => 15 + 2,
        _ => usize::from(request[4]) + 2,
    };
    request.resize(5 + rest, 0);
    client.read_exact(&mut request[5..]).unwrap();
    // Succeeded, and bound to no address in particular.
    client.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]).unwrap();
    (greeting, request, client)
}

#[test]
fn a_socks_proxy_is_asked_the_same_by_both() {
    for destination in ["127.0.0.1:5999", "somedomainname.org:5999", "[::1]:5999"] {
        let theirs = Socket::new(Kind::Push);
        let from_libzmq = asked_of_a_proxy(|proxy| {
            theirs.set_bytes_option(option::SOCKS_PROXY, proxy.as_bytes());
            theirs.set_ipv6();
            theirs.connect_str(&format!("tcp://{destination}"));
        });

        let context = Context::new().unwrap();
        let mut ours = None;
        let from_us = asked_of_a_proxy(|proxy| {
            let options = SocketOptions::new().socks_proxy(SocksProxy::new(&proxy).unwrap());
            let push = ours.insert(context.socket_with::<Push>(options).unwrap());
            push.connect(format!("tcp://{destination}")).unwrap();
        });
        assert_eq!(
            (&from_us.0, &from_us.1),
            (&from_libzmq.0, &from_libzmq.1),
            "{destination}"
        );
        // The sockets go before the proxy's ends of their streams.
        drop((theirs, ours));
    }
}

/// A ROUTER that names the peer it connects to can send to it at once.
#[test]
fn a_router_sends_at_once_to_a_peer_it_named_when_connecting() {
    let context = Context::new().unwrap();

    let mut dealer = context.socket::<Dealer>().unwrap();
    let bound = dealer.bind("tcp://127.0.0.1:*").unwrap();
    let router = Socket::new(Kind::Router);
    router.set(option::ROUTER_MANDATORY, 1);
    router.set_bytes_option(option::CONNECT_ROUTING_ID, b"named");
    router.connect(bound);
    router.send(&[b"named", b"hello"]).unwrap();
    assert_eq!(
        dealer.recv_timeout(FIVE).unwrap(),
        Some(Message::from("hello"))
    );

    let dealer = Socket::new(Kind::Dealer);
    let addr = dealer.bind_loopback();
    let mut router = context.socket::<Router>().unwrap();
    router
        .connect_with_routing_id(format!("tcp://{addr}"), id("named"))
        .unwrap();
    router.send(&id("named"), "hello").unwrap();
    assert_eq!(dealer.recv().unwrap(), [b"hello"]);
}

/// What an XPUB reports when two subscribers subscribe to a topic, one
/// after the other, and then unsubscribe in the same order.
fn reported_by_libzmq(verbosity: Verbosity) -> Vec<String> {
    let context = Context::new().unwrap();
    let xpub = Socket::new(Kind::XPub);
    match verbosity {
        Verbosity::Changes => {}
        Verbosity::Subscriptions => xpub.set(option::XPUB_VERBOSE, 1),
        Verbosity::Everything => xpub.set(option::XPUB_VERBOSER, 1),
    }
    xpub.set_receive_timeout(Duration::from_millis(5));
    let addr = xpub.bind_loopback();
    let mut subscribers = [(); 2].map(|()| {
        let mut sub = context.socket::<Sub>().unwrap();
        sub.connect(format!("tcp://{addr}")).unwrap();
        sub
    });
    let mut reported = Vec::new();
    let mut settle = |subscribers: &mut [zmq::Socket<Sub>; 2]| {
        let started = Instant::now();
        while started.elapsed() < QUIET {
            for sub in subscribers.iter_mut() {
                let _ = sub.try_recv().unwrap();
            }
            if let Ok(heard) = xpub.recv() {
                let sign = if heard[0][0] == 1 { '+' } else { '-' };
                reported.push(format!("{sign}{}", heard[0][1..].escape_ascii()));
            }
        }
    };
    for step in 0..4 {
        match step {
            0 | 1 => subscribers[step].subscribe("A"),
            _ => assert!(subscribers[step - 2].unsubscribe("A")),
        }
        settle(&mut subscribers);
    }
    reported
}

/// As [`reported_by_libzmq`], for an XPUB of ours and subscribers of
/// libzmq's.
fn reported_by_us(verbosity: Verbosity) -> Vec<String> {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>().unwrap();
    xpub.set_verbosity(verbosity);
    let bound = xpub.bind("tcp://127.0.0.1:*").unwrap();
    let subscribers = [(); 2].map(|()| {
        let sub = Socket::new(Kind::Sub);
        sub.connect(bound.clone());
        sub
    });
    let mut reported = Vec::new();
    let mut settle = || {
        let started = Instant::now();
        while started.elapsed() < QUIET {
            for sub in &subscribers {
                sub.events();
            }
            match xpub.recv_timeout(Duration::from_millis(5)).unwrap() {
                Some(XPubEvent::Subscribe(topic)) => {
                    reported.push(format!("+{}", topic.escape_ascii()));
                }
                Some(XPubEvent::Cancel(topic)) => {
                    reported.push(format!("-{}", topic.escape_ascii()));
                }
                Some(XPubEvent::Message(message)) => panic!("{message:?}"),
                None => {}
            }
        }
    };
    for step in 0..4 {
        match step {
            0 | 1 => subscribers[step].subscribe(b"A"),
            _ => subscribers[step - 2].unsubscribe(b"A"),
        }
        settle();
    }
    reported
}

#[test]
fn an_xpub_reports_as_verbosely_as_it_is_asked_to() {
    for (verbosity, expected) in [
        (Verbosity::Changes, &["+A", "-A"][..]),
        (Verbosity::Subscriptions, &["+A", "+A", "-A"]),
        (Verbosity::Everything, &["+A", "+A", "-A", "-A"]),
    ] {
        assert_eq!(reported_by_libzmq(verbosity), expected, "{verbosity:?}");
        assert_eq!(reported_by_us(verbosity), expected, "{verbosity:?}");
    }
}

/// With matching inverted on both, a subscriber gets what its topics do
/// not match.
#[test]
fn inverted_matching_delivers_what_is_not_subscribed_to() {
    let context = Context::new().unwrap();

    let publisher = Socket::new(Kind::Pub);
    publisher.set(option::INVERT_MATCHING, 1);
    let addr = publisher.bind_loopback();
    let mut subscriber = context.socket::<Sub>().unwrap();
    subscriber.set_invert_matching(true);
    subscriber.subscribe("A");
    subscriber.connect(format!("tcp://{addr}")).unwrap();
    until("nothing came from libzmq's publisher", || {
        publisher.send(&[b"A1"]).unwrap();
        publisher.send(&[b"B1"]).unwrap();
        let taken = subscriber.try_recv().unwrap();
        taken.is_some() && {
            assert_eq!(taken, Some(Message::from("B1")));
            true
        }
    });

    let mut publisher = context.socket::<Pub>().unwrap();
    publisher.set_invert_matching(true);
    let bound = publisher.bind("tcp://127.0.0.1:*").unwrap();
    let subscriber = Socket::new(Kind::Sub);
    subscriber.set(option::INVERT_MATCHING, 1);
    subscriber.subscribe(b"A");
    subscriber.connect(bound);
    subscriber.set_receive_timeout(Duration::from_millis(5));
    until("nothing came from our publisher", || {
        publisher.send("A1").unwrap();
        publisher.send("B1").unwrap();
        subscriber.recv().is_ok_and(|taken| {
            assert_eq!(taken, [b"B1"]);
            true
        })
    });
}

/// A PULL that conflates keeps only the last message it was sent.
#[test]
fn a_conflating_pull_keeps_the_last_message() {
    let context = Context::new().unwrap();
    let numbers: Vec<String> = (1..=20).map(|n| n.to_string()).collect();

    let pull = Socket::new(Kind::Pull);
    pull.set(option::CONFLATE, 1);
    let addr = pull.bind_loopback();
    let mut push = context.socket::<Push>().unwrap();
    push.connect(format!("tcp://{addr}")).unwrap();
    // The connection is up before the rest are sent, or libzmq, which
    // takes it up when its socket is first called, would take them one
    // at a time.
    push.send("0").unwrap();
    assert_eq!(pull.recv().unwrap(), [b"0"]);
    for number in &numbers {
        push.send(number.clone()).unwrap();
    }
    thread::sleep(QUIET);
    assert_eq!(pull.recv().unwrap(), [b"20"]);
    pull.set_receive_timeout(QUIET);
    assert!(pull.recv().is_err());

    let mut pull = context.socket::<Pull>().unwrap();
    pull.set_conflate(true);
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let push = Socket::new(Kind::Push);
    push.connect(bound);
    push.send(&[b"0"]).unwrap();
    assert_eq!(pull.recv_timeout(FIVE).unwrap(), Some(Message::from("0")));
    for number in &numbers {
        push.send(&[number.as_bytes()]).unwrap();
    }
    thread::sleep(QUIET);
    assert_eq!(pull.recv_timeout(FIVE).unwrap(), Some(Message::from("20")));
    assert_eq!(pull.recv_timeout(QUIET).unwrap(), None);
}

/// With `immediate`, nothing is queued for a peer that is not there yet.
#[test]
fn an_immediate_push_queues_only_for_a_peer_that_is_there() {
    let context = Context::new().unwrap();

    let addr = free_port();
    let push = Socket::new(Kind::Push);
    push.set_immediate();
    push.connect(addr);
    assert!(push.send_now(&[b"too soon"]).is_err());
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind(format!("tcp://{addr}")).unwrap();
    until("libzmq's PUSH never found our PULL", || {
        let _ = pull.try_recv().unwrap();
        push.send_now(&[b"now"]).is_ok()
    });
    assert_eq!(pull.recv_timeout(FIVE).unwrap(), Some(Message::from("now")));

    let addr = free_port();
    let mut push = context
        .socket_with::<Push>(SocketOptions::new().immediate(true))
        .unwrap();
    push.connect(format!("tcp://{addr}")).unwrap();
    assert!(push.try_send("too soon").is_err());
    let pull = Socket::new(Kind::Pull);
    pull.bind(&format!("tcp://{addr}"));
    until("our PUSH never found libzmq's PULL", || {
        pull.events();
        push.try_send("now").is_ok()
    });
    assert_eq!(pull.recv().unwrap(), [b"now"]);
}

/// With handover, a ROUTER gives an identity to the peer that announces
/// it last.
#[test]
fn a_libzmq_router_with_handover_gives_an_identity_to_our_newcomer() {
    let router = Socket::new(Kind::Router);
    router.set_router_handover();
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let mut dealers = ["one", "two"].map(|body| {
        let mut dealer = context.socket_with::<Dealer>(identity("same")).unwrap();
        dealer.connect(format!("tcp://{addr}")).unwrap();
        dealer.send(body).unwrap();
        assert_eq!(router.recv().unwrap(), [b"same", body.as_bytes()]);
        dealer
    });
    router.send(&[b"same", b"for the newcomer"]).unwrap();
    assert_eq!(
        dealers[1].recv_timeout(FIVE).unwrap(),
        Some(Message::from("for the newcomer"))
    );
    assert_eq!(dealers[0].recv_timeout(QUIET).unwrap(), None);
}

/// Unbinding closes the connections its listener accepted, over `tcp`,
/// and frees its address.
#[test]
fn unbinding_closes_accepted_connections_and_frees_the_address() {
    let context = Context::new().unwrap();

    let pull = Socket::new(Kind::Pull);
    let addr = pull.bind_loopback();
    let mut push = context.socket::<Push>().unwrap();
    push.connect(format!("tcp://{addr}")).unwrap();
    push.send("before").unwrap();
    assert_eq!(pull.recv().unwrap(), [b"before"]);
    pull.unbind(&format!("tcp://{addr}"));
    push.send("after").unwrap();
    pull.set_receive_timeout(QUIET);
    assert!(pull.recv().is_err());
    until("libzmq's address was never freed", || {
        TcpListener::bind(addr).is_ok()
    });

    let mut pull = context.socket::<Pull>().unwrap();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let push = Socket::new(Kind::Push);
    push.connect(bound.clone());
    push.send(&[b"before"]).unwrap();
    assert_eq!(
        pull.recv_timeout(FIVE).unwrap(),
        Some(Message::from("before"))
    );
    pull.unbind(&bound).unwrap();
    push.send(&[b"after"]).unwrap();
    assert_eq!(pull.recv_timeout(QUIET).unwrap(), None);
    assert!(TcpListener::bind(bound.tcp_addr().unwrap()).is_ok());
}

/// After a disconnect, nothing more is sent to the endpoint: with no
/// other peer, a send has nowhere to go.
#[test]
fn disconnecting_leaves_a_push_without_its_peer() {
    let context = Context::new().unwrap();

    let mut pull = context.socket::<Pull>().unwrap();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let push = Socket::new(Kind::Push);
    push.connect_str(&bound);
    push.send(&[b"before"]).unwrap();
    assert_eq!(
        pull.recv_timeout(FIVE).unwrap(),
        Some(Message::from("before"))
    );
    push.disconnect(&bound);
    assert!(push.send_now(&[b"after"]).is_err());
    assert_eq!(pull.recv_timeout(QUIET).unwrap(), None);

    let pull = Socket::new(Kind::Pull);
    let endpoint = format!("tcp://{}", pull.bind_loopback());
    let mut push = context.socket::<Push>().unwrap();
    push.connect(&endpoint).unwrap();
    push.send("before").unwrap();
    assert_eq!(pull.recv().unwrap(), [b"before"]);
    push.disconnect(&endpoint).unwrap();
    assert!(push.try_send("after").is_err());
    pull.set_receive_timeout(QUIET);
    assert!(pull.recv().is_err());
}

#[cfg(feature = "draft")]
mod draft {
    use zmq::{RouterNotify, XSub};
    use zmq_interop::libzmq;
    use zmtp::{Command, Incoming, Subscription};

    use super::*;

    /// What an XSUB told its publisher, as a hand-written XPUB reads it,
    /// up to the message `end`: `+topic` and `-topic`.
    fn upstream(peer: &mut Peer) -> Vec<String> {
        let mut told = Vec::new();
        loop {
            let subscription = match peer.recv().unwrap() {
                Incoming::Command(body) => {
                    Subscription::from_command(Command::parse(&body).unwrap())
                }
                Incoming::Message(message) if message == "end" => return told,
                Incoming::Message(message) => Subscription::from_message(&message),
            };
            told.push(match subscription.expect("a subscription") {
                Subscription::Subscribe(topic) => format!("+{}", topic.escape_ascii()),
                Subscription::Cancel(topic) => format!("-{}", topic.escape_ascii()),
            });
        }
    }

    /// An XSUB tells its publisher of a topic's last unsubscription, and
    /// of every one only when asked to.
    #[test]
    fn an_xsub_sends_every_unsubscription_only_when_asked_to() {
        if !libzmq::has_drafts() {
            return;
        }
        for (verbose, expected) in [
            (false, &["+A", "+A", "-A"][..]),
            (true, &["+A", "+A", "-A", "-A", "-Z"]),
        ] {
            let (listener, addr) = listen();
            let xsub = Socket::new(Kind::XSub);
            xsub.set(option::XSUB_VERBOSE_UNSUBSCRIBE, i32::from(verbose));
            xsub.connect(addr);
            let mut peer = accept(&listener, SocketType::XPub);
            for sent in [
                &b"\x01A"[..],
                b"\x01A",
                b"\x00A",
                b"\x00A",
                b"\x00Z",
                b"end",
            ] {
                xsub.send(&[sent]).unwrap();
            }
            assert_eq!(upstream(&mut peer), expected, "libzmq, verbose: {verbose}");

            let (listener, addr) = listen();
            let context = Context::new().unwrap();
            let mut xsub = context.socket::<XSub>().unwrap();
            xsub.set_verbose_unsubscribe(verbose);
            xsub.connect(format!("tcp://{addr}")).unwrap();
            let mut peer = accept(&listener, SocketType::XPub);
            xsub.subscribe("A");
            xsub.subscribe("A");
            xsub.unsubscribe("A");
            xsub.unsubscribe("A");
            xsub.unsubscribe("Z");
            xsub.send("end").unwrap();
            assert_eq!(upstream(&mut peer), expected, "ours, verbose: {verbose}");
        }
    }

    /// A hello message is the first thing a peer is sent, whichever side
    /// connected.
    #[test]
    fn a_hello_message_is_sent_first() {
        if !libzmq::has_drafts() {
            return;
        }
        let context = Context::new().unwrap();
        let expected = [Message::from("hello"), Message::from("then this")];

        let (listener, addr) = listen();
        let dealer = Socket::new(Kind::Dealer);
        dealer.set_bytes_option(option::HELLO_MSG, b"hello");
        dealer.connect(addr);
        let mut peer = accept(&listener, SocketType::Router);
        dealer.send(&[b"then this"]).unwrap();
        let heard = [peer.recv_message().unwrap(), peer.recv_message().unwrap()];
        assert_eq!(heard, expected, "libzmq connecting");

        let (listener, addr) = listen();
        let mut dealer = context.socket::<Dealer>().unwrap();
        dealer.set_hello_message("hello");
        dealer.connect(format!("tcp://{addr}")).unwrap();
        let mut peer = accept(&listener, SocketType::Router);
        dealer.send("then this").unwrap();
        let heard = [peer.recv_message().unwrap(), peer.recv_message().unwrap()];
        assert_eq!(heard, expected, "ours connecting");

        let router = Socket::new(Kind::Router);
        router.set_bytes_option(option::HELLO_MSG, b"hello");
        // A send to a peer it has yet to take up fails, and is not lost.
        router.set(option::ROUTER_MANDATORY, 1);
        let addr = router.bind_loopback();
        let mut peer = Peer::connect(addr, SocketType::Dealer, b"peer").unwrap();
        until("libzmq's ROUTER never learnt its peer", || {
            router.send_now(&[b"peer", b"then this"]).is_ok()
        });
        let heard = [peer.recv_message().unwrap(), peer.recv_message().unwrap()];
        assert_eq!(heard, expected, "libzmq binding");

        let mut router = context.socket::<Router>().unwrap();
        router.set_hello_message("hello");
        let bound = router.bind("tcp://127.0.0.1:*").unwrap();
        let mut peer =
            Peer::connect(bound.tcp_addr().unwrap(), SocketType::Dealer, b"peer").unwrap();
        until("our ROUTER never learnt its peer", || {
            router.try_send(&id("peer"), "then this").is_ok()
        });
        let heard = [peer.recv_message().unwrap(), peer.recv_message().unwrap()];
        assert_eq!(heard, expected, "ours binding");
    }

    /// A ROUTER that asks to be notified hears an empty message from a
    /// peer when its handshake completes and when it leaves; one with a
    /// disconnect message hears that after what the peer sent.
    #[test]
    fn a_router_hears_of_peers_arriving_and_leaving() {
        if !libzmq::has_drafts() {
            return;
        }
        let context = Context::new().unwrap();

        let router = Socket::new(Kind::Router);
        router.set(option::ROUTER_NOTIFY, 3);
        router.set_bytes_option(option::DISCONNECT_MSG, b"gone");
        let addr = router.bind_loopback();
        let mut dealer = context.socket_with::<Dealer>(identity("peer")).unwrap();
        dealer.connect(format!("tcp://{addr}")).unwrap();
        assert_eq!(router.recv().unwrap(), [&b"peer"[..], b""], "arrival");
        dealer.send("said").unwrap();
        assert_eq!(router.recv().unwrap(), [&b"peer"[..], b"said"]);
        drop(dealer);
        let mut leaving = [router.recv().unwrap(), router.recv().unwrap()];
        leaving.sort();
        assert_eq!(
            leaving,
            [[&b"peer"[..], b""], [&b"peer"[..], b"gone"]],
            "libzmq"
        );

        let mut router = context.socket::<Router>().unwrap();
        router.set_notify(RouterNotify::default().connect(true).disconnect(true));
        router.set_disconnect_message("gone");
        let bound = router.bind("tcp://127.0.0.1:*").unwrap();
        let dealer = Socket::new(Kind::Dealer);
        dealer.set_routing_id(b"peer");
        dealer.connect(bound);
        let mut next = || router.recv_timeout(FIVE).unwrap().expect("a message");
        let empty = Message::from(&b""[..]);
        assert_eq!(next(), (id("peer"), empty.clone()), "arrival");
        dealer.send(&[b"said"]).unwrap();
        assert_eq!(next(), (id("peer"), Message::from("said")));
        drop(dealer);
        let mut leaving = [next().1, next().1];
        leaving.sort_by_key(frames);
        assert_eq!(leaving, [empty, Message::from("gone")], "ours");
    }

    /// A DEALER with a hiccup message hears it when a connection it made
    /// breaks.
    #[test]
    fn a_dealer_hears_a_hiccup_when_its_connection_breaks() {
        if !libzmq::has_drafts() {
            return;
        }
        let context = Context::new().unwrap();

        let mut router = context.socket::<Router>().unwrap();
        let bound = router.bind("tcp://127.0.0.1:*").unwrap();
        let dealer = Socket::new(Kind::Dealer);
        dealer.set_bytes_option(option::HICCUP_MSG, b"hiccup");
        dealer.connect(bound);
        dealer.send(&[b"said"]).unwrap();
        assert!(router.recv_timeout(FIVE).unwrap().is_some());
        drop(router);
        assert_eq!(dealer.recv().unwrap(), [b"hiccup"], "libzmq");

        let router = Socket::new(Kind::Router);
        let addr = router.bind_loopback();
        let mut dealer = context.socket::<Dealer>().unwrap();
        dealer.set_hiccup_message("hiccup");
        dealer.connect(format!("tcp://{addr}")).unwrap();
        dealer.send("said").unwrap();
        router.recv().unwrap();
        drop(router);
        assert_eq!(
            dealer.recv_timeout(FIVE).unwrap(),
            Some(Message::from("hiccup")),
            "ours"
        );
    }
}
