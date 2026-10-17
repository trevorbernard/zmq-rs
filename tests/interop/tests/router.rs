//! DEALER and ROUTER (RFC 28) through the typed `zmq` API, against libzmq.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{
    Context, Dealer, Frame, Identity, Message, RouteOutcome, Router, RoutingId, SocketOptions,
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

fn identity(id: &str) -> SocketOptions {
    SocketOptions {
        identity: Identity::new(id.to_owned()).unwrap(),
        ..SocketOptions::default()
    }
}

fn id(text: &str) -> RoutingId {
    RoutingId::new(text.to_owned()).unwrap()
}

fn message(parts: &[&[u8]]) -> Message {
    let frames = parts
        .iter()
        .map(|p| Frame::copy_from_slice(p))
        .collect::<Vec<_>>();
    Message::try_from(frames).unwrap()
}

#[test]
fn our_dealer_talks_to_a_libzmq_router() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let mut dealer = context.socket_with::<Dealer>(identity("d1"));
    dealer.connect(&format!("tcp://{addr}")).unwrap();

    dealer.send("hello");
    assert_eq!(router.recv().unwrap(), [&b"d1"[..], b"hello"]);
    router.send(&[b"d1", b"reply"]).unwrap();
    let reply = within(Duration::from_secs(5), move || dealer.recv());
    assert_eq!(reply, Message::from("reply"));
}

#[test]
fn our_router_talks_to_a_libzmq_dealer() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    let addr = router.bind("tcp://127.0.0.1:*").unwrap();
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_routing_id(b"lib");
    dealer.connect(addr);
    dealer.send(&[b"hi"]).unwrap();

    let (from, received, router) = within(Duration::from_secs(5), move || {
        let (from, received) = router.recv();
        (from, received, router)
    });
    assert_eq!(from, id("lib"));
    assert_eq!(received, Message::from("hi"));
    let mut router = router;
    assert_eq!(router.send(&from, "back"), RouteOutcome::Sent);
    assert_eq!(dealer.recv().unwrap(), [b"back".to_vec()]);
}

#[test]
fn router_keeps_the_envelope_for_the_application() {
    // A REQ peer prefixes an empty delimiter; ROUTER hands it over intact.
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    let addr = router.bind("tcp://127.0.0.1:*").unwrap();
    let req = Socket::new(Kind::Req);
    req.set_routing_id(b"client");
    req.connect(addr);
    req.send(&[b"question"]).unwrap();

    let (from, request, mut router) = within(Duration::from_secs(5), move || {
        let (from, request) = router.recv();
        (from, request, router)
    });
    assert_eq!(request, message(&[b"", b"question"]));
    router.send(&from, message(&[b"", b"answer"]));
    assert_eq!(req.recv().unwrap(), [b"answer".to_vec()]);
}

#[test]
fn router_generates_routing_ids_for_anonymous_peers() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    let addr = router.bind("tcp://127.0.0.1:*").unwrap();
    let dealers: Vec<Socket> = (0..2)
        .map(|i| {
            let dealer = Socket::new(Kind::Dealer);
            dealer.connect(addr.clone());
            dealer.send(&[format!("from {i}").as_bytes()]).unwrap();
            dealer
        })
        .collect();
    let (received, mut router) = within(Duration::from_secs(5), move || {
        let received: Vec<(RoutingId, Message)> = (0..2).map(|_| router.recv()).collect();
        (received, router)
    });
    for (from, _) in &received {
        assert_eq!(from.as_bytes().len(), 5, "{from:?}");
        assert_eq!(from.as_bytes()[0], 0, "generated ids start with zero");
    }
    assert_ne!(received[0].0, received[1].0);
    for (from, sent) in &received {
        router.send(from, sent.clone());
    }
    for (i, dealer) in dealers.iter().enumerate() {
        assert_eq!(
            dealer.recv().unwrap(),
            [format!("from {i}").into_bytes()],
            "each reply reaches its own peer"
        );
    }
}

#[test]
fn router_drops_what_it_cannot_route_without_blocking() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.bind("tcp://127.0.0.1:*").unwrap();
    assert_eq!(router.send(&id("nobody"), "lost"), RouteOutcome::NoSuchPeer);
}

#[test]
fn router_ignores_a_peer_reusing_an_identity() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    let addr = router.bind("tcp://127.0.0.1:*").unwrap();
    let first = Socket::new(Kind::Dealer);
    first.set_routing_id(b"same");
    first.connect(addr.clone());
    first.send(&[b"first"]).unwrap();
    let (from, received, router) = within(Duration::from_secs(5), move || {
        let (from, received) = router.recv();
        (from, received, router)
    });
    assert_eq!((from, received), (id("same"), Message::from("first")));

    let second = Socket::new(Kind::Dealer);
    second.set_routing_id(b"same");
    second.connect(addr);
    second.send(&[b"second"]).unwrap();
    thread::sleep(Duration::from_millis(300));
    let mut router = router;
    assert_eq!(
        router.try_recv(),
        None,
        "the duplicate's messages are dropped"
    );
    router.send(&id("same"), "to the first");
    assert_eq!(first.recv().unwrap(), [b"to the first".to_vec()]);
}

#[test]
fn dealer_load_balances_and_fair_queues() {
    let routers: Vec<Socket> = (0..2).map(|_| Socket::new(Kind::Router)).collect();
    let context = Context::new().unwrap();
    let mut dealer = context.socket_with::<Dealer>(identity("lb"));
    for router in &routers {
        dealer
            .connect(&format!("tcp://{}", router.bind_loopback()))
            .unwrap();
    }
    for i in 0..4 {
        dealer.send(format!("{i}"));
    }
    for (r, router) in routers.iter().enumerate() {
        for k in 0..2 {
            assert_eq!(
                router.recv().unwrap(),
                [b"lb".to_vec(), format!("{}", k * 2 + r).into_bytes()]
            );
        }
        router
            .send(&[b"lb", format!("reply {r}").as_bytes()])
            .unwrap();
    }
    let mut replies = within(Duration::from_secs(5), move || {
        vec![dealer.recv(), dealer.recv()]
    });
    replies.sort_by_key(|m| m.get(0).unwrap().to_vec());
    assert_eq!(
        replies,
        [Message::from("reply 0"), Message::from("reply 1")]
    );
}

#[test]
fn our_router_and_dealer_talk_to_each_other() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    let addr = router.bind("tcp://127.0.0.1:*").unwrap();
    let mut dealer = context.socket_with::<Dealer>(identity("ours"));
    dealer.connect(&addr.to_string()).unwrap();
    dealer.send("ping");
    let (from, ping, mut router) = within(Duration::from_secs(5), move || {
        let (from, ping) = router.recv();
        (from, ping, router)
    });
    assert_eq!((&from, ping), (&id("ours"), Message::from("ping")));
    router.send(&from, "pong");
    assert_eq!(
        within(Duration::from_secs(5), move || dealer.recv()),
        Message::from("pong")
    );
}

#[test]
fn a_connecting_router_routes_once_the_peer_is_known() {
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_routing_id(b"srv");
    let addr = dealer.bind_loopback();
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    router.connect(&format!("tcp://{addr}")).unwrap();
    dealer.send(&[b"hello router"]).unwrap();
    let (from, hello, mut router) = within(Duration::from_secs(5), move || {
        let (from, hello) = router.recv();
        (from, hello, router)
    });
    assert_eq!((&from, hello), (&id("srv"), Message::from("hello router")));
    assert_eq!(router.send(&from, "hello dealer"), RouteOutcome::Sent);
    assert_eq!(dealer.recv().unwrap(), [b"hello dealer".to_vec()]);
}

#[test]
fn plain_secured_dealer_and_router() {
    use zmq::{Credential, PlainCredentials, Security, ZapCredentials, ZapReply, ZapRequest};

    let expected = PlainCredentials {
        username: Credential::new("admin").unwrap(),
        password: Credential::new("hunter2").unwrap(),
    };
    let check = expected.clone();
    let authenticator = move |request: &ZapRequest| {
        if request.credentials == ZapCredentials::Plain(check.clone()) {
            ZapReply::Ok {
                user_id: Frame::from("admin"),
                metadata: zmtp::Metadata::new(),
            }
        } else {
            ZapReply::AuthenticationFailure
        }
    };
    let context = Context::new().unwrap();
    let mut router = context.socket_with::<Router>(SocketOptions {
        security: Security::plain_server("", authenticator),
        ..SocketOptions::default()
    });
    let addr = router.bind("tcp://127.0.0.1:*").unwrap();
    let mut dealer = context.socket_with::<Dealer>(SocketOptions {
        security: Security::PlainClient(expected),
        ..identity("secure")
    });
    dealer.connect(&addr.to_string()).unwrap();
    dealer.send("over plain");
    let (from, received) = within(Duration::from_secs(5), move || router.recv());
    assert_eq!(
        (from, received),
        (id("secure"), Message::from("over plain"))
    );
}
