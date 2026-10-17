//! CURVE between our sockets and libzmq's.

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{
    Context, Dealer, Frame, Heartbeat, Keypair, Message, Metadata, Pub, Push, Reconnect, Router,
    Security, SocketOptions, ZapCredentials, ZapReply, ZapRequest,
};
use zmq_interop::libzmq::{Kind, Socket};

const FIVE: Duration = Duration::from_secs(5);

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

fn secured(security: Security) -> SocketOptions {
    SocketOptions {
        security,
        ..SocketOptions::default()
    }
}

#[test]
fn our_curve_dealer_talks_to_a_libzmq_curve_router() {
    let server = Keypair::generate();
    let router = Socket::new(Kind::Router);
    router.set_receive_timeout(FIVE);
    router.set_curve_server(server.secret().expose());
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let mut dealer = context.socket_with::<Dealer>(SocketOptions {
        security: Security::curve_client(Keypair::generate(), server.public()),
        identity: zmq::Identity::new("ours").unwrap(),
        ..SocketOptions::default()
    });
    dealer.connect(&format!("tcp://{addr}")).unwrap();
    dealer.send("hello libzmq");
    assert_eq!(router.recv().unwrap(), [&b"ours"[..], b"hello libzmq"]);
    router.send(&[b"ours", b"hello rust"]).unwrap();
    assert_eq!(
        within(FIVE, move || dealer.recv()),
        Message::from("hello rust")
    );
}

#[test]
fn a_libzmq_curve_dealer_is_authenticated_by_our_router() {
    let server = Keypair::generate();
    let server_key = server.public();
    let client = Keypair::generate();
    let client_key = client.public();
    let (seen, keys) = mpsc::channel();
    let context = Context::new().unwrap();
    let mut router = context.socket_with::<Router>(secured(Security::curve_server_authenticated(
        server,
        "",
        move |request: &ZapRequest| {
            let _ = seen.send(request.credentials.clone());
            ZapReply::Ok {
                user_id: Frame::from("libzmq"),
                metadata: Metadata::new(),
            }
        },
    )));
    let bound = router.bind("tcp://127.0.0.1:*").unwrap();
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_receive_timeout(FIVE);
    dealer.set_curve_client(
        client_key.as_bytes(),
        client.secret().expose(),
        server_key.as_bytes(),
    );
    dealer.connect(bound);
    dealer.send(&[b"knock knock"]).unwrap();
    let (from, message, mut router) = within(FIVE, move || {
        let (from, message) = router.recv();
        (from, message, router)
    });
    assert_eq!(message, Message::from("knock knock"));
    assert_eq!(keys.recv().unwrap(), ZapCredentials::Curve(client_key));
    router.send(&from, "who's there").unwrap();
    assert_eq!(dealer.recv().unwrap(), [b"who's there"]);
}

#[test]
fn a_libzmq_curve_sub_subscribes_through_the_box() {
    let server = Keypair::generate();
    let server_key = server.public();
    let context = Context::new().unwrap();
    let mut publisher = context.socket_with::<Pub>(secured(Security::curve_server(server)));
    let bound = publisher.bind("tcp://127.0.0.1:*").unwrap();
    let client = Keypair::generate();
    let subscriber = Socket::new(Kind::Sub);
    subscriber.set_receive_timeout(FIVE);
    subscriber.set_curve_client(
        client.public().as_bytes(),
        client.secret().expose(),
        server_key.as_bytes(),
    );
    subscriber.subscribe(b"news");
    subscriber.connect(bound);
    let started = Instant::now();
    while publisher.send("news: encrypted") == 0 {
        assert!(
            started.elapsed() < FIVE,
            "the boxed SUBSCRIBE never arrived"
        );
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(publisher.send("sports"), 0);
    assert_eq!(subscriber.recv().unwrap(), [b"news: encrypted"]);
}

#[test]
fn libzmq_answers_our_boxed_pings() {
    let server = Keypair::generate();
    let pull = Socket::new(Kind::Pull);
    pull.set_receive_timeout(FIVE);
    pull.set_curve_server(server.secret().expose());
    let addr = pull.bind_loopback();
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(SocketOptions {
        security: Security::curve_client(Keypair::generate(), server.public()),
        heartbeat: Some(Heartbeat::every(Duration::from_millis(20))),
        // A dropped connection must fail the test, not quietly reconnect.
        reconnect: Reconnect::Never,
        ..SocketOptions::default()
    });
    push.connect(&format!("tcp://{addr}")).unwrap();
    push.send("first");
    assert_eq!(pull.recv().unwrap(), [b"first"]);
    // Well past the heartbeat timeout: only boxed PONGs keep us connected.
    thread::sleep(Duration::from_millis(300));
    push.try_send("still connected")
        .expect("the connection survived the heartbeat timeout");
    assert_eq!(pull.recv().unwrap(), [b"still connected"]);
}

#[test]
fn libzmq_refuses_our_client_with_the_wrong_server_key() {
    let pull = Socket::new(Kind::Pull);
    pull.set_receive_timeout(Duration::from_millis(300));
    pull.set_curve_server(Keypair::generate().secret().expose());
    let addr = pull.bind_loopback();
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(secured(Security::curve_client(
        Keypair::generate(),
        Keypair::generate().public(),
    )));
    push.connect(&format!("tcp://{addr}")).unwrap();
    push.send("never");
    assert!(pull.recv().is_err());
}

#[test]
fn a_connection_records_who_authenticated_it() {
    use zmq_core::{ConnectionConfig, Context as Core, Reconnect};
    use zmtp::SocketType;

    for with_authenticator in [false, true] {
        let server = Keypair::generate();
        let server_key = server.public();
        let security = if with_authenticator {
            Security::curve_server_authenticated(server, "", |_: &ZapRequest| ZapReply::Ok {
                user_id: Frame::from("vouched"),
                metadata: Metadata::new(),
            })
        } else {
            Security::curve_server(server)
        };
        let core = Core::new().unwrap();
        let mut listener = core
            .bind(
                "127.0.0.1:0".parse().unwrap(),
                ConnectionConfig {
                    security,
                    ..ConnectionConfig::new(SocketType::Router)
                },
            )
            .unwrap();
        let _client = core.connect(
            listener.local_addr(),
            ConnectionConfig {
                security: Security::curve_client(Keypair::generate(), server_key),
                reconnect: Reconnect::Never,
                ..ConnectionConfig::new(SocketType::Dealer)
            },
        );
        let connection = within(FIVE, move || listener.accept_blocking().unwrap());
        let authenticated = connection.peer().unwrap().authenticated;
        if with_authenticator {
            assert_eq!(authenticated.unwrap().user_id, Frame::from("vouched"));
        } else {
            assert_eq!(authenticated, None, "nobody vouched for it");
        }
    }
}
