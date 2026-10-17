//! CURVE between our own sockets: encrypted, authenticated by key.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{
    Context, Dealer, Frame, Heartbeat, Keypair, Message, Metadata, Pub, Pull, Push, Router,
    Security, SocketOptions, Sub, ZapCredentials, ZapReply, ZapRequest,
};

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
fn push_and_pull_over_curve() {
    let server = Keypair::generate();
    let server_key = server.public();
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(secured(Security::curve_server(server)));
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket_with::<Push>(secured(Security::curve_client(
        Keypair::generate(),
        server_key,
    )));
    push.connect(&bound.to_string()).unwrap();
    let big =
        Message::try_from(vec![Frame::from("header"), Frame::from(vec![9; 200_000])]).unwrap();
    push.send(big.clone());
    push.send("small");
    let received = within(FIVE, move || [pull.recv(), pull.recv()]);
    assert_eq!(received, [big, Message::from("small")]);
}

#[test]
fn the_authenticator_sees_the_clients_public_key() {
    let server = Keypair::generate();
    let server_key = server.public();
    let client = Keypair::generate();
    let client_key = client.public();
    let allowed = client_key;
    let authenticator = move |request: &ZapRequest| {
        if request.credentials == ZapCredentials::Curve(allowed) {
            ZapReply::Ok {
                user_id: Frame::from("trusted"),
                metadata: Metadata::new(),
            }
        } else {
            ZapReply::AuthenticationFailure
        }
    };
    let context = Context::new().unwrap();
    let mut router = context.socket_with::<Router>(secured(Security::curve_server_authenticated(
        server,
        "domain",
        authenticator,
    )));
    let bound = router.bind("tcp://127.0.0.1:*").unwrap();

    let mut trusted =
        context.socket_with::<Dealer>(secured(Security::curve_client(client, server_key)));
    trusted.connect(&bound.to_string()).unwrap();
    trusted.send("let me in");
    let mut stranger = context.socket_with::<Dealer>(secured(Security::curve_client(
        Keypair::generate(),
        server_key,
    )));
    stranger.connect(&bound.to_string()).unwrap();
    stranger.send("me too");

    let (from, message, mut router) = within(FIVE, move || {
        let (from, message) = router.recv();
        (from, message, router)
    });
    assert_eq!(message, Message::from("let me in"));
    thread::sleep(Duration::from_millis(200));
    assert_eq!(router.try_recv(), None, "the stranger never gets through");
    router.send(&from, "welcome").unwrap();
    assert_eq!(
        within(FIVE, move || trusted.recv()),
        Message::from("welcome")
    );
}

#[test]
fn a_client_with_the_wrong_server_key_gets_nowhere() {
    let context = Context::new().unwrap();
    let mut pull =
        context.socket_with::<Pull>(secured(Security::curve_server(Keypair::generate())));
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket_with::<Push>(secured(Security::curve_client(
        Keypair::generate(),
        Keypair::generate().public(),
    )));
    push.connect(&bound.to_string()).unwrap();
    push.send("lost");
    thread::sleep(Duration::from_millis(300));
    assert_eq!(pull.try_recv(), None);
}

#[test]
fn a_plaintext_client_cannot_talk_to_a_curve_server() {
    let context = Context::new().unwrap();
    let mut pull =
        context.socket_with::<Pull>(secured(Security::curve_server(Keypair::generate())));
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&bound.to_string()).unwrap();
    push.send("in the clear");
    thread::sleep(Duration::from_millis(300));
    assert_eq!(pull.try_recv(), None);
}

#[test]
fn heartbeats_and_subscriptions_travel_boxed() {
    let server = Keypair::generate();
    let server_key = server.public();
    let context = Context::new().unwrap();
    let mut publisher = context.socket_with::<Pub>(SocketOptions {
        security: Security::curve_server(server),
        heartbeat: Some(Heartbeat::every(Duration::from_millis(20))),
        ..SocketOptions::default()
    });
    let bound = publisher.bind("tcp://127.0.0.1:*").unwrap();
    let mut subscriber = context.socket_with::<Sub>(SocketOptions {
        security: Security::curve_client(Keypair::generate(), server_key),
        heartbeat: Some(Heartbeat::every(Duration::from_millis(20))),
        ..SocketOptions::default()
    });
    subscriber.subscribe("news");
    subscriber.connect(&bound.to_string()).unwrap();
    let started = std::time::Instant::now();
    while publisher.send("news: boxed") == 0 {
        assert!(started.elapsed() < FIVE, "the subscription never arrived");
        thread::sleep(Duration::from_millis(5));
    }
    // Several heartbeat intervals pass; the connection stays up.
    thread::sleep(Duration::from_millis(200));
    assert_eq!(publisher.send("sports"), 0);
    assert_eq!(publisher.send("news: still here"), 1);
    let received = within(FIVE, move || [subscriber.recv(), subscriber.recv()]);
    assert_eq!(
        received,
        [
            Message::from("news: boxed"),
            Message::from("news: still here")
        ]
    );
}

#[cfg(unix)]
#[test]
fn curve_over_ipc() {
    let server = Keypair::generate();
    let server_key = server.public();
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(secured(Security::curve_server(server)));
    let bound = pull.bind("ipc://*").unwrap();
    let mut push = context.socket_with::<Push>(secured(Security::curve_client(
        Keypair::generate(),
        server_key,
    )));
    push.connect(&bound.to_string()).unwrap();
    push.send("over a unix socket, encrypted");
    assert_eq!(
        within(FIVE, move || pull.recv()),
        Message::from("over a unix socket, encrypted")
    );
}
