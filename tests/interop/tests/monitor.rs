//! Monitor events for connections with libzmq peers.

use std::thread;
use std::time::{Duration, Instant};

use zmq::{
    Context, Credential, Event, EventKind, HandshakeFailure, Keypair, Monitor, PlainCredentials,
    Pull, Push, Reconnect, Security, SocketOptions,
};
use zmq_interop::libzmq::{self, Kind, Socket};

const FIVE: Duration = Duration::from_secs(5);

/// Waits for the first event matching `wanted`, failing after five seconds
/// with everything seen so far.
fn expect(monitor: &mut Monitor, wanted: impl Fn(&EventKind) -> bool) -> Event {
    let started = Instant::now();
    let mut seen = Vec::new();
    loop {
        if let Some(event) = monitor.try_recv() {
            if wanted(&event.kind) {
                return event;
            }
            seen.push(event);
            continue;
        }
        assert!(started.elapsed() < FIVE, "not among {seen:#?}");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn a_libzmq_denial_is_reported_with_its_zap_status() {
    let context = libzmq::Context::new();
    let _zap = libzmq::zap_handler(&context, "400", "");
    let router = Socket::in_context(&context, Kind::Router);
    router.set_plain_server();
    let addr = router.bind_loopback();

    let ours = Context::new().unwrap();
    let mut push = ours.socket_with::<Push>(SocketOptions {
        security: Security::PlainClient(PlainCredentials {
            username: Credential::new("admin").unwrap(),
            password: Credential::new("wrong").unwrap(),
        }),
        ..SocketOptions::default()
    });
    let mut monitor = push.monitor();
    push.connect(&format!("tcp://{addr}")).unwrap();
    expect(&mut monitor, |kind| {
        *kind == EventKind::HandshakeFailed(HandshakeFailure::Authentication { status: 400 })
    });
}

#[test]
fn a_libzmq_server_dropping_a_bad_hello_fails_without_detail() {
    let pull = Socket::new(Kind::Pull);
    pull.set_curve_server(Keypair::generate().secret().expose());
    let addr = pull.bind_loopback();
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(SocketOptions {
        security: Security::curve_client(Keypair::generate(), Keypair::generate().public()),
        reconnect: Reconnect::Never,
        ..SocketOptions::default()
    });
    let mut monitor = push.monitor();
    push.connect(&format!("tcp://{addr}")).unwrap();
    expect(&mut monitor, |kind| *kind == EventKind::Connected);
    expect(&mut monitor, |kind| {
        *kind == EventKind::HandshakeFailed(HandshakeFailure::NoDetail)
    });
    expect(&mut monitor, |kind| *kind == EventKind::Disconnected);
}

#[test]
fn a_libzmq_client_is_accepted_then_disconnects() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let mut monitor = pull.monitor();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let push = Socket::new(Kind::Push);
    push.connect(bound.clone());
    let accepted = expect(&mut monitor, |kind| *kind == EventKind::Accepted);
    assert_eq!(accepted.endpoint, bound.to_string());
    expect(&mut monitor, |kind| *kind == EventKind::HandshakeSucceeded);
    drop(push);
    expect(&mut monitor, |kind| *kind == EventKind::Disconnected);
}
