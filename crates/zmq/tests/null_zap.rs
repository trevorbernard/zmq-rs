//! NULL connections authenticated through ZAP, as libzmq authenticates a
//! NULL socket with a ZAP domain: by the peer's address.

use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{
    Context, EventKind, Frame, HandshakeFailure, Message, Metadata, Monitor, Pull, Push, Security,
    SocketOptions, ZapCredentials, ZapReply, ZapRequest,
};

use zmq::ProtocolError;

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

/// Waits for the first event matching `wanted`.
fn expect(monitor: &mut Monitor, wanted: impl Fn(&EventKind) -> bool) -> EventKind {
    let started = Instant::now();
    let mut seen = Vec::new();
    loop {
        if let Some(event) = monitor.try_recv() {
            if wanted(&event.kind) {
                return event.kind;
            }
            seen.push(event.kind);
            continue;
        }
        assert!(started.elapsed() < FIVE, "not among {seen:#?}");
        thread::sleep(Duration::from_millis(1));
    }
}

fn guarded(
    authenticator: impl Fn(&ZapRequest) -> ZapReply + Send + Sync + 'static,
) -> SocketOptions {
    SocketOptions {
        security: Security::null_authenticated("lan", authenticator),
        ..SocketOptions::default()
    }
}

fn accepted() -> ZapReply {
    ZapReply::Ok {
        user_id: Frame::from("local"),
        metadata: Metadata::new(),
    }
}

#[test]
fn an_accepted_peer_talks_and_zap_saw_its_address() {
    let (seen, requests) = mpsc::channel();
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(guarded(move |request: &ZapRequest| {
        let _ = seen.send(request.clone());
        accepted()
    }));
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&bound.to_string()).unwrap();
    push.send("let in");
    assert_eq!(within(FIVE, move || pull.recv()), Message::from("let in"));
    let request = requests.recv_timeout(FIVE).unwrap();
    assert_eq!(request.credentials, ZapCredentials::Null);
    assert_eq!(request.credentials.mechanism(), "NULL");
    assert_eq!(request.domain, Frame::from("lan"));
    assert_eq!(request.address, "127.0.0.1");
}

#[test]
fn a_denied_peer_hears_400_and_gets_nothing_through() {
    let context = Context::new().unwrap();
    let mut pull =
        context.socket_with::<Pull>(guarded(|_: &ZapRequest| ZapReply::AuthenticationFailure));
    let mut server = pull.monitor();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket::<Push>();
    let mut client = push.monitor();
    push.connect(&bound.to_string()).unwrap();
    push.send("keep out");
    let denied = EventKind::HandshakeFailed(HandshakeFailure::Authentication { status: 400 });
    assert_eq!(expect(&mut server, |kind| *kind == denied), denied);
    assert_eq!(expect(&mut client, |kind| *kind == denied), denied);
    assert_eq!(pull.try_recv(), None);
}

#[test]
fn a_temporary_failure_closes_without_a_word() {
    let context = Context::new().unwrap();
    let mut pull =
        context.socket_with::<Pull>(guarded(|_: &ZapRequest| ZapReply::TemporaryFailure));
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket::<Push>();
    let mut client = push.monitor();
    push.connect(&bound.to_string()).unwrap();
    // No ERROR is sent for 300, so the client only sees the connection go.
    expect(&mut client, |kind| {
        *kind == EventKind::HandshakeFailed(HandshakeFailure::NoDetail)
    });
}

#[test]
fn a_zmtp_1_0_peer_is_refused() {
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(guarded(|_: &ZapRequest| accepted()));
    let mut server = pull.monitor();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut legacy = TcpStream::connect(bound.tcp_addr().unwrap()).unwrap();
    legacy.write_all(b"\x01\x00\x06\x00hello").unwrap();
    // Refused at the greeting, as libzmq refuses it, before any handshake.
    expect(&mut server, |kind| {
        matches!(
            kind,
            EventKind::HandshakeFailed(HandshakeFailure::Protocol(ProtocolError::Greeting(_)))
        )
    });
    assert_eq!(pull.try_recv(), None);
}
