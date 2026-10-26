//! Socket monitoring, as libzmq's `zmq_socket_monitor`: typed events about
//! a socket's listeners and connections.

use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{
    Context, Credential, Event, EventKind, HandshakeFailure, Monitor, PlainCredentials,
    ProtocolError, Pull, Push, Reconnect, Security, SocketOptions, ZapReply, ZapRequest,
};

const FIVE: Duration = Duration::from_secs(5);

/// Every event up to and including the first matching `wanted`, failing
/// after five seconds with everything seen so far.
fn events_until(monitor: &mut Monitor, wanted: impl Fn(&EventKind) -> bool) -> Vec<EventKind> {
    let started = Instant::now();
    let mut seen = Vec::new();
    loop {
        if let Ok(event) = monitor.try_recv() {
            let done = wanted(&event.kind);
            seen.push(event.kind);
            if done {
                return seen;
            }
            continue;
        }
        assert!(started.elapsed() < FIVE, "not among {seen:#?}");
        thread::sleep(Duration::from_millis(1));
    }
}

/// Waits for the first event matching `wanted`, failing after five seconds
/// with everything seen so far.
fn expect(monitor: &mut Monitor, wanted: impl Fn(&EventKind) -> bool) -> Event {
    let started = Instant::now();
    let mut seen = Vec::new();
    loop {
        if let Ok(event) = monitor.try_recv() {
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
fn binding_reports_listening_or_bind_failed() {
    let context = Context::new().unwrap();
    let mut first = context.socket::<Pull>();
    let mut monitor = first.monitor();
    let bound = first.bind("tcp://127.0.0.1:*").unwrap();
    let event = expect(&mut monitor, |kind| *kind == EventKind::Listening);
    assert_eq!(event.endpoint, bound.to_string());

    let mut second = context.socket::<Pull>();
    let mut second_monitor = second.monitor();
    assert!(second.bind(&bound.to_string()).is_err());
    let event = expect(&mut second_monitor, |kind| {
        *kind == EventKind::BindFailed(std::io::ErrorKind::AddrInUse)
    });
    assert_eq!(event.endpoint, bound.to_string());
}

#[test]
fn both_ends_report_a_connection_and_its_handshake() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let mut server = pull.monitor();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket::<Push>();
    let mut client = push.monitor();
    push.connect(&bound.to_string()).unwrap();

    let connected = expect(&mut client, |kind| *kind == EventKind::Connected);
    assert_eq!(connected.endpoint, bound.to_string());
    expect(&mut client, |kind| *kind == EventKind::HandshakeSucceeded);
    let accepted = expect(&mut server, |kind| *kind == EventKind::Accepted);
    assert_eq!(accepted.endpoint, bound.to_string());
    expect(&mut server, |kind| *kind == EventKind::HandshakeSucceeded);
}

#[test]
fn a_lost_peer_is_reported_and_retried() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket_with::<Push>(SocketOptions {
        reconnect: Reconnect::Fixed {
            interval: Duration::from_millis(30),
        },
        ..SocketOptions::default()
    });
    let mut client = push.monitor();
    push.connect(&bound.to_string()).unwrap();
    expect(&mut client, |kind| *kind == EventKind::HandshakeSucceeded);
    drop(pull);
    expect(&mut client, |kind| *kind == EventKind::Disconnected);
    let retried = expect(&mut client, |kind| {
        matches!(kind, EventKind::ConnectRetried { .. })
    });
    let EventKind::ConnectRetried { interval } = retried.kind else {
        unreachable!()
    };
    assert!(interval <= Duration::from_millis(60), "{interval:?}");
}

#[test]
fn closing_a_listener_reports_closed() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let mut monitor = pull.monitor();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    drop(pull);
    let event = expect(&mut monitor, |kind| *kind == EventKind::Closed);
    assert_eq!(event.endpoint, bound.to_string());
}

#[test]
fn an_incompatible_peer_fails_the_handshake_on_both_sides() {
    let context = Context::new().unwrap();
    let mut server_socket = context.socket::<Push>();
    let mut server = server_socket.monitor();
    let bound = server_socket.bind("tcp://127.0.0.1:*").unwrap();
    let mut client_socket = context.socket::<Push>();
    let mut client = client_socket.monitor();
    client_socket.connect(&bound.to_string()).unwrap();

    let failed = expect(&mut server, |kind| {
        matches!(kind, EventKind::HandshakeFailed(_))
    });
    assert!(
        matches!(
            failed.kind,
            EventKind::HandshakeFailed(HandshakeFailure::Protocol(ProtocolError::Handshake(_)))
        ),
        "{failed:?}"
    );
    expect(&mut server, |kind| *kind == EventKind::Disconnected);
    expect(&mut client, |kind| {
        matches!(
            kind,
            EventKind::HandshakeFailed(HandshakeFailure::Protocol(_))
        )
    });
}

#[test]
fn a_denied_client_is_reported_as_an_authentication_failure() {
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(SocketOptions {
        security: Security::plain_server("", |_: &ZapRequest| ZapReply::AuthenticationFailure),
        ..SocketOptions::default()
    });
    let mut server = pull.monitor();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket_with::<Push>(SocketOptions {
        security: Security::PlainClient(PlainCredentials {
            username: Credential::new("who").unwrap(),
            password: Credential::new("me").unwrap(),
        }),
        ..SocketOptions::default()
    });
    let mut client = push.monitor();
    push.connect(&bound.to_string()).unwrap();
    let denied = EventKind::HandshakeFailed(HandshakeFailure::Authentication { status: 400 });
    expect(&mut server, |kind| *kind == denied);
    expect(&mut client, |kind| *kind == denied);
}

#[test]
fn a_silent_server_fails_the_handshake_without_detail() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(SocketOptions {
        handshake_timeout: Some(Duration::from_millis(50)),
        reconnect: Reconnect::Never,
        ..SocketOptions::default()
    });
    let mut client = push.monitor();
    push.connect(&format!("tcp://{}", listener.local_addr().unwrap()))
        .unwrap();
    let (_silent, _) = listener.accept().unwrap();
    expect(&mut client, |kind| *kind == EventKind::Connected);
    expect(&mut client, |kind| {
        *kind == EventKind::HandshakeFailed(HandshakeFailure::NoDetail)
    });
    expect(&mut client, |kind| *kind == EventKind::Disconnected);
}

#[test]
fn every_monitor_hears_every_event() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let mut first = pull.monitor();
    let mut second = pull.monitor();
    pull.bind("tcp://127.0.0.1:*").unwrap();
    expect(&mut first, |kind| *kind == EventKind::Listening);
    expect(&mut second, |kind| *kind == EventKind::Listening);
}

#[cfg(unix)]
#[test]
fn ipc_endpoints_are_reported_too() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let mut monitor = pull.monitor();
    let bound = pull.bind("ipc://*").unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&bound.to_string()).unwrap();
    let accepted = expect(&mut monitor, |kind| *kind == EventKind::Accepted);
    assert_eq!(accepted.endpoint, bound.to_string());
}

#[test]
fn an_accepted_connection_is_not_also_reported_as_connected() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let mut server = pull.monitor();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&bound.to_string()).unwrap();
    let seen = events_until(&mut server, |kind| *kind == EventKind::HandshakeSucceeded);
    assert_eq!(
        seen,
        [
            EventKind::Listening,
            EventKind::Accepted,
            EventKind::HandshakeSucceeded
        ]
    );
}

#[test]
fn an_attempt_that_never_connects_only_retries() {
    // A port with nobody listening: grab one, then free it.
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let context = Context::new().unwrap();
    let mut push = context.socket_with::<Push>(SocketOptions {
        reconnect: Reconnect::Fixed {
            interval: Duration::from_millis(10),
        },
        ..SocketOptions::default()
    });
    let mut monitor = push.monitor();
    push.connect(&format!("tcp://127.0.0.1:{port}")).unwrap();
    let mut seen = Vec::new();
    for _ in 0..3 {
        seen.extend(events_until(&mut monitor, |kind| {
            matches!(kind, EventKind::ConnectRetried { .. })
        }));
    }
    assert!(
        seen.iter()
            .all(|kind| matches!(kind, EventKind::ConnectRetried { .. })),
        "{seen:?}"
    );
}

/// Waits for `monitor` to report its socket gone, past any events.
fn assert_closes(mut monitor: Monitor) {
    let started = Instant::now();
    while monitor.recv_timeout(FIVE).is_ok() {
        assert!(started.elapsed() < FIVE, "still open");
    }
    assert_eq!(
        monitor.recv_timeout(Duration::ZERO),
        Err(zmq::RecvTimeoutError::Disconnected)
    );
}

#[test]
fn a_monitor_closes_with_its_socket_after_an_inproc_bind() {
    let context = Context::new().unwrap();
    let mut socket = context.socket::<Pull>();
    let monitor = socket.monitor();
    socket.bind("inproc://bound-then-dropped").unwrap();
    drop(socket);
    assert_closes(monitor);
}

#[test]
fn a_monitor_closes_with_its_socket_after_an_inproc_connect() {
    let context = Context::new().unwrap();
    let mut socket = context.socket::<Pull>();
    let monitor = socket.monitor();
    socket.connect("inproc://never-bound").unwrap();
    drop(socket);
    assert_closes(monitor);
}
