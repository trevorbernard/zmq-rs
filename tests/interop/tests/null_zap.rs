//! NULL connections authenticated through ZAP, against libzmq.

use std::thread;
use std::time::{Duration, Instant};

use zmq::{
    Context, EventKind, Frame, HandshakeFailure, Message, Metadata, Monitor, Pull, Push, Security,
    SocketOptions, ZapCredentials, ZapReply, ZapRequest,
};
use zmq_interop::libzmq::{self, Kind, Socket};

const FIVE: Duration = Duration::from_secs(5);

/// Waits for the first event matching `wanted`.
fn expect(monitor: &mut Monitor, wanted: impl Fn(&EventKind) -> bool) {
    let started = Instant::now();
    let mut seen = Vec::new();
    loop {
        if let Ok(event) = monitor.try_recv() {
            if wanted(&event.kind) {
                return;
            }
            seen.push(event.kind);
            continue;
        }
        assert!(started.elapsed() < FIVE, "not among {seen:#?}");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn we_authenticate_a_libzmq_null_peer() {
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(SocketOptions {
        security: Security::null_authenticated("lan", |request: &ZapRequest| {
            assert_eq!(request.credentials, ZapCredentials::Null);
            ZapReply::Ok {
                user_id: Frame::from("libzmq"),
                metadata: Metadata::new(),
            }
        }),
        ..SocketOptions::default()
    });
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let push = Socket::new(Kind::Push);
    push.connect(bound);
    push.send(&[b"from libzmq"]).unwrap();
    let started = Instant::now();
    loop {
        if let Some(message) = pull.try_recv() {
            assert_eq!(message, Message::from("from libzmq"));
            break;
        }
        assert!(started.elapsed() < FIVE, "nothing arrived");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn we_refuse_a_libzmq_null_peer() {
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(SocketOptions {
        security: Security::null_authenticated("lan", |_: &ZapRequest| {
            ZapReply::AuthenticationFailure
        }),
        ..SocketOptions::default()
    });
    let mut monitor = pull.monitor();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let push = Socket::new(Kind::Push);
    push.connect(bound);
    push.send(&[b"keep out"]).unwrap();
    expect(&mut monitor, |kind| {
        *kind == EventKind::HandshakeFailed(HandshakeFailure::Authentication { status: 400 })
    });
    assert_eq!(pull.try_recv(), None);
}

#[test]
fn libzmq_authenticates_our_null_peer_through_its_zap_handler() {
    for (status, admitted) in [("200", true), ("400", false)] {
        let zmq_context = libzmq::Context::new();
        let handler = libzmq::zap_handler(&zmq_context, status, "user");
        let pull = Socket::in_context(&zmq_context, Kind::Pull);
        pull.set_receive_timeout(Duration::from_millis(500));
        pull.set_zap_domain(b"lan");
        let addr = pull.bind_loopback();

        let context = Context::new().unwrap();
        let mut push = context.socket::<Push>();
        let mut monitor = push.monitor();
        push.connect(&format!("tcp://{addr}")).unwrap();
        push.send("hello");
        let request = handler.join().unwrap();
        assert_eq!(
            request[5], b"NULL",
            "libzmq asked its ZAP handler about NULL"
        );
        if admitted {
            assert_eq!(pull.recv().unwrap(), [b"hello"]);
        } else {
            expect(&mut monitor, |kind| {
                *kind
                    == EventKind::HandshakeFailed(HandshakeFailure::Authentication { status: 400 })
            });
            assert!(pull.recv().is_err());
        }
    }
}

#[test]
fn the_connection_records_whom_zap_accepted() {
    use zmq_core::{ConnectionConfig, Context as Core, Reconnect};
    use zmtp::SocketType;

    let core = Core::new().unwrap();
    let mut listener = core
        .bind(
            "127.0.0.1:0".parse().unwrap(),
            ConnectionConfig {
                security: Security::null_authenticated("lan", |_: &ZapRequest| ZapReply::Ok {
                    user_id: Frame::from("trusted host"),
                    metadata: Metadata::new(),
                }),
                ..ConnectionConfig::new(SocketType::Pull)
            },
        )
        .unwrap();
    let _client = core.connect(
        listener.local_addr(),
        ConnectionConfig {
            reconnect: Reconnect::Never,
            ..ConnectionConfig::new(SocketType::Push)
        },
    );
    let started = Instant::now();
    let connection = loop {
        if let Ok(connection) = listener.try_accept() {
            break connection;
        }
        assert!(started.elapsed() < FIVE, "never accepted");
        thread::sleep(Duration::from_millis(1));
    };
    let authenticated = connection.peer().unwrap().authenticated.unwrap();
    assert_eq!(authenticated.user_id, Frame::from("trusted host"));
}
