//! libzmq's `tests/test_security_zap.cpp`: ZAP handlers that answer badly
//! or not at all, for NULL, PLAIN and CURVE servers, watched through both
//! sides' monitors.
//!
//! zmq-rs's ZAP handler is an in-process `Authenticator` answering with a
//! `ZapReply`, not a socket speaking RFC 27's frames, so these libzmq
//! cases cannot be written: a reply with the wrong version, the wrong
//! request id, an invalid status code or too many frames
//! (`test_zap_protocol_error_*`); a handler that never reads its socket
//! (`test_zap_unsuccessful_do_not_recv`), as an authenticator is always
//! handed the request; and a ZAP domain enforced with no handler
//! (`test_zap_unsuccessful_no_handler_started`), as a domain comes only
//! with an authenticator (see `security_no_zap_handler`).

use std::sync::Mutex;
use std::time::Duration;

use zmq::{
    Authenticator, Context, Credential, Dealer, EventKind, HandshakeFailure, Identity, Keypair,
    Linger, Message, Monitor, PlainCredentials, Security, Socket, SocketOptions, ZapReply,
    ZapRequest, ZapResponder,
};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, SETTLE};

const DOMAIN: &str = "ZAPTEST";
const CONTENT: &str = "12345678ABCDEFGH12345678abcdefgh";

fn content() -> Message {
    let mut message = Message::new(CONTENT);
    message.push(CONTENT);
    message
}

#[derive(Debug, Clone, Copy)]
enum Mechanism {
    Null,
    Plain,
    Curve,
}

/// The server's and a valid client's options for `mechanism`, the server
/// asking `authenticator`, as libzmq's `socket_config_*` functions. Both
/// close with zero linger, as libzmq's tests close them.
fn options(
    mechanism: Mechanism,
    authenticator: impl Authenticator,
) -> (SocketOptions, SocketOptions) {
    let (server, client) = match mechanism {
        Mechanism::Null => (
            Security::null_authenticated(DOMAIN, authenticator),
            Security::Null,
        ),
        Mechanism::Plain => (
            Security::plain_server(DOMAIN, authenticator),
            Security::PlainClient(PlainCredentials {
                username: Credential::new("testuser").unwrap(),
                password: Credential::new("testpass").unwrap(),
            }),
        ),
        Mechanism::Curve => {
            let server = Keypair::generate();
            let server_key = server.public();
            (
                Security::curve_server_authenticated(server, DOMAIN, authenticator),
                Security::curve_client(Keypair::generate(), server_key),
            )
        }
    };
    let zero_linger = SocketOptions {
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    };
    (
        SocketOptions {
            identity: Identity::new("IDENT").unwrap(),
            security: server,
            // Long enough for a handshake, short enough for a test to see
            // a handler that never answers.
            handshake_timeout: Some(SETTLE),
            ..zero_linger.clone()
        },
        SocketOptions {
            security: client,
            ..zero_linger
        },
    )
}

/// The server and a client, each with its monitor, the client connected.
struct Pair {
    server: Socket<Dealer>,
    server_monitor: Monitor,
    client: Socket<Dealer>,
    client_monitor: Monitor,
}

fn connect(context: &Context, mechanism: Mechanism, authenticator: impl Authenticator) -> Pair {
    let (server_options, client_options) = options(mechanism, authenticator);
    let mut server = context.socket_with::<Dealer>(server_options);
    let server_monitor = server.monitor();
    let endpoint = server.bind(&free_endpoint()).unwrap().to_string();
    let mut client = context.socket_with::<Dealer>(client_options);
    let client_monitor = client.monitor();
    client.connect(&endpoint).unwrap();
    Pair {
        server,
        server_monitor,
        client,
        client_monitor,
    }
}

/// Neither direction gets through, as libzmq's `expect_bounce_fail`.
fn expect_bounce_fail(pair: &mut Pair) {
    let _ = pair.client.try_send(content());
    assert_eq!(pair.server.recv_timeout(SETTLE), None);
    let _ = pair.server.try_send(content());
    assert_eq!(pair.client.recv_timeout(SETTLE), None);
}

/// The handshake failures `monitor` has reported, waiting at most
/// `timeout` for the first.
fn handshake_failures(monitor: &mut Monitor, timeout: Duration) -> Vec<HandshakeFailure> {
    let mut failures = Vec::new();
    let mut wait = timeout;
    while let Ok(event) = monitor.recv_timeout(wait) {
        if let EventKind::HandshakeFailed(failure) = event.kind {
            failures.push(failure);
            wait = Duration::ZERO;
        }
    }
    failures
}

fn authentication(status: u16) -> HandshakeFailure {
    HandshakeFailure::Authentication { status }
}

/// The handler has gone: dropping the responder unanswered.
struct Disconnect;

impl Authenticator for Disconnect {
    fn authenticate(&self, _request: ZapRequest, _respond: ZapResponder) {}
}

/// The handler takes each request and never answers.
#[derive(Default)]
struct DoNotSend(Mutex<Vec<ZapResponder>>);

impl Authenticator for DoNotSend {
    fn authenticate(&self, _request: ZapRequest, respond: ZapResponder) {
        self.0.lock().unwrap().push(respond);
    }
}

/// Status 300: the server reports the failure; the client hears no ERROR,
/// so it reports no failed handshake.
fn zap_wrong_status_temporary_failure(mechanism: Mechanism) {
    let context = Context::new().unwrap();
    let mut pair = connect(&context, mechanism, |_: &ZapRequest| {
        ZapReply::TemporaryFailure
    });
    expect_bounce_fail(&mut pair);
    let failures = handshake_failures(&mut pair.server_monitor, FIVE);
    assert!(failures.contains(&authentication(300)), "{failures:?}");
    let failures = handshake_failures(&mut pair.client_monitor, Duration::ZERO);
    assert!(
        !failures
            .iter()
            .any(|f| matches!(f, HandshakeFailure::Authentication { .. })),
        "{failures:?}"
    );
}

/// Status 500: both sides report it, the client once, as the server's
/// ERROR stops it reconnecting.
fn zap_wrong_status_internal_error(mechanism: Mechanism) {
    let context = Context::new().unwrap();
    let mut pair = connect(&context, mechanism, |_: &ZapRequest| {
        ZapReply::InternalError
    });
    expect_bounce_fail(&mut pair);
    let failures = handshake_failures(&mut pair.server_monitor, FIVE);
    assert!(failures.contains(&authentication(500)), "{failures:?}");
    let failures = handshake_failures(&mut pair.client_monitor, FIVE);
    assert_eq!(failures, [authentication(500)]);
}

/// A handler that has gone. libzmq fails the handshake with no detail
/// (`EPIPE`); zmq-rs treats an authenticator that drops its responder
/// unanswered as an internal error, status 500 (`Authenticator`'s docs).
fn zap_unsuccessful_disconnect(mechanism: Mechanism) {
    let context = Context::new().unwrap();
    let mut pair = connect(&context, mechanism, Disconnect);
    expect_bounce_fail(&mut pair);
    let failures = handshake_failures(&mut pair.server_monitor, FIVE);
    assert!(failures.contains(&authentication(500)), "{failures:?}");
}

/// A handler that never answers: the handshake times out, with no detail.
fn zap_unsuccessful_do_not_send(mechanism: Mechanism) {
    let context = Context::new().unwrap();
    let mut pair = connect(&context, mechanism, DoNotSend::default());
    expect_bounce_fail(&mut pair);
    let failures = handshake_failures(&mut pair.server_monitor, FIVE);
    assert!(
        failures.contains(&HandshakeFailure::NoDetail),
        "{failures:?}"
    );
}

macro_rules! zap_error_tests {
    ($mechanism:ident: $($test:ident => $case:ident),* $(,)?) => {
        $(
            #[test]
            fn $test() {
                $case(Mechanism::$mechanism);
            }
        )*
    };
}

zap_error_tests! { Null:
    zap_wrong_status_temporary_failure_null => zap_wrong_status_temporary_failure,
    zap_wrong_status_internal_error_null => zap_wrong_status_internal_error,
    zap_unsuccessful_disconnect_null => zap_unsuccessful_disconnect,
    zap_unsuccessful_do_not_send_null => zap_unsuccessful_do_not_send,
}

zap_error_tests! { Plain:
    zap_wrong_status_temporary_failure_plain => zap_wrong_status_temporary_failure,
    zap_wrong_status_internal_error_plain => zap_wrong_status_internal_error,
    zap_unsuccessful_disconnect_plain => zap_unsuccessful_disconnect,
    zap_unsuccessful_do_not_send_plain => zap_unsuccessful_do_not_send,
}

zap_error_tests! { Curve:
    zap_wrong_status_temporary_failure_curve => zap_wrong_status_temporary_failure,
    zap_wrong_status_internal_error_curve => zap_wrong_status_internal_error,
    zap_unsuccessful_disconnect_curve => zap_unsuccessful_disconnect,
    zap_unsuccessful_do_not_send_curve => zap_unsuccessful_do_not_send,
}
