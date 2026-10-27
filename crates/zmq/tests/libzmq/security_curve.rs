//! libzmq's `tests/test_security_curve.cpp`: CURVE security with ZAP,
//! well and badly configured clients, and a hand-written client sending
//! malformed HELLO and INITIATE commands.
//!
//! The malformed commands start from valid ones made by `zmtp`'s CURVE
//! client, which plays the part of libzmq's `curve_client_tools_t`.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use zmq::{
    Context, Credential, Dealer, EventKind, Frame, HandshakeFailure, Identity, Keypair, Metadata,
    Monitor, PlainCredentials, ProtocolError, PublicKey, SecretKey, Security, Socket,
    SocketOptions, ZapCredentials, ZapReply, ZapRequest,
};
use zmtp::{CurveClient, CurveClientStep, GreetingError, HandshakeError, Incoming, PeerGreeting};

use crate::support::{FIVE, SETTLE, bounce_content, zero_linger};

const DOMAIN: &str = "ZAPTEST";
const TIMEOUT: Duration = Duration::from_millis(250);
const NULL_KEY: &str = "0000000000000000000000000000000000000000";

const ZMTP_GREETING_CURVE: [u8; 64] = {
    let mut greeting = [0; 64];
    greeting[0] = 0xff;
    greeting[9] = 0x7f;
    greeting[10] = 3;
    let mut at = 0;
    while at < 5 {
        greeting[12 + at] = b"CURVE"[at];
        at += 1;
    }
    greeting
};

/// libzmq's `setUp`: a CURVE server, named `identity`, asking a ZAP
/// handler that lets in only the valid client's public key.
struct Fixture {
    context: Context,
    server: Socket<Dealer>,
    server_monitor: Monitor,
    endpoint: String,
    server_public: PublicKey,
    client: Arc<Keypair>,
    zap_requests: Arc<AtomicUsize>,
    zap_identities: mpsc::Receiver<Frame>,
}

impl Fixture {
    fn new() -> Self {
        Self::named(Identity::new("IDENT").unwrap())
    }

    fn named(identity: Identity) -> Self {
        let context = Context::new().unwrap();
        let server_keys = Keypair::generate();
        let server_public = server_keys.public();
        let client = Arc::new(Keypair::generate());
        let valid = client.public();
        let zap_requests = Arc::new(AtomicUsize::new(0));
        let (seen, zap_identities) = mpsc::channel();
        let counted = zap_requests.clone();
        let handler = move |request: &ZapRequest| {
            counted.fetch_add(1, Ordering::SeqCst);
            let _ = seen.send(request.identity.clone());
            if request.credentials == ZapCredentials::Curve(valid) {
                ZapReply::Ok {
                    user_id: Frame::from("anonymous"),
                    metadata: Metadata::new(),
                }
            } else {
                ZapReply::AuthenticationFailure
            }
        };
        let mut server = context.socket_with::<Dealer>(SocketOptions {
            identity,
            security: Security::curve_server_authenticated(server_keys, DOMAIN, handler),
            ..zero_linger()
        });
        let server_monitor = server.monitor();
        let endpoint = server.bind("tcp://127.0.0.1:*").unwrap().to_string();
        Self {
            context,
            server,
            server_monitor,
            endpoint,
            server_public,
            client,
            zap_requests,
            zap_identities,
        }
    }

    fn client(&self, security: Security) -> (Socket<Dealer>, Monitor) {
        let mut client = self.context.socket_with::<Dealer>(SocketOptions {
            security,
            ..zero_linger()
        });
        let monitor = client.monitor();
        client.connect(&self.endpoint).unwrap();
        (client, monitor)
    }

    fn curve_client(&self, keys: impl Into<Arc<Keypair>>, server: PublicKey) -> Socket<Dealer> {
        self.client(Security::curve_client(keys, server)).0
    }

    /// A message from `client` to the server and back.
    fn bounce(&mut self, client: &mut Socket<Dealer>) {
        client.send(bounce_content());
        assert_eq!(self.server.recv_timeout(FIVE), Some(bounce_content()));
        self.server.send(bounce_content());
        assert_eq!(client.recv_timeout(FIVE), Some(bounce_content()));
    }

    /// Neither direction gets through.
    fn expect_bounce_fail(&mut self, client: &mut Socket<Dealer>) {
        let _ = client.try_send(bounce_content());
        assert_eq!(self.server.recv_timeout(SETTLE), None);
        let _ = self.server.try_send(bounce_content());
        assert_eq!(client.recv_timeout(SETTLE), None);
    }

    fn raw_client(&self) -> TcpStream {
        let stream = TcpStream::connect(self.endpoint.trim_start_matches("tcp://")).unwrap();
        stream.set_read_timeout(Some(FIVE)).unwrap();
        stream
    }
}

/// The handshake events `monitor` reports, waiting at most `timeout` for
/// the first, as libzmq's tests watch only those.
fn handshake_events(monitor: &mut Monitor, timeout: Duration) -> Vec<EventKind> {
    let mut events = Vec::new();
    let mut wait = timeout;
    while let Ok(event) = monitor.recv_timeout(wait) {
        if matches!(
            event.kind,
            EventKind::HandshakeSucceeded | EventKind::HandshakeFailed(_)
        ) {
            events.push(event.kind);
            wait = Duration::ZERO;
        }
    }
    events
}

/// Waits for the server to report the handshake failing with `error`.
fn expect_protocol_error(monitor: &mut Monitor, error: impl Fn(&HandshakeError) -> bool) {
    let events = handshake_events(monitor, FIVE);
    assert!(
        events.iter().any(|event| matches!(
            event,
            EventKind::HandshakeFailed(HandshakeFailure::Protocol(ProtocolError::Handshake(e)))
                if error(e)
        )),
        "{events:?}"
    );
}

#[test]
fn curve_security_with_valid_credentials() {
    let mut fixture = Fixture::new();
    let (mut client, mut client_monitor) = fixture.client(Security::curve_client(
        fixture.client.clone(),
        fixture.server_public,
    ));
    fixture.bounce(&mut client);
    assert_eq!(
        handshake_events(&mut fixture.server_monitor, FIVE),
        [EventKind::HandshakeSucceeded]
    );
    assert_eq!(handshake_events(&mut fixture.server_monitor, TIMEOUT), []);
    assert_eq!(
        handshake_events(&mut client_monitor, FIVE),
        [EventKind::HandshakeSucceeded]
    );
    assert_eq!(handshake_events(&mut client_monitor, TIMEOUT), []);
}

/// libzmq runs `test_curve_security_with_valid_credentials` again with a
/// 255-octet server routing id, which ZAP sees, and which makes the
/// handshake's metadata large.
#[test]
fn curve_security_with_valid_credentials_large_routing_id() {
    let large = "0123456789".repeat(26)[..255].to_owned();
    let mut fixture = Fixture::named(Identity::new(large.clone()).unwrap());
    let mut client = fixture.curve_client(fixture.client.clone(), fixture.server_public);
    fixture.bounce(&mut client);
    assert_eq!(
        fixture.zap_identities.recv_timeout(FIVE).unwrap(),
        Frame::from(large)
    );
}

/// A client with a null server key, or a null secret key, gets nowhere.
/// libzmq only logs how many cryptographic failures the server reports.
#[test]
fn null_server_key() {
    let mut fixture = Fixture::new();
    let null = PublicKey::from_z85(NULL_KEY).unwrap();
    let mut client = fixture.curve_client(fixture.client.clone(), null);
    fixture.expect_bounce_fail(&mut client);
}

// `test_null_client_public_key` gives the client a null public key beside a
// valid secret one. A zmq-rs `Keypair` derives its public key from its
// secret, so the two cannot disagree.

#[test]
fn null_client_secret_key() {
    let mut fixture = Fixture::new();
    let null = Keypair::from_secret(SecretKey::from_z85(NULL_KEY).unwrap());
    let mut client = fixture.curve_client(null, fixture.server_public);
    fixture.expect_bounce_fail(&mut client);
}

/// A client whose key the handler does not know: ZAP refuses it with 400,
/// which both sides report.
#[test]
fn curve_security_with_bogus_client_credentials() {
    let mut fixture = Fixture::new();
    let (mut client, mut client_monitor) = fixture.client(Security::curve_client(
        Keypair::generate(),
        fixture.server_public,
    ));
    fixture.expect_bounce_fail(&mut client);
    let denied = EventKind::HandshakeFailed(HandshakeFailure::Authentication { status: 400 });
    assert_eq!(
        handshake_events(&mut client_monitor, FIVE),
        std::slice::from_ref(&denied)
    );
    let server_events = handshake_events(&mut fixture.server_monitor, FIVE);
    assert!(server_events.contains(&denied), "{server_events:?}");
    assert!(fixture.zap_requests.load(Ordering::SeqCst) >= 1);
}

fn is_mechanism_mismatch(event: &EventKind) -> bool {
    matches!(
        event,
        EventKind::HandshakeFailed(HandshakeFailure::Protocol(ProtocolError::Greeting(
            GreetingError::MechanismMismatch { .. }
        )))
    )
}

/// A client speaking another mechanism is refused by the greeting, without
/// asking ZAP.
///
/// libzmq asserts that the server reports the mismatch. But whichever end
/// sees the other's mechanism first closes without sending the rest of
/// its own greeting, in libzmq as here (`stream_engine_base_t::error`
/// drops queued output), so the other end may see only the connection
/// drop. Either way, one end reports the mismatch.
fn expect_zmtp_mechanism_mismatch(security: Security) {
    let mut fixture = Fixture::new();
    let (mut client, mut client_monitor) = fixture.client(security);
    fixture.expect_bounce_fail(&mut client);
    let server = handshake_events(&mut fixture.server_monitor, FIVE);
    let client = handshake_events(&mut client_monitor, FIVE);
    let dropped = EventKind::HandshakeFailed(HandshakeFailure::NoDetail);
    assert!(
        server
            .iter()
            .all(|event| is_mechanism_mismatch(event) || *event == dropped),
        "{server:?}"
    );
    assert!(
        server.iter().chain(&client).any(is_mechanism_mismatch),
        "server {server:?}, client {client:?}"
    );
    assert_eq!(fixture.zap_requests.load(Ordering::SeqCst), 0);
}

#[test]
fn curve_security_with_null_client_credentials() {
    expect_zmtp_mechanism_mismatch(Security::Null);
}

#[test]
fn curve_security_with_plain_client_credentials() {
    expect_zmtp_mechanism_mismatch(Security::PlainClient(PlainCredentials {
        username: Credential::new("admin").unwrap(),
        password: Credential::new("password").unwrap(),
    }));
}

/// Messages from a ZMTP 1.0 peer, which cannot speak CURVE, never arrive.
#[test]
fn curve_security_unauthenticated_message() {
    let mut fixture = Fixture::new();
    let mut stream = fixture.raw_client();
    // An anonymous ZMTP 1.0 greeting, then a sneaky message.
    stream.write_all(b"\x01\x00").unwrap();
    stream.write_all(b"\x08\x00sneaky\0").unwrap();
    assert_eq!(fixture.server.recv_timeout(TIMEOUT), None);
}

fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0; 2];
    stream.read_exact(&mut header).unwrap();
    assert_eq!(header[0] & 0x02, 0, "a short frame");
    let mut body = vec![0; usize::from(header[1])];
    stream.read_exact(&mut body).unwrap();
    body
}

/// Where a command frame's body starts: after a short or a long header.
fn body_start(frame: &[u8]) -> usize {
    if frame[0] & 0x02 == 0 { 2 } else { 9 }
}

/// A valid HELLO from the valid client, framed, and the client that made
/// it.
fn hello(fixture: &Fixture) -> (CurveClient, Vec<u8>) {
    let mut out = Vec::new();
    let client = CurveClient::start(
        zmtp::SocketType::Dealer,
        Identity::default(),
        &fixture.client,
        fixture.server_public,
        &PeerGreeting::Zmtp3 {
            major: 3,
            minor: 0,
            as_server: true,
        },
        &mut out,
    )
    .unwrap();
    (client, out)
}

/// Sends HELLO with `tamper` applied to its body, after the greeting.
fn send_hello(fixture: &Fixture, tamper: impl FnOnce(&mut [u8])) -> (TcpStream, CurveClient) {
    let mut stream = fixture.raw_client();
    stream.write_all(&ZMTP_GREETING_CURVE).unwrap();
    stream.read_exact(&mut [0; 64]).unwrap();
    let (client, mut frame) = hello(fixture);
    let start = body_start(&frame);
    tamper(&mut frame[start..]);
    stream.write_all(&frame).unwrap();
    (stream, client)
}

#[test]
fn curve_security_invalid_hello_wrong_length() {
    let mut fixture = Fixture::new();
    let mut stream = fixture.raw_client();
    stream.write_all(&ZMTP_GREETING_CURVE).unwrap();
    stream.write_all(b"\x04\x06\x05HELLO").unwrap();
    expect_protocol_error(&mut fixture.server_monitor, |e| {
        matches!(e, HandshakeError::MalformedCommand(_))
    });
}

#[test]
fn curve_security_invalid_hello_command_name() {
    let mut fixture = Fixture::new();
    let _stream = send_hello(&fixture, |hello| hello[5] = b'X');
    expect_protocol_error(&mut fixture.server_monitor, |e| {
        matches!(e, HandshakeError::UnexpectedCommand(_))
    });
}

#[test]
fn curve_security_invalid_hello_version() {
    let mut fixture = Fixture::new();
    let _stream = send_hello(&fixture, |hello| hello[6] = 2);
    expect_protocol_error(&mut fixture.server_monitor, |e| {
        matches!(e, HandshakeError::MalformedCommand(_))
    });
}

#[test]
fn curve_security_invalid_initiate_wrong_length() {
    let mut fixture = Fixture::new();
    let (mut stream, _) = send_hello(&fixture, |_| {});
    read_frame(&mut stream);
    assert_eq!(handshake_events(&mut fixture.server_monitor, TIMEOUT), []);
    stream.write_all(b"\x04\x09\x08INITIATE").unwrap();
    expect_protocol_error(&mut fixture.server_monitor, |e| {
        matches!(e, HandshakeError::MalformedCommand(_))
    });
}

/// Completes HELLO and WELCOME, then sends the client's INITIATE with
/// `tamper` applied to its body.
fn send_initiate(fixture: &mut Fixture, tamper: impl FnOnce(&mut [u8])) -> TcpStream {
    let (mut stream, client) = send_hello(fixture, |_| {});
    let welcome = read_frame(&mut stream);
    let mut initiate = Vec::new();
    let step = client
        .receive(Incoming::Command(Frame::from(welcome)), &mut initiate)
        .unwrap();
    assert!(matches!(step, CurveClientStep::Continue(_)));
    assert_eq!(handshake_events(&mut fixture.server_monitor, TIMEOUT), []);
    let start = body_start(&initiate);
    tamper(&mut initiate[start..]);
    stream.write_all(&initiate).unwrap();
    stream
}

#[test]
fn curve_security_invalid_initiate_command_name() {
    let mut fixture = Fixture::new();
    let _stream = send_initiate(&mut fixture, |initiate| initiate[5] = b'X');
    expect_protocol_error(&mut fixture.server_monitor, |e| {
        matches!(e, HandshakeError::UnexpectedCommand(_))
    });
}

#[test]
fn curve_security_invalid_initiate_command_encrypted_cookie() {
    let mut fixture = Fixture::new();
    let _stream = send_initiate(&mut fixture, |initiate| initiate[30] ^= 1);
    expect_protocol_error(&mut fixture.server_monitor, |e| {
        matches!(e, HandshakeError::Cryptographic)
    });
}

#[test]
fn curve_security_invalid_initiate_command_encrypted_content() {
    let mut fixture = Fixture::new();
    let _stream = send_initiate(&mut fixture, |initiate| initiate[150] ^= 1);
    expect_protocol_error(&mut fixture.server_monitor, |e| {
        matches!(e, HandshakeError::Cryptographic)
    });
}

/// libzmq refuses keys of the wrong length with `EINVAL`; zmq-rs's key
/// types refuse them when parsed.
#[test]
fn curve_security_invalid_keysize() {
    let wrong = "0".repeat(123);
    assert!(PublicKey::from_z85(&wrong).is_err());
    assert!(SecretKey::from_z85(&wrong).is_err());
}
