//! libzmq's `tests/test_security_plain.cpp`: PLAIN security, with ZAP
//! checking the username and password.

use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc;
use std::time::Duration;

use zmq::{
    Context, Credential, Dealer, Frame, Identity, Linger, Message, Metadata, PlainCredentials,
    Security, Socket, SocketOptions, ZapCredentials, ZapReply, ZapRequest,
};

use crate::support::{FIVE, SETTLE};

const CONTENT: &str = "12345678ABCDEFGH12345678abcdefgh";
const DOMAIN: &str = "test";

fn content() -> Message {
    let mut message = Message::new(CONTENT);
    message.push(CONTENT);
    message
}

/// Closed with zero linger, as libzmq's tests close them, so a message
/// queued for a peer that never completes its handshake does not hold up
/// the context's drop.
fn zero_linger() -> SocketOptions {
    SocketOptions {
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    }
}

/// A message from client to server and back, as libzmq's `bounce`.
fn bounce(server: &mut Socket<Dealer>, client: &mut Socket<Dealer>) {
    client.send(content());
    assert_eq!(server.recv_timeout(FIVE), Some(content()));
    server.send(content());
    assert_eq!(client.recv_timeout(FIVE), Some(content()));
}

/// Neither direction gets through, as libzmq's `expect_bounce_fail`.
fn expect_bounce_fail(server: &mut Socket<Dealer>, client: &mut Socket<Dealer>) {
    let _ = client.try_send(content());
    assert_eq!(server.recv_timeout(SETTLE), None);
    let _ = server.try_send(content());
    assert_eq!(client.recv_timeout(SETTLE), None);
}

/// libzmq's test handler: admin and password get in.
fn authenticate(request: &ZapRequest) -> ZapReply {
    match &request.credentials {
        ZapCredentials::Plain(PlainCredentials { username, password })
            if username.expose() == b"admin" && password.expose() == b"password" =>
        {
            ZapReply::Ok {
                user_id: Frame::from("anonymous"),
                metadata: Metadata::new(),
            }
        }
        _ => ZapReply::AuthenticationFailure,
    }
}

/// The PLAIN server, named IDENT, bound to TCP. Each ZAP request goes to
/// the returned receiver.
fn set_up(context: &Context) -> (Socket<Dealer>, String, mpsc::Receiver<ZapRequest>) {
    let (seen, requests) = mpsc::channel();
    let mut server = context.socket_with::<Dealer>(SocketOptions {
        identity: Identity::new("IDENT").unwrap(),
        security: Security::plain_server(DOMAIN, move |request: &ZapRequest| {
            let _ = seen.send(request.clone());
            authenticate(request)
        }),
        ..zero_linger()
    });
    let endpoint = server.bind("tcp://127.0.0.1:*").unwrap().to_string();
    (server, endpoint, requests)
}

fn plain_client(context: &Context, username: &str, password: &str) -> Socket<Dealer> {
    context.socket_with::<Dealer>(SocketOptions {
        security: Security::PlainClient(PlainCredentials {
            username: Credential::new(username).unwrap(),
            password: Credential::new(password).unwrap(),
        }),
        ..zero_linger()
    })
}

#[test]
fn plain_success() {
    let context = Context::new().unwrap();
    let (mut server, endpoint, requests) = set_up(&context);
    let mut client = plain_client(&context, "admin", "password");
    client.connect(&endpoint).unwrap();
    bounce(&mut server, &mut client);

    let request = requests.recv_timeout(FIVE).unwrap();
    assert_eq!(request.domain, Frame::from(DOMAIN));
    assert_eq!(request.identity, Frame::from("IDENT"));
    assert_eq!(request.credentials.mechanism(), "PLAIN");
}

/// A client configured as a PLAIN server is refused by the server's
/// handshake, without asking ZAP.
#[test]
fn plain_client_as_server_fails() {
    let context = Context::new().unwrap();
    let (mut server, endpoint, requests) = set_up(&context);
    let mut client = context.socket_with::<Dealer>(SocketOptions {
        security: Security::plain_server(DOMAIN, authenticate),
        ..zero_linger()
    });
    client.connect(&endpoint).unwrap();
    expect_bounce_fail(&mut server, &mut client);
    assert!(requests.try_recv().is_err(), "ZAP was asked");
}

#[test]
fn plain_wrong_credentials_fails() {
    let context = Context::new().unwrap();
    let (mut server, endpoint, _requests) = set_up(&context);
    let mut client = plain_client(&context, "wronguser", "wrongpass");
    client.connect(&endpoint).unwrap();
    expect_bounce_fail(&mut server, &mut client);
}

/// Messages from a ZMTP 1.0 peer, which cannot speak PLAIN, never arrive.
#[test]
fn plain_vanilla_socket() {
    let context = Context::new().unwrap();
    let (mut server, endpoint, _requests) = set_up(&context);
    let mut stream = TcpStream::connect(endpoint.trim_start_matches("tcp://")).unwrap();
    // An anonymous ZMTP 1.0 greeting, then a sneaky message.
    stream.write_all(b"\x01\x00").unwrap();
    stream.write_all(b"\x08\x00sneaky\0").unwrap();
    assert_eq!(server.recv_timeout(Duration::from_millis(250)), None);
}
