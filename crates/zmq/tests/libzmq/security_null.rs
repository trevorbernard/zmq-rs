//! libzmq's `tests/test_security_null.cpp`: NULL security, with and
//! without ZAP. libzmq enables ZAP for NULL by giving the socket a ZAP
//! domain; zmq-rs by giving it an authenticator, with
//! `Security::null_authenticated`.

use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc;
use std::time::Duration;

use zmq::{
    Context, Dealer, Frame, Linger, Message, Metadata, Security, Socket, SocketOptions,
    ZapCredentials, ZapReply, ZapRequest,
};

use crate::support::{FIVE, SETTLE};

const CONTENT: &str = "12345678ABCDEFGH12345678abcdefgh";

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

/// A server authenticating NULL peers in `domain` with libzmq's test
/// handler, which accepts only the domain "TEST". Each request goes to
/// the returned receiver.
fn server_in_domain(
    context: &Context,
    domain: &'static str,
) -> (Socket<Dealer>, mpsc::Receiver<ZapRequest>) {
    let (seen, requests) = mpsc::channel();
    let handler = move |request: &ZapRequest| {
        let _ = seen.send(request.clone());
        if request.domain == Frame::from("TEST") {
            ZapReply::Ok {
                user_id: Frame::from("anonymous"),
                metadata: Metadata::new(),
            }
        } else {
            ZapReply::AuthenticationFailure
        }
    };
    let server = context.socket_with::<Dealer>(SocketOptions {
        security: Security::null_authenticated(domain, handler),
        ..zero_linger()
    });
    (server, requests)
}

fn connected_client(context: &Context, server: &mut Socket<Dealer>) -> Socket<Dealer> {
    let endpoint = server.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut client = context.socket_with::<Dealer>(zero_linger());
    client.connect(&endpoint).unwrap();
    client
}

/// Without a ZAP domain the handler is not asked, and the connection
/// succeeds.
#[test]
fn no_domain() {
    let context = Context::new().unwrap();
    let mut server = context.socket_with::<Dealer>(zero_linger());
    let mut client = connected_client(&context, &mut server);
    bounce(&mut server, &mut client);
}

#[test]
fn wrong_domain_fails() {
    let context = Context::new().unwrap();
    let (mut server, requests) = server_in_domain(&context, "WRONG");
    let mut client = connected_client(&context, &mut server);
    expect_bounce_fail(&mut server, &mut client);
    let request = requests.recv_timeout(FIVE).unwrap();
    assert_eq!(request.credentials, ZapCredentials::Null);
}

#[test]
fn success() {
    let context = Context::new().unwrap();
    let (mut server, requests) = server_in_domain(&context, "TEST");
    let mut client = connected_client(&context, &mut server);
    bounce(&mut server, &mut client);
    let request = requests.recv_timeout(FIVE).unwrap();
    assert_eq!(request.credentials, ZapCredentials::Null);
    assert_eq!(request.credentials.mechanism(), "NULL");
}

/// Messages from a ZMTP 1.0 peer, which has no handshake to authenticate,
/// never reach a socket that requires ZAP.
#[test]
fn vanilla_socket() {
    let context = Context::new().unwrap();
    let (mut server, _requests) = server_in_domain(&context, "WRONG");
    let endpoint = server.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut stream = TcpStream::connect(endpoint.trim_start_matches("tcp://")).unwrap();
    // An anonymous ZMTP 1.0 greeting, then a sneaky message.
    stream.write_all(b"\x01\x00").unwrap();
    stream.write_all(b"\x08\x00sneaky\0").unwrap();
    assert_eq!(server.recv_timeout(Duration::from_millis(250)), None);
}
