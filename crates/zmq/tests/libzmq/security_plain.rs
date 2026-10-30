//! libzmq's `tests/test_security_plain.cpp`: PLAIN security, with ZAP
//! checking the username and password.

use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc;
use std::time::Duration;

use zmq::{
    Context, Credential, Dealer, Frame, Identity, Metadata, PlainCredentials, Security, Socket,
    SocketOptions, ZapCredentials, ZapReply, ZapRequest,
};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, SETTLE, bounce_content, bounce_dealer, zero_linger};

const DOMAIN: &str = "test";

/// Neither direction gets through, as libzmq's `expect_bounce_fail`.
fn expect_bounce_fail(server: &mut Socket<Dealer>, client: &mut Socket<Dealer>) {
    let _ = client.try_send(bounce_content());
    assert_eq!(server.recv_timeout(SETTLE), None);
    let _ = server.try_send(bounce_content());
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
    let endpoint = server.bind(&free_endpoint()).unwrap().to_string();
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
    bounce_dealer(&mut server, &mut client);

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
