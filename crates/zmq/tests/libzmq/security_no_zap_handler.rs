//! libzmq's `tests/test_security_no_zap_handler.cpp`: a NULL socket with a
//! ZAP domain but no ZAP handler.
//!
//! In zmq-rs a socket asks ZAP only when given an authenticator, so a ZAP
//! domain cannot exist without a handler. libzmq ignores a domain whose
//! handler is missing, which is zmq-rs's plain NULL; with
//! `ZMQ_ZAP_ENFORCE_DOMAIN`, it refuses every peer, which is zmq-rs's
//! authenticator that never answers.

use std::time::Duration;

use zmq::{
    Authenticator, Context, Dealer, Linger, Message, Security, Socket, SocketOptions, ZapRequest,
    ZapResponder,
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

/// Client and server over TCP, the server with `security`.
fn pair(context: &Context, security: Security) -> (Socket<Dealer>, Socket<Dealer>) {
    let mut server = context.socket_with::<Dealer>(SocketOptions {
        security,
        ..zero_linger()
    });
    let endpoint = server.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut client = context.socket_with::<Dealer>(zero_linger());
    client.connect(&endpoint).unwrap();
    (server, client)
}

/// Answers nothing: dropping `respond` unanswered rejects the peer.
struct NoHandler;

impl Authenticator for NoHandler {
    fn authenticate(&self, _request: ZapRequest, _respond: ZapResponder) {}
}

#[test]
fn no_zap_handler() {
    let context = Context::new().unwrap();
    let (mut server, mut client) = pair(&context, Security::Null);
    client.send(content());
    assert_eq!(server.recv_timeout(FIVE), Some(content()));
    server.send(content());
    assert_eq!(client.recv_timeout(FIVE), Some(content()));
}

#[test]
fn no_zap_handler_enforce_domain() {
    let context = Context::new().unwrap();
    let (mut server, mut client) = pair(&context, Security::null_authenticated("TEST", NoHandler));
    let _ = client.try_send(content());
    assert_eq!(server.recv_timeout(SETTLE), None);
    let _ = server.try_send(content());
    assert_eq!(client.recv_timeout(SETTLE), None);
}
