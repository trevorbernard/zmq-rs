//! libzmq's `tests/test_security_no_zap_handler.cpp`: a NULL socket with a
//! ZAP domain but no ZAP handler.
//!
//! In zmq-rs a socket asks ZAP only when given an authenticator, so a ZAP
//! domain cannot exist without a handler. libzmq ignores a domain whose
//! handler is missing, which is zmq-rs's plain NULL; with
//! `ZMQ_ZAP_ENFORCE_DOMAIN`, it refuses every peer, which is zmq-rs's
//! authenticator that never answers.

use zmq::{
    Authenticator, Context, Dealer, Security, Socket, SocketOptions, ZapRequest, ZapResponder,
};

use crate::support::{FIVE, SETTLE, bounce_content, zero_linger};

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
    client.send(bounce_content());
    assert_eq!(server.recv_timeout(FIVE), Some(bounce_content()));
    server.send(bounce_content());
    assert_eq!(client.recv_timeout(FIVE), Some(bounce_content()));
}

#[test]
fn no_zap_handler_enforce_domain() {
    let context = Context::new().unwrap();
    let (mut server, mut client) = pair(&context, Security::null_authenticated("TEST", NoHandler));
    let _ = client.try_send(bounce_content());
    assert_eq!(server.recv_timeout(SETTLE), None);
    let _ = server.try_send(bounce_content());
    assert_eq!(client.recv_timeout(SETTLE), None);
}
