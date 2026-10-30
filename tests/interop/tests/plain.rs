//! The PLAIN mechanism and ZAP, in both directions, against libzmq.

use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;

use zmq_interop::libzmq::{Context, Kind, Socket, zap_handler};
use zmq_interop::{ConnectError, Peer, Security};
use zmq_test_ports::free_port;
use zmtp::{
    Credential, ErrorReason, Frame, HandshakeError, Identity, Message, Metadata, PlainCredentials,
    SocketType, ZapCredentials, ZapFailure, ZapReply,
};

fn credentials(password: &str) -> PlainCredentials {
    PlainCredentials {
        username: Credential::new("admin").unwrap(),
        password: Credential::new(password.to_owned()).unwrap(),
    }
}

fn identity(id: &str) -> Identity {
    Identity::new(id.to_owned()).unwrap()
}

/// A libzmq ROUTER acting as PLAIN server in domain `global`, whose ZAP
/// handler answers `status`.
fn libzmq_server(
    status: &'static str,
) -> (
    Socket,
    std::net::SocketAddr,
    thread::JoinHandle<Vec<Vec<u8>>>,
) {
    let context = Context::new();
    let zap = zap_handler(&context, status, "user-1");
    let router = Socket::in_context(&context, Kind::Router);
    router.set_plain_server();
    router.set_zap_domain(b"global");
    let addr = router.bind_loopback();
    (router, addr, zap)
}

#[test]
fn our_client_authenticates_with_libzmq_server() {
    let (router, addr, zap) = libzmq_server("200");
    let client = thread::spawn(move || {
        let security = Security::PlainClient(credentials("hunter2"));
        let mut peer =
            Peer::connect_with(addr, SocketType::Dealer, identity("rusty"), security).unwrap();
        peer.send(&Message::try_from(vec![Frame::new(), Frame::from("hi")]).unwrap())
            .unwrap();
        peer.established
    });
    assert_eq!(router.recv().unwrap(), [&b"rusty"[..], b"", b"hi"]);
    let established = client.join().unwrap();
    assert_eq!(established.peer_socket_type, Some(SocketType::Router));
    assert_eq!(
        established.authenticated, None,
        "clients are not authenticated by ZAP"
    );

    let request = zap.join().unwrap();
    let request: Vec<&[u8]> = request.iter().map(Vec::as_slice).collect();
    assert_eq!(
        request,
        [
            &b"1.0"[..],
            b"1",
            b"global",
            b"127.0.0.1",
            b"",
            b"PLAIN",
            b"admin",
            b"hunter2"
        ],
        "RFC 27 request from libzmq, carrying our HELLO credentials"
    );
}

#[test]
fn our_client_is_rejected_by_libzmq_server() {
    let (_router, addr, zap) = libzmq_server("400");
    let security = Security::PlainClient(credentials("wrong"));
    let result = Peer::connect_with(addr, SocketType::Dealer, identity("rusty"), security);
    let expected = HandshakeError::PeerError(ErrorReason::new("400").unwrap());
    assert!(
        matches!(&result, Err(ConnectError::Handshake(err)) if *err == expected),
        "{result:?}"
    );
    zap.join().unwrap();
}

#[test]
fn libzmq_client_authenticates_with_our_server() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_plain_client(b"admin", b"hunter2");
    dealer.set_routing_id(b"lib");
    dealer.connect(listener.local_addr().unwrap());
    dealer.send(&[b"hello"]).unwrap();

    let (requests, seen) = mpsc::channel();
    let security = Security::PlainServer {
        domain: Frame::from("global"),
        authenticate: Box::new(move |request| {
            requests.send(request.clone()).unwrap();
            ZapReply::Ok {
                user_id: Frame::from("user-1"),
                metadata: Metadata::new(),
            }
        }),
    };
    let mut peer = Peer::accept(&listener, SocketType::Router, identity("srv"), security).unwrap();

    let request = seen.recv().unwrap();
    assert_eq!(request.domain, Frame::from("global"));
    assert_eq!(request.address, "127.0.0.1");
    assert_eq!(request.identity, Frame::from("srv"));
    assert_eq!(
        request.credentials,
        ZapCredentials::Plain(credentials("hunter2"))
    );

    let established = &peer.established;
    assert_eq!(established.peer_socket_type, Some(SocketType::Dealer));
    assert_eq!(established.peer_identity, Frame::from("lib"));
    assert_eq!(
        established.authenticated.as_ref().map(|a| &a.user_id),
        Some(&Frame::from("user-1"))
    );
    assert_eq!(peer.recv_message().unwrap(), Message::new("hello"));
}

#[test]
fn our_server_rejects_libzmq_client() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_plain_client(b"admin", b"wrong");
    dealer.connect(listener.local_addr().unwrap());

    let security = Security::PlainServer {
        domain: Frame::new(),
        authenticate: Box::new(|_| ZapReply::AuthenticationFailure),
    };
    let result = Peer::accept(&listener, SocketType::Router, identity("srv"), security);
    assert!(
        matches!(
            result,
            Err(ConnectError::Handshake(HandshakeError::Authentication(
                ZapFailure::Denied
            )))
        ),
        "{result:?}"
    );
}
