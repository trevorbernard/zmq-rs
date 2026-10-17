//! The CURVE mechanism from `zmtp` against libzmq's, on the wire. Proves
//! the box layout, nonces, cookie and vouch match libzmq byte for byte.

use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq_interop::libzmq::{Kind, Socket};
use zmq_interop::{ConnectError, Peer, Security};
use zmtp::{
    Frame, HandshakeError, Identity, Keypair, Message, Metadata, PublicKey, SecretKey, SocketType,
    ZapCredentials, ZapFailure, ZapReply,
};

const FIVE: Duration = Duration::from_secs(5);

fn accepted() -> ZapReply {
    ZapReply::Ok {
        user_id: Frame::from("client"),
        metadata: Metadata::new(),
    }
}

fn multipart() -> Message {
    Message::try_from(vec![
        Frame::from("header"),
        Frame::new(),
        Frame::from(vec![0xa5; 100_000]),
    ])
    .unwrap()
}

#[test]
fn our_curve_client_talks_to_a_libzmq_curve_server() {
    let server_keys = Keypair::generate();
    let router = Socket::new(Kind::Router);
    router.set_receive_timeout(FIVE);
    router.set_curve_server(server_keys.secret().expose());
    let addr = router.bind_loopback();

    let mut peer = Peer::connect_with(
        addr,
        SocketType::Dealer,
        Identity::new("me").unwrap(),
        Security::CurveClient {
            keypair: Keypair::generate(),
            server_key: server_keys.public(),
        },
    )
    .unwrap();
    assert_eq!(peer.established.peer_socket_type, Some(SocketType::Router));

    peer.send(&Message::from("hello")).unwrap();
    assert_eq!(router.recv().unwrap(), [&b"me"[..], b"hello"]);
    peer.send(&multipart()).unwrap();
    let received = router.recv().unwrap();
    assert_eq!(received.len(), 4);
    assert_eq!(received[3].len(), 100_000);

    router.send(&[b"me", b"back", &[0x5a; 70_000]]).unwrap();
    let reply = peer.recv_message().unwrap();
    assert_eq!(reply.get(0).unwrap().as_slice(), b"back");
    assert_eq!(reply.get(1).unwrap().len(), 70_000);
}

#[test]
fn a_libzmq_curve_client_talks_to_our_curve_server() {
    // The client keys from libzmq's zmq_curve(7), the server's generated.
    let client_keys = Keypair::from_secret(
        SecretKey::from_z85("D:)Q[IlAW!ahhC2ac:9*A}h:p?([4%wOTJ%JR%cs").unwrap(),
    );
    let client_public = client_keys.public();
    assert_eq!(
        client_public,
        PublicKey::from_z85("Yne@$w-vo<fVvi]a<NY6T1ed:M$fCG*[IaLV{hID").unwrap()
    );
    let server_keys = Keypair::generate();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_receive_timeout(FIVE);
    dealer.set_routing_id(b"libzmq");
    dealer.set_curve_client(
        client_public.as_bytes(),
        client_keys.secret().expose(),
        server_keys.public().as_bytes(),
    );
    dealer.connect(listener.local_addr().unwrap());

    let (seen, credentials) = mpsc::channel();
    let mut peer = Peer::accept(
        &listener,
        SocketType::Router,
        Identity::default(),
        Security::CurveServer {
            keypair: server_keys,
            authenticate: Box::new(move |request| {
                let _ = seen.send(request.credentials.clone());
                accepted()
            }),
        },
    )
    .unwrap();
    assert_eq!(
        credentials.recv_timeout(FIVE).unwrap(),
        ZapCredentials::Curve(client_public)
    );
    assert_eq!(peer.established.peer_identity, Frame::from("libzmq"));

    dealer.send(&[b"hi", &[1; 50_000]]).unwrap();
    let message = peer.recv_message().unwrap();
    assert_eq!(message.get(0).unwrap().as_slice(), b"hi");
    assert_eq!(message.get(1).unwrap().len(), 50_000);
    peer.send(&multipart()).unwrap();
    let received = dealer.recv().unwrap();
    assert_eq!(received.len(), 3);
    assert_eq!(received[2].len(), 100_000);
}

#[test]
fn a_client_with_the_wrong_server_key_is_refused() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client_keys = Keypair::generate();
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_curve_client(
        client_keys.public().as_bytes(),
        client_keys.secret().expose(),
        Keypair::generate().public().as_bytes(),
    );
    dealer.connect(listener.local_addr().unwrap());
    let result = Peer::accept(
        &listener,
        SocketType::Router,
        Identity::default(),
        Security::CurveServer {
            keypair: Keypair::generate(),
            authenticate: Box::new(|_| accepted()),
        },
    );
    assert!(
        matches!(
            result,
            Err(ConnectError::Handshake(HandshakeError::Cryptographic))
        ),
        "{result:?}"
    );
}

#[test]
fn a_denied_libzmq_client_never_gets_through() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let server_keys = Keypair::generate();
    let client_keys = Keypair::generate();
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_curve_client(
        client_keys.public().as_bytes(),
        client_keys.secret().expose(),
        server_keys.public().as_bytes(),
    );
    dealer.connect(listener.local_addr().unwrap());
    let result = Peer::accept(
        &listener,
        SocketType::Router,
        Identity::default(),
        Security::CurveServer {
            keypair: server_keys,
            authenticate: Box::new(|_| ZapReply::AuthenticationFailure),
        },
    );
    assert!(
        matches!(
            result,
            Err(ConnectError::Handshake(HandshakeError::Authentication(
                ZapFailure::Denied
            )))
        ),
        "{result:?}"
    );
    thread::sleep(Duration::from_millis(50));
}

#[test]
fn libzmq_refuses_our_client_with_the_wrong_server_key() {
    let router = Socket::new(Kind::Router);
    router.set_curve_server(Keypair::generate().secret().expose());
    let addr = router.bind_loopback();
    let result = Peer::connect_with(
        addr,
        SocketType::Dealer,
        Identity::default(),
        Security::CurveClient {
            keypair: Keypair::generate(),
            server_key: Keypair::generate().public(),
        },
    );
    // libzmq closes without a word after a HELLO it cannot open.
    assert!(result.is_err(), "{result:?}");
}
