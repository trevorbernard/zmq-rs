//! Greeting, NULL handshake and message exchange between `zmtp` and libzmq.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use zmq_interop::libzmq::{Kind, Socket};
use zmq_interop::{ConnectError, Peer};
use zmtp::{
    Command, Frame, GreetingError, HandshakeError, Identity, LegacyPeers, Mechanism, Message,
    SocketType, Version,
};

fn message(parts: &[&[u8]]) -> Message {
    let frames = parts
        .iter()
        .map(|p| Frame::copy_from_slice(p))
        .collect::<Vec<_>>();
    Message::try_from(frames).unwrap()
}

#[test]
fn dealer_handshakes_with_router_which_routes_by_our_identity() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let client = thread::spawn(move || {
        let mut peer = Peer::connect(addr, SocketType::Dealer, b"rusty").unwrap();
        peer.send(&message(&[b"", b"hi"])).unwrap();
        peer.established
    });
    assert_eq!(
        router.recv().unwrap(),
        [&b"rusty"[..], b"", b"hi"],
        "libzmq routes by the Identity in our READY"
    );
    let established = client.join().unwrap();
    assert_eq!(established.version, Version::V3_1);
    assert_eq!(established.peer_socket_type, Some(SocketType::Router));
    assert_eq!(established.peer_identity, Frame::new());
}

#[test]
fn multipart_messages_round_trip_through_libzmq() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let sizes = [0, 1, 23, 24, 255, 256, 100_000];
    let bodies: Vec<Vec<u8>> = sizes.iter().map(|&n| vec![0xA5; n]).collect();
    let sent = Message::try_from(
        bodies
            .iter()
            .map(|b| Frame::copy_from_slice(b))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let client = thread::spawn({
        let sent = sent.clone();
        move || {
            let mut peer = Peer::connect(addr, SocketType::Dealer, b"").unwrap();
            peer.send(&sent).unwrap();
            peer.recv_message().unwrap()
        }
    });
    let received = router.recv().unwrap();
    assert_eq!(
        received.len(),
        sizes.len() + 1,
        "routing id plus every frame"
    );
    let parts: Vec<&[u8]> = received.iter().map(Vec::as_slice).collect();
    router.send(&parts).unwrap();
    assert_eq!(client.join().unwrap(), sent);
}

#[test]
fn router_learns_libzmq_identity() {
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_routing_id(b"lib");
    let addr = dealer.bind_loopback();
    let client = thread::spawn(move || {
        let mut peer = Peer::connect(addr, SocketType::Router, b"").unwrap();
        let first = peer.recv_message().unwrap();
        (peer.established, first)
    });
    dealer.send(&[b"x"]).unwrap();
    let (established, first) = client.join().unwrap();
    assert_eq!(established.peer_socket_type, Some(SocketType::Dealer));
    assert_eq!(established.peer_identity, Frame::from("lib"));
    assert_eq!(first, message(&[b"x"]));
}

#[test]
fn incompatible_socket_type_is_rejected() {
    let router = Socket::new(Kind::Router);
    router.set_receive_timeout(Duration::from_millis(300));
    let addr = router.bind_loopback();
    let result = Peer::connect(addr, SocketType::Sub, b"");
    assert!(
        matches!(
            result,
            Err(ConnectError::Handshake(HandshakeError::IncompatiblePeer {
                peer: SocketType::Router
            }))
        ),
        "{result:?}"
    );
    assert!(router.recv().is_err(), "libzmq never delivers a message");
}

#[test]
fn mechanism_mismatch_never_completes() {
    let router = Socket::new(Kind::Router);
    router.set_plain_server();
    let addr = router.bind_loopback();
    let null = Mechanism::Null {
        legacy_peers: LegacyPeers::Allow,
    };
    let result = Peer::connect_with(addr, SocketType::Dealer, Identity::default(), null);
    // Whichever side notices first closes the connection.
    assert!(
        matches!(
            result,
            Err(
                ConnectError::Greeting(GreetingError::MechanismMismatch { .. })
                    | ConnectError::Closed
                    | ConnectError::Io(_)
            )
        ),
        "{result:?}"
    );
}

#[test]
fn subscribe_command_reaches_libzmq_xpub() {
    let xpub = Socket::new(Kind::XPub);
    let addr = xpub.bind_loopback();
    let client = thread::spawn(move || {
        let mut peer = Peer::connect(addr, SocketType::Sub, b"").unwrap();
        peer.send_command(&Command::Subscribe(Frame::from("weather")))
            .unwrap();
        peer
    });
    assert_eq!(xpub.recv().unwrap(), [b"\x01weather".to_vec()]);
    drop(client.join().unwrap());
}

#[test]
fn libzmq_pub_filters_by_our_subscription() {
    let publisher = Socket::new(Kind::Pub);
    let addr = publisher.bind_loopback();
    let done = Arc::new(AtomicBool::new(false));
    let client = thread::spawn({
        let done = done.clone();
        move || {
            let mut peer = Peer::connect(addr, SocketType::Sub, b"").unwrap();
            peer.send_command(&Command::Subscribe(Frame::from("w")))
                .unwrap();
            let first = peer.recv_message().unwrap();
            done.store(true, Ordering::Release);
            first
        }
    });
    // libzmq applies the subscription asynchronously, so publish until the
    // subscriber has something. Unsubscribed topics must never arrive.
    for _ in 0..500 {
        if done.load(Ordering::Acquire) {
            break;
        }
        publisher.send(&[b"x: dropped"]).unwrap();
        publisher.send(&[b"w: delivered"]).unwrap();
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(client.join().unwrap(), message(&[b"w: delivered"]));
}
