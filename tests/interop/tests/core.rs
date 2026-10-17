//! zmq-core connections against libzmq and against a zmtp peer.

use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq_core::{Connection, ConnectionConfig, Context};
use zmq_interop::libzmq::{Kind, Socket};
use zmq_interop::{ConnectError, Peer, Security};
use zmq_pipe::Hwm;
use zmtp::{Frame, Identity, Message, SocketType};

/// Runs `f` on another thread and fails the test if it takes longer than
/// `limit`, so a lost wakeup fails instead of hanging the suite.
fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
        .recv_timeout(limit)
        .unwrap_or_else(|_| panic!("timed out after {limit:?}"))
}

fn message(parts: &[&[u8]]) -> Message {
    let frames = parts
        .iter()
        .map(|p| Frame::copy_from_slice(p))
        .collect::<Vec<_>>();
    Message::try_from(frames).unwrap()
}

fn dealer(identity: &str) -> ConnectionConfig {
    ConnectionConfig {
        identity: Identity::new(identity.to_owned()).unwrap(),
        ..ConnectionConfig::new(SocketType::Dealer)
    }
}

fn recv(connection: Connection) -> (Connection, Option<Message>) {
    within(Duration::from_secs(5), move || {
        let mut connection = connection;
        let message = connection.rx.recv_blocking();
        (connection, message)
    })
}

#[test]
fn defaults_match_libzmq() {
    let config = ConnectionConfig::new(SocketType::Dealer);
    let thousand = Hwm::Messages(NonZeroUsize::new(1000).unwrap());
    assert_eq!(config.send_hwm, thousand);
    assert_eq!(config.recv_hwm, thousand);
    assert_eq!(config.identity, Identity::default());
    assert_eq!(config.max_frame_size, None);
}

#[test]
fn exchanges_messages_with_libzmq_router() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let mut connection = context.connect(addr, dealer("core"));

    connection.tx.send_blocking(message(&[b"", b"hi"])).unwrap();
    assert_eq!(router.recv().unwrap(), [&b"core"[..], b"", b"hi"]);

    router.send(&[b"core", b"", b"back"]).unwrap();
    let (_connection, reply) = recv(connection);
    assert_eq!(reply, Some(message(&[b"", b"back"])));
}

#[test]
fn messages_sent_before_the_handshake_arrive_in_order() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let mut connection = context.connect(addr, dealer("early"));
    for i in 0..100u32 {
        connection
            .tx
            .try_send(message(&[&i.to_be_bytes()]))
            .unwrap();
    }
    for i in 0..100u32 {
        assert_eq!(router.recv().unwrap(), [&b"early"[..], &i.to_be_bytes()]);
    }
}

#[test]
fn large_messages_round_trip() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let mut connection = context.connect(addr, dealer("big"));
    let big = vec![0x5A; 4 << 20];

    connection.tx.send_blocking(message(&[b"", &big])).unwrap();
    let received = router.recv().unwrap();
    assert_eq!(received.len(), 3);
    assert_eq!(received[2], big);

    let parts: Vec<&[u8]> = received.iter().map(Vec::as_slice).collect();
    router.send(&parts).unwrap();
    let (_connection, echoed) = recv(connection);
    assert_eq!(echoed, Some(message(&[b"", &big])));
}

#[test]
fn a_full_receive_pipe_applies_backpressure_without_loss() {
    // A libzmq DEALER blocks at its HWM rather than dropping (a ROUTER would
    // drop), so every message must arrive while our slow reader holds the
    // engine back with a receive HWM of 4.
    let libzmq = Socket::new(Kind::Dealer);
    let addr = libzmq.bind_loopback();
    let context = Context::new().unwrap();
    let config = ConnectionConfig {
        recv_hwm: Hwm::Messages(NonZeroUsize::new(4).unwrap()),
        ..dealer("slow")
    };
    let mut connection = context.connect(addr, config);
    let total = 20_000u32;
    let sender = thread::spawn(move || {
        for i in 0..total {
            libzmq.send(&[&i.to_be_bytes()]).unwrap();
        }
        libzmq
    });
    let received = within(Duration::from_secs(20), move || {
        let mut received = Vec::with_capacity(total as usize);
        while received.len() < total as usize {
            if received.len() % 100 == 0 {
                thread::sleep(Duration::from_millis(1));
            }
            received.push(connection.rx.recv_blocking().expect("open"));
        }
        received
    });
    let _libzmq = sender.join().unwrap();
    for (i, msg) in received.iter().enumerate() {
        assert_eq!(*msg, message(&[&(i as u32).to_be_bytes()]), "message {i}");
    }
}

#[test]
fn peer_disconnect_closes_the_pipes() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let mut connection = context.connect(addr, dealer("gone"));
    connection.tx.send_blocking(message(&[b"hi"])).unwrap();
    router.recv().unwrap();
    drop(router);
    let (connection, next) = recv(connection);
    assert_eq!(next, None, "receive pipe closed");
    assert!(connection.tx.is_closed(), "send pipe closed");
}

#[test]
fn dropping_the_connection_closes_the_tcp_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let context = Context::new().unwrap();
    let connection = context.connect(listener.local_addr().unwrap(), dealer("bye"));
    let mut peer = Peer::accept(
        &listener,
        SocketType::Router,
        Identity::default(),
        Security::Null,
    )
    .unwrap();
    assert_eq!(peer.established.peer_identity, Frame::from("bye"));
    drop(connection);
    let result = within(Duration::from_secs(5), move || peer.recv());
    assert!(matches!(result, Err(ConnectError::Closed)), "{result:?}");
}

#[test]
fn incompatible_peer_closes_the_pipes() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let connection = context.connect(addr, ConnectionConfig::new(SocketType::Sub));
    let (_connection, next) = recv(connection);
    assert_eq!(next, None);
}

#[test]
fn refused_connection_closes_the_pipes() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    // The listener is dropped, so nothing accepts on `addr`.
    let context = Context::new().unwrap();
    let connection = context.connect(addr, dealer("nobody"));
    let (_connection, next) = recv(connection);
    assert_eq!(next, None);
}

#[test]
fn connections_spread_over_io_threads() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::with_io_threads(NonZeroUsize::new(3).unwrap()).unwrap();
    let mut connections: Vec<Connection> = (0..6)
        .map(|i| context.connect(addr, dealer(&format!("c{i}"))))
        .collect();
    for (i, connection) in connections.iter_mut().enumerate() {
        connection
            .tx
            .send_blocking(message(&[format!("from c{i}").as_bytes()]))
            .unwrap();
    }
    let mut seen: Vec<Vec<Vec<u8>>> = (0..6).map(|_| router.recv().unwrap()).collect();
    seen.sort();
    for (i, parts) in seen.iter().enumerate() {
        assert_eq!(parts[0], format!("c{i}").as_bytes());
        assert_eq!(parts[1], format!("from c{i}").as_bytes());
    }
}

#[test]
fn dropping_the_context_stops_threads_and_closes_connections() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let mut connection = context.connect(addr, dealer("ctx"));
    connection.tx.send_blocking(message(&[b"hi"])).unwrap();
    router.recv().unwrap();
    within(Duration::from_secs(5), move || drop(context));
    let (_connection, next) = recv(connection);
    assert_eq!(next, None);
}
