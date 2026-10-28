//! zmq-core connections against libzmq and against a zmtp peer.

use std::net::{SocketAddr, TcpListener};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq_core::{Connection, ConnectionConfig, Context, Linger, Reconnect, ShutdownTimedOut};
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
    assert_eq!(config.max_message_size, None);
    assert_eq!(config.linger, Linger::Forever);
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
    let config = ConnectionConfig {
        reconnect: Reconnect::Never,
        ..dealer("gone")
    };
    let mut connection = context.connect(addr, config);
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
    // Closing with the peer's READY still unread would reset the stream, so
    // the connection reads past it first.
    peer.send(&message(&[b"read"])).unwrap();
    let (connection, read) = recv(connection);
    assert_eq!(read, Some(message(&[b"read"])));
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
    let config = ConnectionConfig {
        reconnect: Reconnect::Never,
        ..dealer("nobody")
    };
    let connection = context.connect(addr, config);
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

#[test]
fn reconnects_after_the_peer_restarts_and_delivers_queued_messages() {
    let first = Socket::new(Kind::Router);
    let addr = first.bind_loopback();
    let context = Context::new().unwrap();
    let mut connection = context.connect(addr, dealer("back"));
    connection.tx.send_blocking(message(&[b"before"])).unwrap();
    assert_eq!(first.recv().unwrap(), [&b"back"[..], b"before"]);

    drop(first);
    thread::sleep(Duration::from_millis(50));
    // Sent while the peer is down: queued in the session's pipe.
    connection.tx.send_blocking(message(&[b"during"])).unwrap();
    assert!(
        !connection.tx.is_closed(),
        "the session outlives the connection"
    );

    let second = Socket::new(Kind::Router);
    second.bind(&format!("tcp://{addr}"));
    assert_eq!(second.recv().unwrap(), [&b"back"[..], b"during"]);
    second.send(&[b"back", b"welcome back"]).unwrap();
    let (_connection, reply) = recv(connection);
    assert_eq!(reply, Some(message(&[b"welcome back"])));
}

#[test]
fn keeps_retrying_until_a_peer_listens() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let context = Context::new().unwrap();
    let config = ConnectionConfig {
        reconnect: Reconnect::Backoff {
            initial: Duration::from_millis(20),
            max: Duration::from_millis(100),
        },
        ..dealer("patient")
    };
    let mut connection = context.connect(addr, config);
    connection.tx.send_blocking(message(&[b"anyone?"])).unwrap();
    thread::sleep(Duration::from_millis(300));

    let router = Socket::new(Kind::Router);
    router.bind(&format!("tcp://{addr}"));
    assert_eq!(router.recv().unwrap(), [&b"patient"[..], b"anyone?"]);
}

#[test]
fn protocol_errors_do_not_reconnect() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    // The default policy reconnects, but an incompatible peer is a protocol
    // error, which ends the connection as in libzmq.
    let connection = context.connect(addr, ConnectionConfig::new(SocketType::Sub));
    let (connection, next) = recv(connection);
    assert_eq!(next, None);
    assert!(connection.tx.is_closed());
}

/// A port with nothing listening on it yet, below the ephemeral range so no
/// outgoing connection can take it while the test waits.
fn free_port() -> SocketAddr {
    static NEXT: AtomicU16 = AtomicU16::new(20000);
    loop {
        let port = NEXT.fetch_add(1, Ordering::Relaxed);
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) {
            return listener.local_addr().unwrap();
        }
    }
}

fn eager(identity: &str) -> ConnectionConfig {
    ConnectionConfig {
        reconnect: Reconnect::Backoff {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(50),
        },
        ..dealer(identity)
    }
}

#[test]
fn a_dropped_connection_lingers_until_its_messages_are_delivered() {
    let addr = free_port();
    let context = Context::new().unwrap();
    let mut connection = context.connect(addr, eager("linger"));
    for part in [&b"one"[..], b"two", b"three"] {
        connection.tx.send_blocking(message(&[part])).unwrap();
    }
    drop(connection);
    thread::sleep(Duration::from_millis(200));

    let router = Socket::new(Kind::Router);
    router.bind(&format!("tcp://{addr}"));
    for part in [&b"one"[..], b"two", b"three"] {
        assert_eq!(router.recv().unwrap(), [&b"linger"[..], part]);
    }
}

#[test]
fn zero_linger_discards_queued_messages() {
    let addr = free_port();
    let context = Context::new().unwrap();
    let config = ConnectionConfig {
        linger: Linger::For(Duration::ZERO),
        ..eager("discard")
    };
    let mut connection = context.connect(addr, config);
    connection.tx.send_blocking(message(&[b"lost"])).unwrap();
    drop(connection);
    thread::sleep(Duration::from_millis(100));

    let router = Socket::new(Kind::Router);
    router.set_receive_timeout(Duration::from_millis(500));
    router.bind(&format!("tcp://{addr}"));
    assert!(router.recv().is_err(), "nothing is delivered");
}

#[test]
fn linger_gives_up_at_its_deadline() {
    let addr = free_port();
    let context = Context::new().unwrap();
    let config = ConnectionConfig {
        linger: Linger::For(Duration::from_millis(100)),
        ..eager("deadline")
    };
    let mut connection = context.connect(addr, config);
    connection.tx.send_blocking(message(&[b"late"])).unwrap();
    drop(connection);
    thread::sleep(Duration::from_millis(400));

    let router = Socket::new(Kind::Router);
    router.set_receive_timeout(Duration::from_millis(500));
    router.bind(&format!("tcp://{addr}"));
    assert!(
        router.recv().is_err(),
        "the connection gave up before the peer appeared"
    );
}

#[test]
fn dropping_only_the_receiver_keeps_the_connection_sending() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let Connection { mut tx, rx, .. } = context.connect(addr, dealer("half"));
    drop(rx);
    tx.send_blocking(message(&[b"first"])).unwrap();
    assert_eq!(router.recv().unwrap(), [&b"half"[..], b"first"]);
    // Replies go nowhere, but do not break the connection.
    router.send(&[b"half", b"ignored"]).unwrap();
    thread::sleep(Duration::from_millis(50));
    tx.send_blocking(message(&[b"second"])).unwrap();
    assert_eq!(router.recv().unwrap(), [&b"half"[..], b"second"]);
}

#[test]
fn shutdown_delivers_queued_messages_first() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let mut connection = context.connect(addr, dealer("drain"));
    let total = 1_000u32;
    for i in 0..total {
        connection
            .tx
            .send_blocking(message(&[&i.to_be_bytes()]))
            .unwrap();
    }
    let result = within(Duration::from_secs(10), move || {
        context.shutdown(Duration::from_secs(5))
    });
    assert_eq!(result, Ok(()));
    for i in 0..total {
        assert_eq!(router.recv().unwrap(), [&b"drain"[..], &i.to_be_bytes()]);
    }
    let (_connection, next) = recv(connection);
    assert_eq!(next, None, "shutdown closed the connection");
}

#[test]
fn shutdown_reports_what_it_had_to_abandon() {
    let context = Context::new().unwrap();
    let mut connection = context.connect(free_port(), eager("stuck"));
    connection
        .tx
        .send_blocking(message(&[b"undeliverable"]))
        .unwrap();
    let started = std::time::Instant::now();
    let result = within(Duration::from_secs(5), move || {
        context.shutdown(Duration::from_millis(200))
    });
    assert_eq!(result, Err(ShutdownTimedOut { abandoned: 1 }));
    assert!(started.elapsed() >= Duration::from_millis(200));
}

#[test]
fn concurrent_shutdowns_each_report_what_was_abandoned() {
    let context = Context::new().unwrap();
    let mut connection = context.connect(free_port(), eager("stuck"));
    connection
        .tx
        .send_blocking(message(&[b"undeliverable"]))
        .unwrap();
    let other = context.clone();
    let first = thread::spawn(move || other.shutdown(Duration::from_millis(200)));
    let second = within(Duration::from_secs(5), move || {
        context.shutdown(Duration::from_millis(200))
    });
    assert_eq!(second, Err(ShutdownTimedOut { abandoned: 1 }));
    assert_eq!(
        first.join().unwrap(),
        Err(ShutdownTimedOut { abandoned: 1 })
    );
}

#[test]
fn shutdown_closes_listeners_and_later_work() {
    let context = Context::new().unwrap();
    let listener = context
        .bind(
            "127.0.0.1:0".parse().unwrap(),
            ConnectionConfig::new(SocketType::Router),
        )
        .unwrap();
    let shut = context.clone();
    within(Duration::from_secs(5), move || {
        shut.shutdown(Duration::from_secs(1))
    })
    .unwrap();
    let accepted = within(Duration::from_secs(5), move || {
        let mut listener = listener;
        listener.accept_blocking()
    });
    assert!(accepted.is_none(), "the listener closed");
    let after = context.connect(free_port(), dealer("after"));
    let (_after, next) = recv(after);
    assert_eq!(next, None, "a stopped context closes new connections");
}

#[test]
fn a_connection_learns_its_peer_from_the_handshake() {
    let libzmq = Socket::new(Kind::Dealer);
    libzmq.set_routing_id(b"lib");
    let addr = libzmq.bind_loopback();
    let context = Context::new().unwrap();
    let connection = context.connect(addr, dealer("us"));
    libzmq.send(&[b"first"]).unwrap();
    // The peer is known before its first message can be read.
    let (connection, first) = recv(connection);
    assert_eq!(first, Some(message(&[b"first"])));
    let peer = connection.peer().expect("known once a message has arrived");
    assert_eq!(peer.peer_identity, Frame::from("lib"));
    assert_eq!(peer.peer_socket_type, Some(SocketType::Dealer));
}

#[test]
fn an_accepted_connection_knows_its_peer_on_arrival() {
    let context = Context::new().unwrap();
    let mut listener = context
        .bind(
            "127.0.0.1:0".parse().unwrap(),
            ConnectionConfig::new(SocketType::Router),
        )
        .unwrap();
    let libzmq = Socket::new(Kind::Dealer);
    libzmq.set_routing_id(b"client");
    libzmq.connect(listener.local_addr());
    let connection =
        within(Duration::from_secs(5), move || listener.accept_blocking()).expect("accepted");
    let peer = connection.peer().expect("delivered after the handshake");
    assert_eq!(peer.peer_identity, Frame::from("client"));
}
