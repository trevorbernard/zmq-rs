//! ZMTP 3.1 heartbeats and the handshake deadline, observed on the wire
//! with a zmtp peer and against libzmq.

use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq_core::{Connection, ConnectionConfig, Context, Heartbeat, Reconnect};
use zmq_interop::libzmq::{Kind, Socket};
use zmq_interop::{Peer, Security};
use zmq_test_ports::free_port;
use zmtp::{Command, Identity, Incoming, Message, Ping, PingContext, SocketType, Ttl};

const FIVE: Duration = Duration::from_secs(5);

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

fn dealer(heartbeat: Option<Heartbeat>) -> ConnectionConfig {
    ConnectionConfig {
        heartbeat,
        reconnect: Reconnect::Never,
        ..ConnectionConfig::new(SocketType::Dealer)
    }
}

fn accept(listener: &TcpListener) -> Peer {
    Peer::accept(
        listener,
        SocketType::Router,
        Identity::default(),
        Security::Null,
    )
    .unwrap()
}

fn command(incoming: Incoming) -> Option<Command> {
    match incoming {
        Incoming::Command(body) => Some(Command::parse(&body).unwrap()),
        Incoming::Message(_) => None,
    }
}

/// Waits for the connection's receive pipe to close.
fn closes(connection: Connection) -> Duration {
    let started = Instant::now();
    within(FIVE, move || {
        let mut connection = connection;
        assert_eq!(connection.rx.recv_blocking(), None);
    });
    started.elapsed()
}

#[test]
fn pings_go_out_at_the_interval_with_our_ttl() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let heartbeat = Heartbeat {
        interval: Duration::from_millis(50),
        timeout: Duration::from_secs(2),
        ttl: Duration::from_millis(500),
    };
    let _connection = context.connect(listener.local_addr().unwrap(), dealer(Some(heartbeat)));
    let mut peer = accept(&listener);
    let mut arrivals = Vec::new();
    while arrivals.len() < 3 {
        if let Some(Command::Ping(ping)) = command(peer.recv().unwrap()) {
            assert_eq!(ping.ttl, Ttl::from_deciseconds(5));
            arrivals.push(Instant::now());
        }
    }
    for pair in arrivals.windows(2) {
        assert!(
            pair[1] - pair[0] >= Duration::from_millis(30),
            "{arrivals:?}"
        );
    }
}

#[test]
fn a_silent_peer_times_out() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let connection = context.connect(
        listener.local_addr().unwrap(),
        dealer(Some(Heartbeat::every(Duration::from_millis(50)))),
    );
    let _silent = accept(&listener);
    let elapsed = closes(connection);
    assert!(elapsed < Duration::from_secs(2), "took {elapsed:?}");
}

#[test]
fn a_timed_out_connection_reconnects() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let config = ConnectionConfig {
        reconnect: Reconnect::Fixed {
            interval: Duration::from_millis(20),
        },
        ..dealer(Some(Heartbeat::every(Duration::from_millis(50))))
    };
    let _connection = context.connect(listener.local_addr().unwrap(), config);
    let _first = accept(&listener);
    listener.set_nonblocking(false).unwrap();
    let second = within(FIVE, move || accept(&listener));
    drop(second);
}

#[test]
fn a_peer_answering_pings_keeps_the_connection() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let mut connection = context.connect(
        listener.local_addr().unwrap(),
        dealer(Some(Heartbeat::every(Duration::from_millis(50)))),
    );
    let mut peer = accept(&listener);
    let (done, answered) = mpsc::channel();
    // Answer pings for well over the timeout, then wait for a message.
    thread::spawn(move || {
        let started = Instant::now();
        let mut pings = 0;
        loop {
            match peer.recv().unwrap() {
                Incoming::Command(body) => {
                    if let Command::Ping(ping) = Command::parse(&body).unwrap() {
                        pings += 1;
                        peer.send_command(&Command::Pong(ping.context)).unwrap();
                    }
                }
                Incoming::Message(message) => {
                    let _ = done.send((pings, started.elapsed(), message));
                    return;
                }
            }
        }
    });
    thread::sleep(Duration::from_millis(400));
    connection
        .tx
        .send_blocking(Message::from("still here"))
        .expect("the connection survived");
    let answered = answered.recv_timeout(FIVE).unwrap();
    let (pings, elapsed, message) = answered;
    assert_eq!(message, Message::from("still here"));
    assert!(pings >= 3, "{pings} pings in {elapsed:?}");
}

#[test]
fn we_answer_pings_and_honour_the_peers_ttl() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let connection = context.connect(listener.local_addr().unwrap(), dealer(None));
    let mut peer = accept(&listener);
    peer.send_command(&Command::Ping(Ping {
        ttl: Ttl::from_deciseconds(1),
        context: PingContext::new(&b"ctx"[..]).unwrap(),
    }))
    .unwrap();
    let pong = command(peer.recv().unwrap());
    assert_eq!(
        pong,
        Some(Command::Pong(PingContext::new(&b"ctx"[..]).unwrap()))
    );
    // Now silent: after the 100 ms TTL we give up on the peer.
    let elapsed = closes(connection);
    assert!(elapsed < Duration::from_secs(2), "took {elapsed:?}");
}

#[test]
fn libzmq_heartbeats_keep_its_connection_to_us() {
    let context = Context::new().unwrap();
    let mut listener = context
        .bind(free_port(), ConnectionConfig::new(SocketType::Router))
        .unwrap();
    let libzmq = Socket::new(Kind::Dealer);
    libzmq.set_heartbeat(
        Duration::from_millis(50),
        Duration::from_millis(100),
        Duration::ZERO,
    );
    libzmq.connect(listener.local_addr());
    let (connection, mut listener) = within(FIVE, move || {
        let connection = listener.accept_blocking().expect("accepted");
        (connection, listener)
    });
    thread::sleep(Duration::from_millis(500));
    libzmq.send(&[b"after a while"]).unwrap();
    let message = within(FIVE, move || {
        let mut connection = connection;
        connection.rx.recv_blocking()
    });
    assert_eq!(message, Some(Message::from("after a while")));
    assert!(
        listener.try_accept().is_err(),
        "libzmq never had to reconnect: our PONGs kept it alive"
    );
}

#[test]
fn our_heartbeats_are_answered_by_libzmq() {
    let router = Socket::new(Kind::Router);
    let addr = router.bind_loopback();
    let context = Context::new().unwrap();
    let mut connection = context.connect(
        addr,
        ConnectionConfig {
            identity: Identity::new("hb").unwrap(),
            ..dealer(Some(Heartbeat::every(Duration::from_millis(50))))
        },
    );
    thread::sleep(Duration::from_millis(500));
    connection
        .tx
        .send_blocking(Message::from("alive"))
        .expect("libzmq's PONGs kept the connection open");
    assert_eq!(router.recv().unwrap(), [&b"hb"[..], b"alive"]);
}

#[test]
fn a_stalled_handshake_times_out() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let config = ConnectionConfig {
        handshake_timeout: Some(Duration::from_millis(100)),
        ..dealer(None)
    };
    let connection = context.connect(listener.local_addr().unwrap(), config);
    // Accept the TCP connection but never say a word.
    let (_stream, _) = listener.accept().unwrap();
    let elapsed = closes(connection);
    assert!(elapsed < Duration::from_secs(2), "took {elapsed:?}");
}

#[test]
fn the_default_handshake_deadline_matches_libzmq() {
    let config = ConnectionConfig::new(SocketType::Dealer);
    assert_eq!(config.handshake_timeout, Some(Duration::from_secs(30)));
    assert_eq!(config.heartbeat, None);
}
