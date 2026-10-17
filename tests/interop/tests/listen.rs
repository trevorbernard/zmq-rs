//! zmq-core listeners: libzmq and zmq-core peers connecting to us.

use std::io;
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq_core::{Connection, ConnectionConfig, Context, Listener};
use zmq_interop::libzmq::{Kind, Socket};
use zmq_pipe::TryRecvError;
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

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn router() -> ConnectionConfig {
    ConnectionConfig {
        identity: Identity::new("srv").unwrap(),
        ..ConnectionConfig::new(SocketType::Router)
    }
}

fn accept(listener: Listener) -> (Listener, Option<Connection>) {
    within(Duration::from_secs(5), move || {
        let mut listener = listener;
        let connection = listener.accept_blocking();
        (listener, connection)
    })
}

fn recv(connection: Connection) -> (Connection, Option<Message>) {
    within(Duration::from_secs(5), move || {
        let mut connection = connection;
        let message = connection.rx.recv_blocking();
        (connection, message)
    })
}

#[test]
fn bind_reports_the_actual_port() {
    let context = Context::new().unwrap();
    let listener = context.bind(loopback(), router()).unwrap();
    assert_ne!(listener.local_addr().port(), 0);
    TcpStream::connect(listener.local_addr()).expect("port is listening");
}

#[test]
fn bind_reports_an_address_in_use() {
    let context = Context::new().unwrap();
    let first = context.bind(loopback(), router()).unwrap();
    let second = context.bind(first.local_addr(), router());
    assert_eq!(
        second.map(|l| l.local_addr()).unwrap_err().kind(),
        io::ErrorKind::AddrInUse
    );
}

#[test]
fn libzmq_dealer_talks_to_our_listener() {
    let context = Context::new().unwrap();
    let listener = context.bind(loopback(), router()).unwrap();
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_routing_id(b"lib");
    dealer.connect(listener.local_addr());
    dealer.send(&[b"hi"]).unwrap();

    let (_listener, connection) = accept(listener);
    let (mut connection, first) = recv(connection.expect("a connection"));
    assert_eq!(first, Some(message(&[b"hi"])));

    connection.tx.send_blocking(message(&[b"back"])).unwrap();
    assert_eq!(dealer.recv().unwrap(), [b"back".to_vec()]);
}

#[test]
fn every_connecting_peer_is_accepted() {
    let context = Context::new().unwrap();
    let mut listener = context.bind(loopback(), router()).unwrap();
    let dealers: Vec<Socket> = (0..5)
        .map(|i| {
            let dealer = Socket::new(Kind::Dealer);
            dealer.connect(listener.local_addr());
            dealer.send(&[format!("peer {i}").as_bytes()]).unwrap();
            dealer
        })
        .collect();
    let mut greetings = Vec::new();
    for _ in 0..dealers.len() {
        let (back, connection) = accept(listener);
        listener = back;
        let (_connection, first) = recv(connection.expect("a connection"));
        greetings.push(first.expect("a message"));
    }
    greetings.sort_by_key(|m| m.get(0).unwrap().to_vec());
    for (i, greeting) in greetings.iter().enumerate() {
        assert_eq!(*greeting, message(&[format!("peer {i}").as_bytes()]));
    }
}

#[test]
fn our_connect_reaches_our_listener() {
    let context = Context::new().unwrap();
    let listener = context.bind(loopback(), router()).unwrap();
    let dealer = ConnectionConfig {
        identity: Identity::new("cli").unwrap(),
        ..ConnectionConfig::new(SocketType::Dealer)
    };
    let mut client = context.connect(listener.local_addr(), dealer);
    client.tx.send_blocking(message(&[b"ping"])).unwrap();

    let (_listener, server) = accept(listener);
    let (mut server, first) = recv(server.expect("a connection"));
    assert_eq!(first, Some(message(&[b"ping"])));
    server.tx.send_blocking(message(&[b"pong"])).unwrap();
    let (_client, reply) = recv(client);
    assert_eq!(reply, Some(message(&[b"pong"])));
}

#[test]
fn peers_that_fail_the_handshake_are_never_delivered() {
    let context = Context::new().unwrap();
    let mut listener = context.bind(loopback(), router()).unwrap();
    let sub = Socket::new(Kind::Sub);
    sub.connect(listener.local_addr());
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        listener.try_accept().map(|_| ()),
        Err(TryRecvError::Empty),
        "an incompatible SUB is not a connection"
    );

    let dealer = Socket::new(Kind::Dealer);
    dealer.connect(listener.local_addr());
    dealer.send(&[b"ok"]).unwrap();
    let (_listener, connection) = accept(listener);
    let (_connection, first) = recv(connection.expect("the DEALER is accepted"));
    assert_eq!(first, Some(message(&[b"ok"])));
}

#[test]
fn dropping_the_listener_closes_the_port() {
    let context = Context::new().unwrap();
    let listener = context.bind(loopback(), router()).unwrap();
    let addr = listener.local_addr();
    drop(listener);
    let deadline = Instant::now() + Duration::from_secs(5);
    while TcpStream::connect(addr).is_ok() {
        assert!(Instant::now() < deadline, "port still open");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn dropping_the_context_ends_accepting() {
    let context = Context::new().unwrap();
    let listener = context.bind(loopback(), router()).unwrap();
    within(Duration::from_secs(5), move || drop(context));
    let (_listener, connection) = accept(listener);
    assert!(connection.is_none());
}

#[test]
fn accept_works_with_async() {
    let context = Context::new().unwrap();
    let mut listener = context.bind(loopback(), router()).unwrap();
    let dealer = Socket::new(Kind::Dealer);
    dealer.connect(listener.local_addr());
    dealer.send(&[b"async"]).unwrap();
    let connection = within(Duration::from_secs(5), move || {
        block_on(async move { listener.accept().await })
    });
    let (_connection, first) = recv(connection.expect("a connection"));
    assert_eq!(first, Some(message(&[b"async"])));
}

/// A minimal `block_on`: polls with a waker that unparks this thread.
fn block_on<F: Future>(future: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct Unpark(thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}
