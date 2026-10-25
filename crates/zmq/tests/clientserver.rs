//! CLIENT and SERVER (RFC 41): single-part messages, and a server that
//! routes replies by the id it gave each client. Draft sockets.
#![cfg(feature = "draft")]

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{
    Client, ClientId, Context, Dealer, Frame, Hwm, NoSuchClient, Server, ServerSendError,
    SocketOptions,
};

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

#[test]
fn a_server_answers_each_client_by_its_id() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>();
    let bound = server.bind("tcp://127.0.0.1:*").unwrap();
    let mut clients: Vec<_> = (0..3)
        .map(|n| {
            let mut client = context.socket::<Client>();
            client.connect(&bound.to_string()).unwrap();
            client.send(format!("hello from {n}"));
            client
        })
        .collect();

    let (requests, mut server) = within(FIVE, move || {
        let requests: Vec<(ClientId, Frame)> = (0..3).map(|_| server.recv()).collect();
        (requests, server)
    });
    let ids: HashSet<ClientId> = requests.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids.len(), 3, "every client has its own id");
    for (id, request) in requests {
        let text = String::from_utf8_lossy(&request).into_owned();
        server.send(id, format!("re: {text}")).unwrap();
    }
    for (n, client) in clients.iter_mut().enumerate() {
        let mut client = std::mem::replace(client, context.socket::<Client>());
        let reply = within(FIVE, move || client.recv());
        assert_eq!(reply, Frame::from(format!("re: hello from {n}")));
    }
}

#[test]
fn client_ids_are_never_zero() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>();
    server.bind("inproc://ids").unwrap();
    let mut client = context.socket::<Client>();
    client.connect("inproc://ids").unwrap();
    client.send("hi");
    let (id, _) = server.try_recv().unwrap();
    assert_ne!(id.get(), 0);
}

#[test]
fn sending_to_an_unknown_or_departed_client_fails() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>();
    server.bind("inproc://departed").unwrap();
    let mut client = context.socket::<Client>();
    client.connect("inproc://departed").unwrap();
    client.send("hi");
    let (id, _) = server.try_recv().unwrap();
    let stranger = ClientId::new(id.get().wrapping_add(1000).max(1)).unwrap();
    assert_eq!(
        server.send(stranger, "who?"),
        Err(NoSuchClient(Frame::from("who?")))
    );
    drop(client);
    assert_eq!(
        server.send(id, "gone"),
        Err(NoSuchClient(Frame::from("gone")))
    );
}

#[test]
fn a_full_client_makes_try_send_hand_the_frame_back() {
    let context = Context::new().unwrap();
    let one = Hwm::Messages(NonZeroUsize::MIN);
    let mut server = context.socket_with::<Server>(SocketOptions {
        send_hwm: one,
        ..SocketOptions::default()
    });
    server.bind("inproc://full").unwrap();
    let mut client = context.socket_with::<Client>(SocketOptions {
        recv_hwm: one,
        ..SocketOptions::default()
    });
    client.connect("inproc://full").unwrap();
    client.send("hi");
    let (id, _) = server.try_recv().unwrap();
    server.try_send(id, "1").unwrap();
    server.try_send(id, "2").unwrap();
    assert_eq!(
        server.try_send(id, "3"),
        Err(ServerSendError::Full(Frame::from("3")))
    );
    // A blocking send waits for room.
    let sending = thread::spawn(move || {
        server.send(id, "3").unwrap();
        server
    });
    let received = within(FIVE, move || [client.recv(), client.recv(), client.recv()]);
    assert_eq!(
        received,
        [Frame::from("1"), Frame::from("2"), Frame::from("3")]
    );
    sending.join().unwrap();
}

#[test]
fn a_disconnected_client_is_forgotten() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>();
    server.bind("inproc://disconnect").unwrap();
    let mut client = context.socket::<Client>();
    client.connect("inproc://disconnect").unwrap();
    client.send("hi");
    let (id, _) = server.try_recv().unwrap();
    assert!(server.disconnect_client(id));
    assert!(!server.disconnect_client(id), "already gone");
    assert_eq!(
        server.try_send(id, "after"),
        Err(ServerSendError::NoSuchClient(Frame::from("after")))
    );
    assert!(
        client.try_send("anyone?").is_err(),
        "its connection was closed"
    );
}

#[test]
fn a_client_load_balances_across_servers() {
    let context = Context::new().unwrap();
    let mut servers: Vec<_> = (0..2)
        .map(|n| {
            let mut server = context.socket::<Server>();
            server.bind(&format!("inproc://balanced-{n}")).unwrap();
            server
        })
        .collect();
    let mut client = context.socket::<Client>();
    for n in 0..2 {
        client.connect(&format!("inproc://balanced-{n}")).unwrap();
    }
    for i in 0..4 {
        client.send(format!("{i}"));
    }
    for server in &mut servers {
        assert!(server.try_recv().is_some());
        assert!(server.try_recv().is_some());
        assert!(server.try_recv().is_none());
    }
}

#[test]
fn only_clients_and_servers_pair() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>();
    server.bind("inproc://strangers").unwrap();
    let mut dealer = context.socket::<Dealer>();
    dealer.connect("inproc://strangers").unwrap();
    assert!(dealer.try_send("not a client").is_err());
    assert!(server.try_recv().is_none());
}
