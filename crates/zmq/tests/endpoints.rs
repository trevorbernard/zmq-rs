//! Undoing binds and connects, as libzmq's `zmq_unbind` and
//! `zmq_disconnect`, and spreading connections over I/O threads.

use std::net::TcpListener;
use std::time::Duration;

use zmq::{Context, Error, Message, Pull, Push, Reconnect, SocketOptions};

const FIVE: Duration = Duration::from_secs(5);
const BRIEFLY: Duration = Duration::from_millis(200);

fn fast_reconnect() -> SocketOptions {
    SocketOptions {
        reconnect: Reconnect::Fixed {
            interval: Duration::from_millis(10),
        },
        ..SocketOptions::default()
    }
}

/// A TCP endpoint nothing listens on.
fn unused_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    format!("tcp://{}", listener.local_addr().unwrap())
}

#[test]
fn connections_spread_over_several_io_threads() {
    let context = Context::with_io_threads(3).unwrap();
    let mut pulls: Vec<_> = (0..3).map(|_| context.socket::<Pull>()).collect();
    let mut push = context.socket::<Push>();
    for pull in &mut pulls {
        let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
        push.connect(&bound.to_string()).unwrap();
    }
    for i in 0..3 {
        push.send(format!("m{i}"));
    }
    for pull in &mut pulls {
        assert!(pull.recv_timeout(FIVE).is_some(), "every peer gets one");
    }
}

#[test]
fn a_context_needs_an_io_thread() {
    let err = Context::with_io_threads(0).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn an_unbound_port_can_be_bound_again_at_once() {
    let context = Context::new().unwrap();
    let mut first = context.socket::<Pull>();
    let bound = first.bind("tcp://127.0.0.1:*").unwrap().to_string();
    first.unbind(&bound).unwrap();
    let mut second = context.socket::<Pull>();
    second.bind(&bound).expect("the port is free");
}

#[test]
fn unbinding_closes_accepted_connections() {
    let context = Context::new().unwrap();
    let mut first = context.socket::<Pull>();
    let bound = first.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut push = context.socket_with::<Push>(fast_reconnect());
    push.connect(&bound).unwrap();
    push.send("before");
    assert_eq!(first.recv_timeout(FIVE), Some(Message::from("before")));
    first.unbind(&bound).unwrap();
    // The push reconnects to whoever binds the port next.
    let mut second = context.socket::<Pull>();
    second.bind(&bound).unwrap();
    push.send("after");
    assert_eq!(second.recv_timeout(FIVE), Some(Message::from("after")));
    assert_eq!(first.recv_timeout(BRIEFLY), None);
}

#[test]
fn a_wildcard_host_names_the_same_bind() {
    let port = TcpListener::bind("0.0.0.0:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.bind(&format!("tcp://*:{port}")).unwrap();
    pull.unbind(&format!("tcp://0.0.0.0:{port}")).unwrap();
    pull.bind(&format!("tcp://0.0.0.0:{port}")).unwrap();
    pull.unbind(&format!("tcp://*:{port}")).unwrap();
}

#[test]
fn unbinding_what_is_not_bound_fails() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
    assert!(matches!(
        pull.unbind("tcp://127.0.0.1:*"),
        Err(Error::UnknownEndpoint(_))
    ));
    pull.unbind(&bound).unwrap();
    assert!(matches!(
        pull.unbind(&bound),
        Err(Error::UnknownEndpoint(_))
    ));
    assert!(matches!(
        pull.unbind("nonsense"),
        Err(Error::InvalidEndpoint(_))
    ));
}

#[cfg(unix)]
#[test]
fn unbinding_ipc_removes_its_file() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("ipc://*").unwrap();
    let zmq::Endpoint::Ipc(path) = &bound else {
        panic!("an ipc endpoint");
    };
    assert!(path.exists());
    pull.unbind(&bound.to_string()).unwrap();
    assert!(!path.exists());
}

#[test]
fn an_unbound_inproc_name_is_free() {
    let context = Context::new().unwrap();
    let mut first = context.socket::<Pull>();
    first.bind("inproc://name").unwrap();
    first.unbind("inproc://name").unwrap();
    let mut second = context.socket::<Pull>();
    second.bind("inproc://name").expect("the name is free");
}

#[test]
fn a_disconnected_peer_gets_no_more() {
    let context = Context::new().unwrap();
    let mut kept = context.socket::<Pull>();
    let kept_at = kept.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut dropped = context.socket::<Pull>();
    let dropped_at = dropped.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut push = context.socket::<Push>();
    push.connect(&kept_at).unwrap();
    push.connect(&dropped_at).unwrap();
    push.disconnect(&dropped_at).unwrap();
    for i in 0..4 {
        push.send(format!("m{i}"));
    }
    for i in 0..4 {
        assert_eq!(
            kept.recv_timeout(FIVE),
            Some(Message::from(format!("m{i}")))
        );
    }
    assert_eq!(dropped.recv_timeout(BRIEFLY), None);
}

#[test]
fn a_disconnected_endpoint_is_not_retried() {
    let context = Context::new().unwrap();
    let endpoint = unused_endpoint();
    let mut push = context.socket_with::<Push>(fast_reconnect());
    push.connect(&endpoint).unwrap();
    push.disconnect(&endpoint).unwrap();
    let mut pull = context.socket::<Pull>();
    pull.bind(&endpoint).unwrap();
    assert!(push.send_timeout("lost", BRIEFLY).is_err(), "no peer left");
    assert_eq!(pull.recv_timeout(BRIEFLY), None);
}

#[test]
fn a_connection_can_be_named_by_its_address() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let port = match pull.bind("tcp://127.0.0.1:*").unwrap() {
        zmq::Endpoint::Tcp(addr) => addr.port(),
        other => panic!("a tcp endpoint, not {other}"),
    };
    let mut push = context.socket::<Push>();
    push.connect(&format!("tcp://localhost:{port}")).unwrap();
    push.disconnect(&format!("tcp://127.0.0.1:{port}")).unwrap();
    assert!(matches!(
        push.disconnect(&format!("tcp://localhost:{port}")),
        Err(Error::UnknownEndpoint(_))
    ));
}

#[test]
fn an_immediate_connection_can_be_disconnected() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut push = context.socket_with::<Push>(SocketOptions {
        immediate: true,
        ..fast_reconnect()
    });
    push.connect(&bound).unwrap();
    push.send("connected");
    assert_eq!(pull.recv_timeout(FIVE), Some(Message::from("connected")));
    push.disconnect(&bound).unwrap();
    assert!(push.send_timeout("lost", BRIEFLY).is_err(), "no peer left");
}

#[test]
fn an_inproc_connection_can_be_disconnected() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.bind("inproc://pipeline").unwrap();
    let mut push = context.socket::<Push>();
    push.connect("inproc://pipeline").unwrap();
    push.disconnect("inproc://pipeline").unwrap();
    assert!(push.send_timeout("lost", BRIEFLY).is_err(), "no peer left");
}
