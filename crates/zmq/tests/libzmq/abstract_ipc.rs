//! libzmq's `tests/test_abstract_ipc.cpp`: binding and connecting a name
//! in Linux's abstract socket namespace.

#![cfg(any(target_os = "linux", target_os = "android"))]

use std::process;

use zmq::{Context, Dealer, Endpoint, Error, Message};

use crate::support::{FIVE, within};

/// Sends a two-part message from `client` to `server` and back, as
/// libzmq's `bounce`.
fn bounce(server: zmq::Socket<Dealer>, client: zmq::Socket<Dealer>) {
    within(FIVE, move || {
        let (mut server, mut client) = (server, client);
        let mut message = Message::new("12345678ABCDEFGH12345678abcdefgh");
        message.push("12345678ABCDEFGH12345678abcdefgh");
        client.send(message.clone());
        let received = server.recv();
        assert_eq!(received, message);
        server.send(received);
        assert_eq!(client.recv(), message);
    });
}

#[test]
fn roundtrip() {
    // libzmq binds `@tmp-tester`; the process id keeps parallel runs apart.
    let name = format!("tmp-tester-{}", process::id());
    let endpoint = format!("ipc://@{name}");
    let context = Context::new().unwrap();
    let mut server = context.socket::<Dealer>();
    let bound = server.bind(&endpoint).unwrap();
    assert_eq!(bound, Endpoint::IpcAbstract(name));
    assert_eq!(bound.to_string(), endpoint);
    let mut client = context.socket::<Dealer>();
    client.connect(&endpoint).unwrap();
    bounce(server, client);
}

#[test]
fn empty_abstract_name() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Dealer>();
    assert!(matches!(
        server.bind("ipc://@"),
        Err(Error::InvalidEndpoint(_))
    ));
}
