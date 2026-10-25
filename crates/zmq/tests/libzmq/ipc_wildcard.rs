//! libzmq's `tests/test_ipc_wildcard.cpp`: binding `ipc://*` picks a fresh
//! path that peers can connect to.

#![cfg(unix)]

use zmq::{Context, Endpoint, Message, Pair};

use crate::support::{FIVE, within};

#[test]
fn ipc_wildcard() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Pair>();
    let bound = server.bind("ipc://*").unwrap();
    assert!(matches!(bound, Endpoint::Ipc(_)));
    let mut client = context.socket::<Pair>();
    client.connect(&bound.to_string()).unwrap();
    within(FIVE, move || {
        let mut message = Message::new("12345678ABCDEFGH12345678abcdefgh");
        message.push("12345678ABCDEFGH12345678abcdefgh");
        client.send(message.clone());
        let received = server.recv();
        assert_eq!(received, message);
        server.send(received);
        assert_eq!(client.recv(), message);
    });
}
