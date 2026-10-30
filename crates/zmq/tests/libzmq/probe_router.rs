//! libzmq's `tests/test_probe_router.cpp`: `ZMQ_PROBE_ROUTER`, an empty
//! message a connecting socket sends first so that a ROUTER learns its
//! routing id at once.

use zmq::{Context, Dealer, Identity, Message, Router, RoutingId, SocketOptions};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, within};

fn named(identity: &'static str) -> SocketOptions {
    SocketOptions {
        identity: Identity::new(identity).unwrap(),
        ..SocketOptions::default()
    }
}

#[test]
fn probe_router_router() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Router>();
    let endpoint = server.bind(&free_endpoint()).unwrap().to_string();

    let mut client = context.socket_with::<Router>(named("X"));
    client.set_probe(true);
    client.connect(&endpoint).unwrap();

    // The probe: routing id X and an empty message.
    let (from, probe, mut server) = within(FIVE, move || {
        let (from, probe) = server.recv();
        (from, probe, server)
    });
    assert_eq!(from.as_bytes(), b"X");
    assert_eq!(probe, Message::from(""));

    server.send(&from, "Hello").unwrap();

    // The server's routing id is generated, as it set none: five octets,
    // as libzmq generates them.
    let (server_id, message) = within(FIVE, move || client.recv());
    assert_eq!(server_id.as_bytes().len(), 5);
    assert_eq!(message, Message::from("Hello"));
}

#[test]
fn probe_router_dealer() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Router>();
    let endpoint = server.bind(&free_endpoint()).unwrap().to_string();

    let mut client = context.socket_with::<Dealer>(named("X"));
    client.set_probe(true);
    client.connect(&endpoint).unwrap();

    let (from, probe, mut server) = within(FIVE, move || {
        let (from, probe) = server.recv();
        (from, probe, server)
    });
    assert_eq!(from, RoutingId::new("X").unwrap());
    assert_eq!(probe, Message::from(""));

    server.send(&from, "Hello").unwrap();
    assert_eq!(within(FIVE, move || client.recv()), Message::from("Hello"));
}
