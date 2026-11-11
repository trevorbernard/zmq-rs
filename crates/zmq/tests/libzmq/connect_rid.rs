//! libzmq's `tests/test_connect_rid.cpp`: `ZMQ_CONNECT_ROUTING_ID`, a
//! routing id a ROUTER gives the peer it connects to, so that it can send
//! to it at once. Its STREAM test is left out: there are no STREAM sockets.

use std::time::Duration;

use zmq::{Context, Error, Identity, Message, Router, RoutingId, SocketOptions};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, within};

const SETTLE: Duration = Duration::from_millis(300);

fn named(identity: &'static str) -> SocketOptions {
    SocketOptions {
        identity: Identity::new(identity).unwrap(),
        ..SocketOptions::default()
    }
}

fn router_2_router(named_: bool) {
    let context = Context::new().unwrap();
    let options = |identity| {
        if named_ {
            named(identity)
        } else {
            SocketOptions::default()
        }
    };
    let mut rbind = context.socket_with::<Router>(options("X"));
    let endpoint = rbind.bind(&free_endpoint()).unwrap().to_string();
    let mut rconn1 = context.socket_with::<Router>(options("Y"));

    let conn1 = RoutingId::new("conn1").unwrap();
    rconn1
        .connect_with_routing_id(&endpoint, conn1.clone())
        .unwrap();
    rconn1.send(&conn1, "hi 1").unwrap();

    let (from, message, mut rbind) = within(FIVE, move || {
        let (from, message) = rbind.recv().unwrap();
        (from, message, rbind)
    });
    if named_ {
        assert_eq!(from.as_bytes(), b"Y");
    } else {
        assert_eq!(from.as_bytes()[0], 0);
    }
    assert_eq!(message, Message::from("hi 1"));

    rbind.send(&from, "ok").unwrap();

    // The bound socket's own identity does not rename the connection.
    let (from, message) = within(FIVE, move || rconn1.recv().unwrap());
    assert_eq!(from, conn1);
    assert_eq!(message, Message::from("ok"));
}

#[test]
fn router_2_router_unnamed() {
    router_2_router(false);
}

#[test]
fn router_2_router_named() {
    router_2_router(true);
}

#[test]
fn router_2_router_while_receiving() {
    let context = Context::new().unwrap();
    let mut xbind = context.socket_with::<Router>(named("X"));
    let x_endpoint = xbind.bind(&free_endpoint()).unwrap().to_string();
    let mut zbind = context.socket_with::<Router>(named("Z"));
    let z_endpoint = zbind.bind(&free_endpoint()).unwrap().to_string();
    let mut yconn = context.socket_with::<Router>(named("Y"));

    let x = RoutingId::new("X").unwrap();
    let z = RoutingId::new("Z").unwrap();
    yconn
        .connect_with_routing_id(&x_endpoint, x.clone())
        .unwrap();
    yconn.send(&x, "hi 1").unwrap();
    std::thread::sleep(SETTLE);

    // X, with a message from Y waiting, connects to Z and sends to it.
    xbind
        .connect_with_routing_id(&z_endpoint, z.clone())
        .unwrap();
    xbind.send(&z, "hi 1").unwrap();
    std::thread::sleep(SETTLE);

    assert_eq!(yconn.try_recv(), Ok(None));
    let (from, message) = within(FIVE, move || zbind.recv().unwrap());
    assert_eq!(from, x);
    assert_eq!(message, Message::from("hi 1"));
}

/// libzmq asserts on this, in a test it leaves commented out.
#[test]
fn a_routing_id_in_use_is_refused() {
    let context = Context::new().unwrap();
    let mut rbind = context.socket::<Router>();
    let endpoint = rbind.bind(&free_endpoint()).unwrap().to_string();
    let mut rconn1 = context.socket::<Router>();
    let conn1 = RoutingId::new("conn1").unwrap();
    rconn1
        .connect_with_routing_id(&endpoint, conn1.clone())
        .unwrap();
    assert!(matches!(
        rconn1.connect_with_routing_id(&endpoint, conn1.clone()),
        Err(Error::RoutingIdInUse)
    ));

    // Free again once that connect is gone.
    rconn1.disconnect(&endpoint).unwrap();
    rconn1.connect_with_routing_id(&endpoint, conn1).unwrap();
}
