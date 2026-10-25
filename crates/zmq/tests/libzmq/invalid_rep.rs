//! libzmq's `tests/test_invalid_rep.cpp`: a REQ ignores a malformed reply
//! and still takes the valid one after it.

use std::time::Duration;

use zmq::{Context, Linger, Req, Router, SocketOptions};

use crate::reqrep_inproc::{Requesting, msg};
use crate::support::FIVE;

#[test]
fn invalid_rep() {
    // Create REQ/ROUTER wiring.
    let no_linger = || SocketOptions {
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    };
    let context = Context::new().unwrap();
    let mut router_socket = context.socket_with::<Router>(no_linger());
    let mut req_socket = context.socket_with::<Req>(no_linger());
    router_socket.bind("inproc://invalid_rep").unwrap();
    req_socket.connect("inproc://invalid_rep").unwrap();

    // Initial request.
    let requesting = Requesting::start(req_socket, msg(&["r"]));

    // Receive the request.
    let (addr, request) = router_socket.recv_timeout(FIVE).expect("the request");
    assert_eq!(request, msg(&["", "r"]));

    // Send invalid replies. libzmq sends the address alone, a reply of no
    // frames, which a zmq-rs message cannot be; these lack the delimiter,
    // or have nothing after it.
    router_socket.send(&addr, msg(&["b"])).unwrap();
    router_socket.send(&addr, msg(&[""])).unwrap();

    // Send valid reply.
    router_socket.send(&addr, msg(&["", "b"])).unwrap();

    // Check whether we've got the valid reply.
    let (_req, reply) = requesting.finish();
    assert_eq!(reply, msg(&["b"]));
}
