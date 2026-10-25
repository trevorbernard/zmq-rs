//! libzmq's `tests/test_invalid_rep.cpp`: a REQ ignores a malformed reply
//! and still takes the valid one after it.

use zmq::{Context, Req, Router};

use crate::support::{FIVE, Requesting, msg, zero_linger};

#[test]
fn invalid_rep() {
    // Create REQ/ROUTER wiring.
    let context = Context::new().unwrap();
    let mut router_socket = context.socket_with::<Router>(zero_linger());
    let mut req_socket = context.socket_with::<Req>(zero_linger());
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
