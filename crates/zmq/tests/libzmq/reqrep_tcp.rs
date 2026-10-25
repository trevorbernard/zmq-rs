//! libzmq's `tests/test_reqrep_tcp.cpp`: REQ-REP over tcp, with one REP
//! and with several, over IPv4 and IPv6, undoing each connect and bind by
//! the endpoint it used.
//!
//! Not ported: `multi_connect_same_port`, which connects from fixed
//! source ports with libzmq's `tcp://source;destination` endpoints, which
//! zmq-rs does not support. `multi_connect` uses that syntax for one of its
//! three connections; here all three connect plainly.

use zmq::{Context, Rep, Req};

use crate::support::bounce_req_rep;

fn loopback(ipv6: bool) -> &'static str {
    if ipv6 {
        "tcp://[::1]:*"
    } else {
        "tcp://127.0.0.1:*"
    }
}

fn single_connect(ipv6: bool) {
    let context = Context::new().unwrap();
    let mut sb = context.socket::<Rep>();
    let my_endpoint = sb.bind(loopback(ipv6)).unwrap().to_string();
    let mut sc = context.socket::<Req>();
    sc.connect(&my_endpoint).unwrap();
    let mut sc = bounce_req_rep(&mut sb, sc);

    // the sockets are disconnected and unbound explicitly in this test
    // case to check that this can be done successfully with the expected
    // endpoints/addresses
    sc.disconnect(&my_endpoint).unwrap();
    sb.unbind(&my_endpoint).unwrap();
}

fn multi_connect(ipv6: bool) {
    let context = Context::new().unwrap();
    let mut sbs: Vec<_> = (0..3).map(|_| context.socket::<Rep>()).collect();
    let endpoints: Vec<_> = sbs
        .iter_mut()
        .map(|sb| sb.bind(loopback(ipv6)).unwrap().to_string())
        .collect();
    let mut sc = context.socket::<Req>();
    for endpoint in &endpoints {
        sc.connect(endpoint).unwrap();
    }

    for i in [0, 1, 2, 0, 1, 2, 0] {
        sc = bounce_req_rep(&mut sbs[i], sc);
    }

    // see comment on disconnect and unbind in single_connect
    for endpoint in [&endpoints[0], &endpoints[2], &endpoints[1]] {
        sc.disconnect(endpoint).unwrap();
    }
    for (sb, endpoint) in sbs.iter_mut().zip(&endpoints) {
        sb.unbind(endpoint).unwrap();
    }
}

#[test]
fn single_connect_ipv4() {
    single_connect(false);
}

#[test]
fn multi_connect_ipv4() {
    multi_connect(false);
}

#[test]
fn single_connect_ipv6() {
    single_connect(true);
}

#[test]
fn multi_connect_ipv6() {
    multi_connect(true);
}
