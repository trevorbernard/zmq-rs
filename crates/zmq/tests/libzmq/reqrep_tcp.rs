//! libzmq's `tests/test_reqrep_tcp.cpp`: REQ-REP over tcp, with one REP
//! and with several, over IPv4 and IPv6, undoing each connect and bind by
//! the endpoint it used.
//!
//! Where libzmq connects from the fixed source ports 5564 and 5565, with
//! `tcp://source;destination`, these take free ports.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use zmq::{Context, Rep, Req};
use zmq_test_ports::{free_endpoint, free_port};

use crate::support::bounce_req_rep;

fn loopback(ipv6: bool) -> String {
    if ipv6 {
        format!("tcp://[::1]:{}", free_port().port())
    } else {
        free_endpoint()
    }
}

fn single_connect(ipv6: bool) {
    let context = Context::new().unwrap();
    let mut sb = context.socket::<Rep>();
    let my_endpoint = sb.bind(&loopback(ipv6)).unwrap().to_string();
    let mut sc = context.socket::<Req>();
    sc.connect(&my_endpoint).unwrap();
    let mut sc = bounce_req_rep(&mut sb, sc);

    // the sockets are disconnected and unbound explicitly in this test
    // case to check that this can be done successfully with the expected
    // endpoints/addresses
    sc.disconnect(&my_endpoint).unwrap();
    sb.unbind(&my_endpoint).unwrap();
}

/// A free port on the loopback address, to connect from.
fn source(ipv6: bool) -> SocketAddr {
    let ip: IpAddr = if ipv6 {
        Ipv6Addr::LOCALHOST.into()
    } else {
        Ipv4Addr::LOCALHOST.into()
    };
    SocketAddr::new(ip, free_port().port())
}

/// `tcp://source;destination`, the destination being `endpoint`'s.
fn connect_address(source: SocketAddr, endpoint: &str) -> String {
    let destination = endpoint.strip_prefix("tcp://").expect("a tcp endpoint");
    format!("tcp://{source};{destination}")
}

/// Two REQs each reach both REPs from the same two source ports, crossed
/// over: a source port is reused for a different destination.
fn multi_connect_same_port(ipv6: bool) {
    let context = Context::new().unwrap();
    let mut sb0 = context.socket::<Rep>();
    let my_endpoint_0 = sb0.bind(&loopback(ipv6)).unwrap().to_string();
    let mut sb1 = context.socket::<Rep>();
    let my_endpoint_1 = sb1.bind(&loopback(ipv6)).unwrap().to_string();
    let (first, second) = (source(ipv6), source(ipv6));

    let mut sc0 = context.socket::<Req>();
    let my_endpoint_2 = connect_address(first, &my_endpoint_0);
    sc0.connect(&my_endpoint_2).unwrap();
    let my_endpoint_3 = connect_address(second, &my_endpoint_1);
    sc0.connect(&my_endpoint_3).unwrap();

    let mut sc1 = context.socket::<Req>();
    let my_endpoint_4 = connect_address(second, &my_endpoint_0);
    sc1.connect(&my_endpoint_4).unwrap();
    let my_endpoint_5 = connect_address(first, &my_endpoint_1);
    sc1.connect(&my_endpoint_5).unwrap();

    let mut sc0 = bounce_req_rep(&mut sb0, sc0);
    sc0 = bounce_req_rep(&mut sb1, sc0);
    let mut sc1 = bounce_req_rep(&mut sb0, sc1);
    sc1 = bounce_req_rep(&mut sb1, sc1);
    sc0 = bounce_req_rep(&mut sb0, sc0);
    sc0 = bounce_req_rep(&mut sb1, sc0);

    // see comment on disconnect and unbind in single_connect
    sc1.disconnect(&my_endpoint_4).unwrap();
    sc1.disconnect(&my_endpoint_5).unwrap();
    sc0.disconnect(&my_endpoint_2).unwrap();
    sc0.disconnect(&my_endpoint_3).unwrap();

    sb0.unbind(&my_endpoint_0).unwrap();
    sb1.unbind(&my_endpoint_1).unwrap();
}

fn multi_connect(ipv6: bool) {
    let context = Context::new().unwrap();
    let mut sbs: Vec<_> = (0..3).map(|_| context.socket::<Rep>()).collect();
    let endpoints: Vec<_> = sbs
        .iter_mut()
        .map(|sb| sb.bind(&loopback(ipv6)).unwrap().to_string())
        .collect();
    let mut sc = context.socket::<Req>();
    sc.connect(&endpoints[0]).unwrap();
    sc.connect(&endpoints[1]).unwrap();
    let my_endpoint_3 = connect_address(source(ipv6), &endpoints[2]);
    sc.connect(&my_endpoint_3).unwrap();

    for i in [0, 1, 2, 0, 1, 2, 0] {
        sc = bounce_req_rep(&mut sbs[i], sc);
    }

    // see comment on disconnect and unbind in single_connect
    for endpoint in [&endpoints[0], &my_endpoint_3, &endpoints[1]] {
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
fn multi_connect_same_port_ipv4() {
    multi_connect_same_port(false);
}

#[test]
fn single_connect_ipv6() {
    single_connect(true);
}

#[test]
fn multi_connect_ipv6() {
    multi_connect(true);
}

#[test]
fn multi_connect_same_port_ipv6() {
    multi_connect_same_port(true);
}
