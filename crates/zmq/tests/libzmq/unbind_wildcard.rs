//! libzmq's `tests/test_unbind_wildcard.cpp`: a socket bound to a wildcard
//! address or port is unbound by the endpoint the bind reported, and a peer
//! connected to that endpoint disconnects from it.
//!
//! zmq-rs has no `ZMQ_IPV6`: an IPv6 address is bound or connected as
//! given, and `tcp://*` binds IPv4 only, as libzmq does with `ZMQ_IPV6`
//! off. So libzmq's `test_address_wildcard_ipv6`, which binds `tcp://*`
//! dual-stack, is not ported, and `test_port_wildcard_ipv4_address_ipv6`
//! would repeat `port_wildcard_ipv4_address`.

use std::net::TcpListener;

use zmq::{Context, Message, Rep, Req};

use crate::support::{FIVE, within};

/// A request from `client` answered by `server` with what it received, as
/// libzmq's `bounce` does over REQ and REP.
fn bounce(server: &mut zmq::Socket<Rep>, client: zmq::Socket<Req>) -> zmq::Socket<Req> {
    let mut request = Message::new("12345678ABCDEFGH12345678abcdefgh");
    request.push("12345678ABCDEFGH12345678abcdefgh");
    let asking = std::thread::spawn({
        let request = request.clone();
        move || {
            let mut client = client;
            let reply = client.request(request).unwrap();
            (client, reply)
        }
    });
    let received = server.recv_timeout(FIVE).expect("a request");
    assert_eq!(received.body(), &request);
    let body = received.body().clone();
    received.reply(body);
    let (client, reply) = within(FIVE, move || asking.join().unwrap());
    assert_eq!(reply, request);
    client
}

/// Binds `bind`, connects to the endpoint it reports, bounces a request,
/// then disconnects and unbinds by that endpoint.
fn round_trip_then_unbind(bind: &str) {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Rep>();
    let mut client = context.socket::<Req>();
    let endpoint = server.bind(bind).unwrap().to_string();
    client.connect(&endpoint).unwrap();
    let mut client = bounce(&mut server, client);
    client.disconnect(&endpoint).unwrap();
    server.unbind(&endpoint).unwrap();
}

#[test]
fn address_wildcard_ipv4() {
    round_trip_then_unbind("tcp://*:*");
}

#[test]
fn port_wildcard_ipv4_address() {
    round_trip_then_unbind("tcp://127.0.0.1:*");
}

#[test]
fn port_wildcard_ipv6_address() {
    if TcpListener::bind("[::1]:0").is_err() {
        eprintln!("ipv6 is not available");
        return;
    }
    round_trip_then_unbind("tcp://[::1]:*");
}
