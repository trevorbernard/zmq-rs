//! libzmq's `tests/test_connect_resolve.cpp`: which TCP endpoints connect
//! accepts. libzmq's `test_x` is defined but never run, so it is not
//! ported.

use zmq::{Context, Error, Pub};

fn connect(endpoint: &str) -> Result<(), Error> {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.connect(endpoint)
}

#[test]
fn hostname_ipv4() {
    connect("tcp://localhost:1234").unwrap();
}

#[test]
fn loopback_ipv6() {
    connect("tcp://[::1]:1234").unwrap();
}

#[test]
fn hostname_with_spaces_fails() {
    assert!(connect("tcp://in val id:1234").is_err());
}

#[test]
fn no_hostname_fails() {
    assert!(connect("tcp://").is_err());
}

#[test]
fn invalid_service_fails() {
    assert!(connect("tcp://localhost:invalid").is_err());
}

#[test]
fn invalid_proto_fails() {
    // libzmq's EPROTONOSUPPORT.
    assert!(matches!(
        connect("invalid://localhost:1234"),
        Err(Error::UnsupportedTransport(_))
    ));
}
