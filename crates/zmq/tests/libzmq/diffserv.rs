//! libzmq's `tests/test_diffserv.cpp`: `ZMQ_TOS`, the type of service of a
//! socket's TCP connections.
//!
//! libzmq reads the option back with `zmq_getsockopt`, which returns what
//! was set. Ours is a field of `SocketOptions`, so there is nothing to read
//! back; that the sockets carry it is tested in `zmq-core`'s `transport`.

use zmq::{Context, Pair, SocketOptions};
use zmq_test_ports::free_endpoint;

use crate::support::bounce_pair;

fn serviced(type_of_service: u8) -> SocketOptions {
    SocketOptions {
        type_of_service,
        ..SocketOptions::default()
    }
}

#[test]
fn diffserv() {
    let context = Context::new().unwrap();
    let mut sb = context.socket_with::<Pair>(serviced(0x28));
    let my_endpoint = sb.bind(&free_endpoint()).unwrap().to_string();

    let mut sc = context.socket_with::<Pair>(serviced(0x58));
    sc.connect(&my_endpoint).unwrap();

    bounce_pair(&mut sb, &mut sc);
}
