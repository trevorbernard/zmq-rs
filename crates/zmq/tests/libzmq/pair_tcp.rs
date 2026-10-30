//! libzmq's `tests/test_pair_tcp.cpp`: a PAIR round trip over tcp,
//! connecting by address and by name. Not ported: the fastpath case, for
//! libzmq's Windows-only `ZMQ_LOOPBACK_FASTPATH`, and the Windows I/O
//! completion port case, which needs `ZMQ_FD`.

use zmq::{Context, Endpoint, Pair};
use zmq_test_ports::free_endpoint;

use crate::support::bounce_pair;

#[test]
fn pair_tcp_regular() {
    let context = Context::new().unwrap();
    let mut sb = context.socket::<Pair>();
    let endpoint = sb.bind(&free_endpoint()).unwrap().to_string();
    let mut sc = context.socket::<Pair>();
    sc.connect(&endpoint).unwrap();
    bounce_pair(&mut sb, &mut sc);
}

/// Connects to `localhost`, so that names are resolved, where the other
/// cases connect to the numeric address bound.
#[test]
fn pair_tcp_connect_by_name() {
    let context = Context::new().unwrap();
    let mut sb = context.socket::<Pair>();
    let Endpoint::Tcp(bound) = sb.bind(&free_endpoint()).unwrap() else {
        panic!("a tcp endpoint");
    };
    let mut sc = context.socket::<Pair>();
    sc.connect(&format!("tcp://localhost:{}", bound.port()))
        .unwrap();
    bounce_pair(&mut sb, &mut sc);
}
