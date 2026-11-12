//! libzmq's `tests/test_pair_tcp_cap_net_admin.cpp`: `ZMQ_BINDTODEVICE`, a
//! PAIR round trip between sockets bound to the loopback device.
//!
//! Linux only, as `SO_BINDTODEVICE` is. libzmq skips the test where the
//! option is not supported; ours also where it is not permitted, as before
//! Linux 5.7 without `CAP_NET_RAW`.
#![cfg(any(target_os = "linux", target_os = "android"))]

use std::io;

use zmq::{Context, Error, Pair, SocketOptions};
use zmq_test_ports::free_endpoint;

use crate::support::bounce_pair;

fn bound_to_device() -> SocketOptions {
    SocketOptions {
        bind_to_device: Some("lo".to_owned()),
        ..SocketOptions::default()
    }
}

#[test]
fn pair_tcp_bind_to_device() {
    let context = Context::new().unwrap();
    let mut sb = context.socket_with::<Pair>(bound_to_device());
    let my_endpoint = match sb.bind(&free_endpoint()) {
        Ok(endpoint) => endpoint.to_string(),
        Err(Error::Io(err)) if err.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("SO_BINDTODEVICE not permitted");
            return;
        }
        Err(err) => panic!("{err}"),
    };

    let mut sc = context.socket_with::<Pair>(bound_to_device());
    sc.connect(&my_endpoint).unwrap();

    bounce_pair(&mut sb, &mut sc);
}
