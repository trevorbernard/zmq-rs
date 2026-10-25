//! libzmq's `tests/test_pair_ipc.cpp`: a PAIR round trip over ipc, and a
//! path too long for a Unix socket.
#![cfg(unix)]

use zmq::{Context, Error, Pair};

use crate::support::bounce_pair;

#[test]
fn roundtrip() {
    let context = Context::new().unwrap();
    let mut sb = context.socket::<Pair>();
    let endpoint = sb.bind("ipc://*").unwrap().to_string();
    let mut sc = context.socket::<Pair>();
    sc.connect(&endpoint).unwrap();
    bounce_pair(&mut sb, &mut sc);
}

/// libzmq fails with `ENAMETOOLONG`; here the operating system's refusal
/// comes back as `Error::Io`.
#[test]
fn endpoint_too_long() {
    let context = Context::new().unwrap();
    let mut sb = context.socket::<Pair>();
    let endpoint_too_long = format!("ipc://{}", "a".repeat(108));
    assert!(matches!(sb.bind(&endpoint_too_long), Err(Error::Io(_))));
}
