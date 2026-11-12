//! libzmq's `tests/test_bind_src_address.cpp`: a connect's source address,
//! `tcp://source;destination`.
//!
//! libzmq resolves the source only when it connects, so its `connect`
//! accepts `localhost` as one and then never connects, as a source is never
//! looked up. Ours resolves the source at once and refuses that endpoint:
//! see `docs/rfc-compliance.md`, "`unbind` and `disconnect`".

use zmq::{Context, Error, Pub};

#[test]
fn test_x() {
    let context = Context::new().unwrap();
    let mut sock = context.socket::<Pub>();
    sock.connect("tcp://127.0.0.1:0;localhost:1234").unwrap();
    assert!(matches!(
        sock.connect("tcp://localhost:5555;localhost:1235"),
        Err(Error::InvalidEndpoint(_))
    ));
    // The loopback interface by name, as Linux names it.
    #[cfg(target_os = "linux")]
    sock.connect("tcp://lo:5555;localhost:1235").unwrap();
}
