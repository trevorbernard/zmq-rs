//! libzmq's `tests/test_last_endpoint.cpp`: binding reports where it bound,
//! as `ZMQ_LAST_ENDPOINT` does. zmq-rs returns it from `bind`.

use std::net::TcpListener;

use zmq::{Context, Router};

/// Two TCP endpoints on ports free now, standing in for libzmq's fixed
/// `ENDPOINT_1` and `ENDPOINT_2`. Both listeners are held until both ports
/// are known, so the two differ.
fn free_endpoints() -> [String; 2] {
    let listeners = [(); 2].map(|()| TcpListener::bind("127.0.0.1:0").unwrap());
    listeners.map(|listener| format!("tcp://{}", listener.local_addr().unwrap()))
}

#[test]
fn last_endpoint() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    for endpoint in free_endpoints() {
        let reported = router.bind(&endpoint).unwrap();
        assert_eq!(reported.to_string(), endpoint);
    }
}
