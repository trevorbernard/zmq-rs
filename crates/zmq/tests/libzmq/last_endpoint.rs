//! libzmq's `tests/test_last_endpoint.cpp`: binding reports where it bound,
//! as `ZMQ_LAST_ENDPOINT` does. zmq-rs returns it from `bind`.

use zmq::{Context, Router};
use zmq_test_ports::free_endpoint;

#[test]
fn last_endpoint() {
    let context = Context::new().unwrap();
    let mut router = context.socket::<Router>();
    for endpoint in [free_endpoint(), free_endpoint()] {
        let reported = router.bind(&endpoint).unwrap();
        assert_eq!(reported.to_string(), endpoint);
    }
}
