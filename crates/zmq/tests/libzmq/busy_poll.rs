//! libzmq's `tests/test_busy_poll.cpp`: `ZMQ_BUSY_POLL`, with which a
//! socket still binds.

use std::time::Duration;

use zmq::{Context, Dealer};
use zmq_test_ports::free_endpoint;

#[test]
fn busy_poll() {
    let context = Context::new().unwrap();
    let mut socket = context.socket::<Dealer>();
    socket.set_busy_poll(Some(Duration::from_micros(1)));
    socket.bind(&free_endpoint()).unwrap();
}
