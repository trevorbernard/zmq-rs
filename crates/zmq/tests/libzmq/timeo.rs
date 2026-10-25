//! libzmq's `tests/test_timeo.cpp`: receive timeouts, as `ZMQ_RCVTIMEO`.
//! zmq-rs gives the timeout per call, with `recv_timeout`, rather than as
//! a socket option.

use std::time::{Duration, Instant};

use zmq::{Context, Dealer, Message};

use crate::support::{FIVE, within};

#[test]
fn timeo() {
    let context = Context::new().unwrap();
    let mut frontend = context.socket::<Dealer>();
    frontend.bind("inproc://timeout_test").unwrap();

    // A receive on a socket with no peers returns at once.
    assert_eq!(frontend.try_recv(), None);

    // The receive timeout is honored.
    let timeout = Duration::from_millis(250);
    let jitter = Duration::from_millis(50);
    let started = Instant::now();
    assert_eq!(frontend.recv_timeout(timeout), None);
    let elapsed = started.elapsed();
    assert!(elapsed > timeout - jitter, "returned after {elapsed:?}");

    // Messages still flow as usual.
    let mut backend = context.socket::<Dealer>();
    backend.connect("inproc://timeout_test").unwrap();
    assert!(backend.send_timeout("Hello", timeout).is_ok());
    let received = within(FIVE, move || frontend.recv());
    assert_eq!(received, Message::from("Hello"));
}
