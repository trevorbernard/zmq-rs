//! Undoing binds and connects, as libzmq's `zmq_unbind` and
//! `zmq_disconnect`, and spreading connections over I/O threads.

use std::time::Duration;

use zmq::{Context, Pull, Push};

const FIVE: Duration = Duration::from_secs(5);

#[test]
fn connections_spread_over_several_io_threads() {
    let context = Context::with_io_threads(3).unwrap();
    let mut pulls: Vec<_> = (0..3).map(|_| context.socket::<Pull>()).collect();
    let mut push = context.socket::<Push>();
    for pull in &mut pulls {
        let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
        push.connect(&bound.to_string()).unwrap();
    }
    for i in 0..3 {
        push.send(format!("m{i}"));
    }
    for pull in &mut pulls {
        assert!(pull.recv_timeout(FIVE).is_some(), "every peer gets one");
    }
}

#[test]
fn a_context_needs_an_io_thread() {
    let err = Context::with_io_threads(0).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}
