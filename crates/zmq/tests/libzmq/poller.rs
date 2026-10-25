//! libzmq's `tests/test_poller.cpp`, onto `zmq::poll`, `zmq::poll_timeout`
//! and `PollItem::when`.
//!
//! Not ported:
//! - the null-pointer, size, add, modify, remove and invalid-events cases:
//!   `zmq::poll` takes its items for one call, so there is no poller to
//!   register with, and adding a socket twice does not compile, as both
//!   items would borrow it mutably;
//! - `test_poll_fd`, `test_call_poller_fd` and the `*_fd_*` cases: zmq-rs
//!   does not poll plain file descriptors;
//! - `test_call_poller_wait_empty_without_timeout_fails` and
//!   `..._wait_all_empty_with_timeout_fails`: polling no sockets without a
//!   timeout does not compile (see `zmq::poll`'s documentation).

use std::time::Duration;

use zmq::{Context, Message, Pull, Push};

use crate::support::FIVE;

fn pipeline(context: &Context) -> (zmq::Socket<Push>, zmq::Socket<Pull>) {
    let mut vent = context.socket::<Push>();
    let endpoint = vent.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut sink = context.socket::<Pull>();
    sink.connect(&endpoint).unwrap();
    (vent, sink)
}

#[test]
fn poll_basic() {
    let context = Context::new().unwrap();
    let (mut vent, mut sink) = pipeline(&context);

    vent.send("H");

    // We expect a message only on the sink
    assert_eq!(zmq::poll_timeout([sink.readable()], FIVE), Some([true]));
    assert_eq!(sink.try_recv(), Some(Message::from("H")));

    // We expect timed out
    assert_eq!(zmq::poll_timeout([sink.readable()], Duration::ZERO), None);
}

#[test]
fn call_poller_add_no_events() {
    // A socket polled for nothing is never reported, however ready.
    let context = Context::new().unwrap();
    let (mut vent, mut sink) = pipeline(&context);
    vent.send("H");
    assert_eq!(zmq::poll_timeout([sink.readable()], FIVE), Some([true]));

    assert_eq!(
        zmq::poll_timeout([sink.readable().when(false)], Duration::from_millis(100)),
        None
    );
}

#[test]
fn call_poller_modify_no_events() {
    // Polling a socket only sometimes: left out, then put back.
    let context = Context::new().unwrap();
    let (mut vent, mut sink) = pipeline(&context);
    vent.send("H");

    assert_eq!(
        zmq::poll_timeout([sink.readable().when(false)], Duration::from_millis(100)),
        None
    );
    assert_eq!(
        zmq::poll_timeout([sink.readable().when(true)], FIVE),
        Some([true])
    );
}

#[test]
fn call_poller_wait_empty_with_timeout_fails() {
    // Waiting on no sockets only times out.
    assert_eq!(zmq::poll_timeout([], Duration::ZERO), None);
}

#[test]
fn call_poller_wait_all_inf_disabled_fails() {
    let context = Context::new().unwrap();
    let (_vent, mut sink) = pipeline(&context);
    assert_eq!(
        zmq::poll_timeout([sink.readable().when(false)], Duration::ZERO),
        None
    );
}

#[cfg(feature = "draft")]
#[test]
fn poll_client_server() {
    use zmq::{Client, Frame, Server};

    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>();
    let endpoint = server.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut client = context.socket::<Client>();
    client.connect(&endpoint).unwrap();

    client.send("I");
    assert_eq!(zmq::poll_timeout([server.readable()], FIVE), Some([true]));
    let (_, frame) = server.try_recv().expect("readable");
    assert_eq!(frame, Frame::from("I"));

    // libzmq then polls the server for POLLOUT. A SERVER's sends never
    // wait, so it has no `writable` to poll (see `zmq::WaitsForRoom`); the
    // client's sends do.
    assert_eq!(zmq::poll_timeout([client.writable()], FIVE), Some([true]));
}
