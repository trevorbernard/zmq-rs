//! libzmq's `tests/test_poller.cpp`, onto `zmq::poll`, `zmq::poll_timeout`
//! and `PollItem::when`.
//!
//! Not ported:
//! - the null-pointer, size, add, modify, remove and invalid-events cases:
//!   `zmq::poll` takes its items for one call, so there is no poller to
//!   register with, and adding a socket twice does not compile, as both
//!   items would borrow it mutably;
//! - `test_call_poller_fd`, a poller's own descriptor, which zmq-rs has
//!   not. A plain file descriptor is polled in `zmq_poll_fd.rs` too;
//! - `test_call_poller_wait_all_empty_with_timeout_fails`: the same call
//!   here as `..._wait_empty_without_timeout_fails`, since `zmq::poll`
//!   stands for both `zmq_poller_wait` and `zmq_poller_wait_all`;
//! - `test_call_poller_wait_all_inf_disabled_fails`: its endless wait has
//!   no form here, and with a timeout it is `call_poller_add_no_events`
//!   on a socket with nothing to read, which passes whatever `when` does.

use std::os::fd::AsFd;
use std::time::Duration;

use zmq::{Context, Message, PollItem, Pull, Push};
use zmq_test_ports::free_endpoint;

use crate::support::FIVE;

fn pipeline(context: &Context) -> (zmq::Socket<Push>, zmq::Socket<Pull>) {
    let mut vent = context.socket::<Push>().unwrap();
    let endpoint = vent.bind(free_endpoint()).unwrap().to_string();
    let mut sink = context.socket::<Pull>().unwrap();
    sink.connect(&endpoint).unwrap();
    (vent, sink)
}

#[test]
fn poll_basic() {
    let context = Context::new().unwrap();
    let (mut vent, mut sink) = pipeline(&context);

    vent.send("H").unwrap();

    // We expect a message only on the sink
    assert_eq!(
        zmq::poll_timeout([sink.readable()], FIVE).unwrap(),
        Some([true])
    );
    assert_eq!(sink.try_recv().unwrap(), Some(Message::from("H")));

    // We expect timed out
    assert_eq!(
        zmq::poll_timeout([sink.readable()], Duration::ZERO).unwrap(),
        None
    );
}

#[test]
fn call_poller_add_no_events() {
    // A socket polled for nothing is never reported, however ready.
    let context = Context::new().unwrap();
    let (mut vent, mut sink) = pipeline(&context);
    vent.send("H").unwrap();
    assert_eq!(
        zmq::poll_timeout([sink.readable()], FIVE).unwrap(),
        Some([true])
    );

    assert_eq!(
        zmq::poll_timeout([sink.readable().when(false)], Duration::from_millis(100)).unwrap(),
        None
    );
}

#[test]
fn call_poller_modify_no_events() {
    // Polling a socket only sometimes: left out, then put back.
    let context = Context::new().unwrap();
    let (mut vent, mut sink) = pipeline(&context);
    vent.send("H").unwrap();

    assert_eq!(
        zmq::poll_timeout([sink.readable().when(false)], Duration::from_millis(100)).unwrap(),
        None
    );
    assert_eq!(
        zmq::poll_timeout([sink.readable().when(true)], FIVE).unwrap(),
        Some([true])
    );
}

#[test]
fn call_poller_wait_empty_with_timeout_fails() {
    // Waiting on no sockets only times out.
    assert_eq!(zmq::poll_timeout([], Duration::ZERO).unwrap(), None);
}

#[test]
#[should_panic(expected = "polling no sockets would wait forever")]
fn call_poller_wait_empty_without_timeout_fails() {
    // This would never be able to return since no socket was registered:
    // libzmq fails with EFAULT, and zmq-rs panics. An empty array does not
    // compile (see `zmq::poll`'s documentation).
    drop(zmq::poll(Vec::new()));
}

#[test]
fn poll_fd() {
    let context = Context::new().unwrap();
    let (_vent, mut bowl) = pipeline(&context);

    // Check we can poll an FD
    let fd = bowl.fd().unwrap();
    assert_eq!(
        zmq::poll_timeout(
            [PollItem::fd_readable(fd.as_fd()).unwrap()],
            Duration::from_millis(500)
        )
        .unwrap(),
        Some([true])
    );
}

#[cfg(feature = "draft")]
#[test]
fn poll_client_server() {
    use zmq::{Client, Frame, Server};

    let context = Context::new().unwrap();
    let mut server = context.socket::<Server>().unwrap();
    let endpoint = server.bind(free_endpoint()).unwrap().to_string();
    let mut client = context.socket::<Client>().unwrap();
    client.connect(&endpoint).unwrap();

    client.send("I").unwrap();
    assert_eq!(
        zmq::poll_timeout([server.readable()], FIVE).unwrap(),
        Some([true])
    );
    let (_, frame) = server.try_recv().unwrap().expect("readable");
    assert_eq!(frame, Frame::from("I"));

    // libzmq then polls the server for POLLOUT. A SERVER's sends never
    // wait, so it has no `writable` to poll (see `zmq::WaitsForRoom`); the
    // client's sends do.
    assert_eq!(
        zmq::poll_timeout([client.writable()], FIVE).unwrap(),
        Some([true])
    );
}
