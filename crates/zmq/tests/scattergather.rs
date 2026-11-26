//! SCATTER and GATHER (RFC 49): PUSH and PULL for single-part messages.
//! Draft sockets.
#![cfg(feature = "draft")]

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Frame, Gather, Scatter};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);

/// Runs `f` on another thread and fails the test if it takes longer than
/// `limit`, so a lost wakeup fails instead of hanging the suite.
fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
        .recv_timeout(limit)
        .unwrap_or_else(|_| panic!("timed out after {limit:?}"))
}

#[test]
fn scatter_and_gather_over_tcp() {
    let context = Context::new().unwrap();
    let mut gather = context.socket::<Gather>().unwrap();
    let bound = gather.bind(free_endpoint()).unwrap();
    let mut scatter = context.socket::<Scatter>().unwrap();
    scatter.connect(&bound).unwrap();
    for i in 0..10 {
        scatter.send(format!("{i}")).unwrap();
    }
    let received = within(FIVE, move || {
        (0..10).map(|_| gather.recv().unwrap()).collect::<Vec<_>>()
    });
    let expected: Vec<Frame> = (0..10).map(|i| Frame::from(format!("{i}"))).collect();
    assert_eq!(received, expected);
}

#[test]
fn scatter_round_robins_across_gathers() {
    let context = Context::new().unwrap();
    let mut gathers: Vec<_> = (0..2)
        .map(|n| {
            let mut gather = context.socket::<Gather>().unwrap();
            gather.bind(format!("inproc://gather-{n}")).unwrap();
            gather
        })
        .collect();
    let mut scatter = context.socket::<Scatter>().unwrap();
    for n in 0..2 {
        scatter.connect(format!("inproc://gather-{n}")).unwrap();
    }
    for i in 0..4 {
        scatter.send(format!("{i}")).unwrap();
    }
    assert_eq!(gathers[0].try_recv().unwrap(), Some(Frame::from("0")));
    assert_eq!(gathers[1].try_recv().unwrap(), Some(Frame::from("1")));
    assert_eq!(gathers[0].try_recv().unwrap(), Some(Frame::from("2")));
    assert_eq!(gathers[1].try_recv().unwrap(), Some(Frame::from("3")));
}

#[test]
fn gather_fair_queues_from_scatters() {
    let context = Context::new().unwrap();
    let mut gather = context.socket::<Gather>().unwrap();
    gather.bind("inproc://fair").unwrap();
    let mut scatters: Vec<_> = (0..2)
        .map(|_| {
            let mut scatter = context.socket::<Scatter>().unwrap();
            scatter.connect("inproc://fair").unwrap();
            scatter
        })
        .collect();
    for (n, scatter) in scatters.iter_mut().enumerate() {
        for i in 0..2 {
            scatter.send(format!("{n}.{i}")).unwrap();
        }
    }
    let received: Vec<Frame> = (0..4)
        .map(|_| gather.try_recv().unwrap().unwrap())
        .collect();
    assert_eq!(
        received,
        ["0.0", "1.0", "0.1", "1.1"].map(Frame::from),
        "alternating between scatters"
    );
}

#[test]
fn a_scatter_without_gathers_hands_the_frame_back() {
    let context = Context::new().unwrap();
    let mut scatter = context.socket::<Scatter>().unwrap();
    assert_eq!(
        scatter.try_send("nobody"),
        Err(zmq::TrySendError::Full(Frame::from("nobody")))
    );
}

#[test]
fn inproc_pairs_a_gather_with_any_socket_type() {
    // As in libzmq, where only a handshake checks socket types.
    let context = Context::new().unwrap();
    let mut gather = context.socket::<Gather>().unwrap();
    gather.bind("inproc://strangers").unwrap();
    let mut push = context.socket::<zmq::Push>().unwrap();
    push.connect("inproc://strangers").unwrap();
    push.try_send("not a scatter").unwrap();
    let frame = gather
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .expect("the push's message");
    assert_eq!(frame, "not a scatter");
}
