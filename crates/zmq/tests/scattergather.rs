//! SCATTER and GATHER (RFC 49): PUSH and PULL for single-part messages.
//! Draft sockets.
#![cfg(feature = "draft")]

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Frame, Gather, Scatter};

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
    let mut gather = context.socket::<Gather>();
    let bound = gather.bind("tcp://127.0.0.1:*").unwrap();
    let mut scatter = context.socket::<Scatter>();
    scatter.connect(&bound.to_string()).unwrap();
    for i in 0..10 {
        scatter.send(format!("{i}"));
    }
    let received = within(FIVE, move || {
        (0..10).map(|_| gather.recv()).collect::<Vec<_>>()
    });
    let expected: Vec<Frame> = (0..10).map(|i| Frame::from(format!("{i}"))).collect();
    assert_eq!(received, expected);
}

#[test]
fn scatter_round_robins_across_gathers() {
    let context = Context::new().unwrap();
    let mut gathers: Vec<_> = (0..2)
        .map(|n| {
            let mut gather = context.socket::<Gather>();
            gather.bind(&format!("inproc://gather-{n}")).unwrap();
            gather
        })
        .collect();
    let mut scatter = context.socket::<Scatter>();
    for n in 0..2 {
        scatter.connect(&format!("inproc://gather-{n}")).unwrap();
    }
    for i in 0..4 {
        scatter.send(format!("{i}"));
    }
    assert_eq!(gathers[0].try_recv(), Some(Frame::from("0")));
    assert_eq!(gathers[1].try_recv(), Some(Frame::from("1")));
    assert_eq!(gathers[0].try_recv(), Some(Frame::from("2")));
    assert_eq!(gathers[1].try_recv(), Some(Frame::from("3")));
}

#[test]
fn gather_fair_queues_from_scatters() {
    let context = Context::new().unwrap();
    let mut gather = context.socket::<Gather>();
    gather.bind("inproc://fair").unwrap();
    let mut scatters: Vec<_> = (0..2)
        .map(|_| {
            let mut scatter = context.socket::<Scatter>();
            scatter.connect("inproc://fair").unwrap();
            scatter
        })
        .collect();
    for (n, scatter) in scatters.iter_mut().enumerate() {
        for i in 0..2 {
            scatter.send(format!("{n}.{i}"));
        }
    }
    let received: Vec<Frame> = (0..4).map(|_| gather.try_recv().unwrap()).collect();
    assert_eq!(
        received,
        ["0.0", "1.0", "0.1", "1.1"].map(Frame::from),
        "alternating between scatters"
    );
}

#[test]
fn a_scatter_without_gathers_hands_the_frame_back() {
    let context = Context::new().unwrap();
    let mut scatter = context.socket::<Scatter>();
    assert_eq!(scatter.try_send("nobody"), Err(Frame::from("nobody")));
}

#[test]
fn only_scatters_and_gathers_pair() {
    let context = Context::new().unwrap();
    let mut gather = context.socket::<Gather>();
    gather.bind("inproc://strangers").unwrap();
    let mut push = context.socket::<zmq::Push>();
    push.connect("inproc://strangers").unwrap();
    assert!(push.try_send("not a scatter").is_err());
    assert_eq!(gather.try_recv(), None);
}
