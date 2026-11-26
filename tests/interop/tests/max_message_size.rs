//! What `ZMQ_MAXMSGSIZE` limits, in libzmq and here: each frame, not the
//! frames of a message together.

use std::time::Duration;

use zmq::{Context, Frame, Message, Pull, Push, SocketOptions};
use zmq_interop::libzmq::{Kind, Socket};

const LIMIT: u64 = 100;
const SHORT: Duration = Duration::from_millis(500);

/// Two frames, each under the limit, that together are over it.
fn two_frames() -> [Vec<u8>; 2] {
    [vec![1; 80], vec![2; 80]]
}

#[test]
fn libzmq_takes_a_message_whose_frames_are_each_under_its_limit() {
    let pull = Socket::new(Kind::Pull);
    pull.set_max_message_size(LIMIT as i64);
    let addr = pull.bind_loopback();
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.connect(format!("tcp://{addr}")).unwrap();
    let [first, second] = two_frames();
    push.send(Message::from([
        Frame::from(first.clone()),
        Frame::from(second.clone()),
    ]))
    .unwrap();
    assert_eq!(pull.recv().unwrap(), [first, second]);
}

#[test]
fn libzmq_drops_a_peer_that_sends_a_frame_over_its_limit() {
    let pull = Socket::new(Kind::Pull);
    pull.set_max_message_size(LIMIT as i64);
    pull.set_receive_timeout(SHORT);
    let addr = pull.bind_loopback();
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.connect(format!("tcp://{addr}")).unwrap();
    push.send(vec![3; LIMIT as usize + 1]).unwrap();
    assert!(pull.recv().is_err());
}

#[test]
fn we_take_a_message_whose_frames_are_each_under_our_limit() {
    let context = Context::new().unwrap();
    let mut pull = context
        .socket_with::<Pull>(SocketOptions::new().max_message_size(LIMIT))
        .unwrap();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let push = Socket::new(Kind::Push);
    push.connect(bound);
    let [first, second] = two_frames();
    push.send(&[&first, &second]).unwrap();
    assert_eq!(
        pull.recv_timeout(SHORT).unwrap(),
        Some(Message::from([Frame::from(first), Frame::from(second)]))
    );
}

#[test]
fn we_drop_a_peer_that_sends_a_frame_over_our_limit() {
    let context = Context::new().unwrap();
    let mut pull = context
        .socket_with::<Pull>(SocketOptions::new().max_message_size(LIMIT))
        .unwrap();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    let push = Socket::new(Kind::Push);
    push.connect(bound);
    push.send(&[&vec![3; LIMIT as usize + 1]]).unwrap();
    assert_eq!(pull.recv_timeout(SHORT).unwrap(), None);
}
