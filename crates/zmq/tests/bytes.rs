//! The `bytes` feature: a frame and a `bytes::Bytes` convert both ways
//! without copying a payload too large to hold inline.
#![cfg(feature = "bytes")]

use bytes::Bytes;
use zmq::{Context, Frame, Pair};

#[test]
fn bytes_become_a_frame_and_back_sharing_the_buffer() {
    let payload = Bytes::from(vec![7; 1024]);
    let buffer = payload.as_ptr();
    let frame = Frame::from(payload);
    assert_eq!(frame.as_ptr(), buffer);
    assert_eq!(Bytes::from(frame).as_ptr(), buffer);
}

#[test]
fn a_small_payload_is_copied_inline() {
    let frame = Frame::from(Bytes::from_static(b"abc"));
    assert_eq!(frame, Frame::from("abc"));
    assert_eq!(Bytes::from(frame), &b"abc"[..]);
}

#[test]
fn bytes_sent_over_inproc_arrive_in_the_same_buffer() {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Pair>();
    server.bind("inproc://bytes-feature").unwrap();
    let mut client = context.socket::<Pair>();
    client.connect("inproc://bytes-feature").unwrap();

    let payload = Bytes::from(vec![7; 1024]);
    let buffer = payload.as_ptr();
    client.send(Frame::from(payload)).unwrap();
    let received = server.recv().unwrap();
    let frame = received.into_iter().next().unwrap();
    assert_eq!(Bytes::from(frame).as_ptr(), buffer);
}
