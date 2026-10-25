//! libzmq's `tests/test_req_correlate.cpp`: `ZMQ_REQ_CORRELATE`, a request
//! id in each request's envelope, so that a reply with the wrong id is
//! dropped.

use std::thread;

use zmq::{Context, Frame, Message, Req, Router};

use crate::support::{FIVE, within};

fn message(frames: &[&[u8]]) -> Message {
    Message::try_from(
        frames
            .iter()
            .map(|frame| Frame::copy_from_slice(frame))
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

#[test]
fn req_correlate() {
    let context = Context::new().unwrap();
    let mut req = context.socket::<Req>();
    let mut router = context.socket::<Router>();
    req.set_correlation(true);
    let endpoint = router.bind("tcp://127.0.0.1:*").unwrap().to_string();
    req.connect(&endpoint).unwrap();

    // A multipart request.
    let asking = thread::spawn(move || req.request(message(&[b"ABC", b"DEF"])));

    // The router sees the peer's routing id, the 4-octet request id, the
    // delimiter and the request.
    let (peer, request) = router.recv_timeout(FIVE).expect("the request");
    assert!(!peer.as_bytes().is_empty());
    let frames: Vec<Frame> = request.into_iter().collect();
    assert_eq!(frames.len(), 4);
    let request_id = frames[0].clone();
    assert_eq!(request_id.len(), 4);
    assert_eq!(
        &frames[1..],
        [Frame::new(), Frame::from("ABC"), Frame::from("DEF")]
    );

    // A reply with the wrong request id, then one with the right id.
    let mut bad_id = request_id.to_vec();
    bad_id[3] = bad_id[3].wrapping_add(1);
    router
        .send(&peer, message(&[&bad_id, b"", b"DATA"]))
        .unwrap();
    router
        .send(&peer, message(&[&request_id, b"", b"GHI"]))
        .unwrap();

    // Had the bad reply got through, this would be DATA.
    let reply = within(FIVE, move || asking.join().unwrap()).unwrap();
    assert_eq!(reply, Message::from("GHI"));
}
