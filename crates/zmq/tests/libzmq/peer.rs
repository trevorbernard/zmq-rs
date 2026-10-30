//! libzmq's `tests/test_peer.cpp`: PEER sockets address each other by the
//! id each gives the other, the connecting side's known at once from
//! `connect_peer`.

use zmq::{Context, Frame, Peer};
use zmq_test_ports::free_endpoint;

use crate::support::{FIVE, within};

#[test]
fn peer() {
    let context = Context::new().unwrap();
    let mut peer1 = context.socket::<Peer>();
    let endpoint = peer1.bind(&free_endpoint()).unwrap().to_string();

    let mut peer2 = context.socket::<Peer>();
    let peer1_id = peer2.connect_peer(&endpoint).unwrap();
    peer2.send(peer1_id, vec![1]).unwrap();

    let (peer2_id, frame, mut peer1) = within(FIVE, move || {
        let (id, frame) = peer1.recv();
        (id, frame, peer1)
    });
    assert_eq!(frame, Frame::from(vec![1]));

    peer1.send(peer2_id, vec![2]).unwrap();
    let (from, frame) = within(FIVE, move || peer2.recv());
    assert_eq!(from, peer1_id);
    assert_eq!(frame, Frame::from(vec![2]));
}
