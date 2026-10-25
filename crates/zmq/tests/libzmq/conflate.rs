//! libzmq's `tests/test_conflate.cpp`: a conflating PULL keeps only the
//! latest of the messages sent to it.

use zmq::{Context, Pull, Push};

use crate::support::{FIVE, settle};

#[test]
fn conflate() {
    let context = Context::new().unwrap();
    let mut s_in = context.socket::<Pull>();
    s_in.set_conflate(true);
    let endpoint = s_in.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut s_out = context.socket::<Push>();
    s_out.connect(&endpoint).unwrap();

    let message_count = 20;
    for j in 0..message_count {
        s_out.send(i32::to_le_bytes(j).to_vec());
    }
    settle();

    let received = s_in.recv_timeout(FIVE).expect("the latest message");
    let payload = i32::from_le_bytes(received.last()[..].try_into().unwrap());
    assert_eq!(payload, message_count - 1);
}
