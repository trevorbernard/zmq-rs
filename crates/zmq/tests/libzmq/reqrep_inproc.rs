//! libzmq's `tests/test_reqrep_inproc.cpp`: a REQ-REP round trip over
//! inproc.

use zmq::{Context, Rep, Req};

use crate::support::bounce_req_rep;

#[test]
fn roundtrip() {
    let context = Context::new().unwrap();
    let mut sb = context.socket::<Rep>();
    sb.bind("inproc://reqrep_inproc").unwrap();
    let mut sc = context.socket::<Req>();
    sc.connect("inproc://reqrep_inproc").unwrap();
    bounce_req_rep(&mut sb, sc);
}
