//! libzmq's `tests/test_scatter_gather.cpp`: SCATTER load-balances single
//! frames over its GATHER peers.
//!
//! `test_scatter_gather_multipart_fails` has no port: SCATTER's `send`
//! takes one `Frame`, so a multipart send does not compile.

use zmq::{Context, Frame, Gather, Scatter};

use crate::support::{FIVE, within};

#[test]
fn scatter_gather() {
    let context = Context::new().unwrap();
    let mut scatter = context.socket::<Scatter>();
    scatter.bind("inproc://test-scatter-gather").unwrap();
    let mut gather = context.socket::<Gather>();
    gather.connect("inproc://test-scatter-gather").unwrap();
    let mut gather2 = context.socket::<Gather>();
    gather2.connect("inproc://test-scatter-gather").unwrap();

    scatter.send("1");
    scatter.send("2");

    let (first, second) = within(FIVE, move || (gather.recv(), gather2.recv()));
    assert_eq!(first, Frame::from("1"));
    assert_eq!(second, Frame::from("2"));
}
