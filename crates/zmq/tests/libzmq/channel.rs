//! libzmq's `tests/test_channel.cpp`: CHANNEL is PAIR in single frames.
//!
//! `test_sndmore_fails` has no port: CHANNEL's `send` takes one `Frame`,
//! so a multipart send does not compile.

use zmq::{Channel, Context, Frame, Socket};

use crate::support::{FIVE, within};

fn bound_and_connected(context: &Context, name: &str) -> (Socket<Channel>, Socket<Channel>) {
    let mut sb = context.socket::<Channel>();
    sb.bind(&format!("inproc://{name}")).unwrap();
    let mut sc = context.socket::<Channel>();
    sc.connect(&format!("inproc://{name}")).unwrap();
    (sb, sc)
}

#[test]
fn roundtrip() {
    let context = Context::new().unwrap();
    let (mut sb, mut sc) = bound_and_connected(&context, "channel-roundtrip");

    sb.send("HELLO");
    let (received, mut sc) = within(FIVE, move || (sc.recv(), sc));
    assert_eq!(received, Frame::from("HELLO"));

    sc.send("WORLD");
    assert_eq!(within(FIVE, move || sb.recv()), Frame::from("WORLD"));
}
