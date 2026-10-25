//! libzmq's `tests/test_pair_inproc.cpp`: a PAIR round trip over inproc.

use zmq::{Context, Frame, Message, Pair, Socket};

use crate::support::{FIVE, bounce_pair, msg};

fn pair(context: &Context, name: &str) -> (Socket<Pair>, Socket<Pair>) {
    let mut sb = context.socket::<Pair>();
    sb.bind(&format!("inproc://{name}")).unwrap();
    let mut sc = context.socket::<Pair>();
    sc.connect(&format!("inproc://{name}")).unwrap();
    (sb, sc)
}

#[test]
fn roundtrip() {
    let context = Context::new().unwrap();
    let (mut sb, mut sc) = pair(&context, "pair_inproc_roundtrip");
    bounce_pair(&mut sb, &mut sc);
}

/// `zmq_send_const` sends from memory that outlives the message without
/// copying it; frames made from `&'static` data do the same.
#[test]
fn zmq_send_const() {
    let context = Context::new().unwrap();
    let (mut sb, mut sc) = pair(&context, "pair_inproc_send_const");
    let mut message = Message::new(Frame::from(b"foo".as_slice()));
    message.push(Frame::from(b"foobar".as_slice()));
    sb.send(message);
    assert_eq!(sc.recv_timeout(FIVE), Some(msg(&["foo", "foobar"])));
}
