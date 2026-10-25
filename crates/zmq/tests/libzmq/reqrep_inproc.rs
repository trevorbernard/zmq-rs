//! libzmq's `tests/test_reqrep_inproc.cpp`: a REQ-REP round trip over
//! inproc. Also holds the REQ helpers the other request-reply ports share.

use std::sync::mpsc;
use std::thread;

use zmq::{Context, Frame, Message, PeerGone, Rep, Req, Socket};

use crate::support::FIVE;

/// What libzmq's `bounce` sends, twice, as a two-part message.
pub(crate) const BOUNCE_CONTENT: &str = "12345678ABCDEFGH12345678abcdefgh";

/// A message of `parts`, as libzmq's `s_send_seq`; `""` is an empty frame.
pub(crate) fn msg(parts: &[&str]) -> Message {
    let frames: Vec<Frame> = parts
        .iter()
        .map(|part| Frame::from(part.to_string()))
        .collect();
    Message::try_from(frames).expect("at least one part")
}

/// A request made on another thread, so the test can play the peer
/// meanwhile, as libzmq's tests do by sending and receiving separately.
pub(crate) struct Requesting(mpsc::Receiver<(Socket<Req>, Result<Message, PeerGone>)>);

impl Requesting {
    pub(crate) fn start(mut req: Socket<Req>, message: Message) -> Self {
        let (done, result) = mpsc::channel();
        thread::spawn(move || {
            let reply = req.request(message);
            let _ = done.send((req, reply));
        });
        Self(result)
    }

    /// The socket back, and the reply.
    pub(crate) fn finish(self) -> (Socket<Req>, Message) {
        let (req, reply) = self.0.recv_timeout(FIVE).expect("a reply in time");
        (req, reply.expect("the peer stayed"))
    }
}

/// libzmq's `bounce`: a two-part request from `req`, checked at `rep`, and
/// the same two parts back.
pub(crate) fn bounce(rep: &mut Socket<Rep>, req: Socket<Req>) -> Socket<Req> {
    let content = msg(&[BOUNCE_CONTENT, BOUNCE_CONTENT]);
    let requesting = Requesting::start(req, content.clone());
    let request = rep.recv_timeout(FIVE).expect("the request");
    assert_eq!(request.body(), &content);
    request.reply(content.clone());
    let (req, reply) = requesting.finish();
    assert_eq!(reply, content);
    req
}

#[test]
fn roundtrip() {
    let context = Context::new().unwrap();
    let mut sb = context.socket::<Rep>();
    sb.bind("inproc://reqrep_inproc").unwrap();
    let mut sc = context.socket::<Req>();
    sc.connect("inproc://reqrep_inproc").unwrap();
    bounce(&mut sb, sc);
}
