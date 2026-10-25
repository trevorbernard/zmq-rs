//! What the ported tests share, as libzmq's `testutil.hpp`.

#![allow(dead_code)]

use std::panic;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;

use zmq::{Dealer, Frame, Linger, Message, Pair, PeerGone, Rep, Req, Socket, SocketOptions};

/// How long a test waits for something that should happen.
pub(crate) const FIVE: Duration = Duration::from_secs(5);

/// How long a test waits to see that something does not happen, as
/// libzmq's `SETTLE_TIME`.
pub(crate) const SETTLE: Duration = Duration::from_millis(300);

/// Runs `f` on another thread and fails the test if it takes longer than
/// `limit`, so a lost wakeup fails instead of hanging the suite. A panic in
/// `f` is passed on as it is.
pub(crate) fn within<T: Send + 'static>(
    limit: Duration,
    f: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (done, result) = mpsc::channel();
    let running = thread::spawn(move || {
        let _ = done.send(f());
    });
    match result.recv_timeout(limit) {
        Ok(value) => value,
        Err(RecvTimeoutError::Timeout) => panic!("timed out after {limit:?}"),
        Err(RecvTimeoutError::Disconnected) => match running.join() {
            Err(panicked) => panic::resume_unwind(panicked),
            Ok(()) => unreachable!("f returned without sending its result"),
        },
    }
}

/// Waits briefly for messages in flight to arrive, as libzmq's
/// `msleep (SETTLE_TIME)`.
pub(crate) fn settle() {
    thread::sleep(SETTLE);
}

/// Options for a socket closed with zero linger, as libzmq's tests close
/// theirs, so a message queued to a peer that never completes its
/// handshake does not hold the context's drop.
pub(crate) fn zero_linger() -> SocketOptions {
    SocketOptions {
        linger: Linger::For(Duration::ZERO),
        ..SocketOptions::default()
    }
}

/// A message of `parts`, as libzmq's `s_send_seq`; `""` is an empty frame.
pub(crate) fn msg(parts: &[&str]) -> Message {
    let frames: Vec<Frame> = parts
        .iter()
        .map(|part| Frame::from(part.to_string()))
        .collect();
    Message::try_from(frames).expect("at least one part")
}

/// What libzmq's `bounce` sends, twice, as a two-part message.
pub(crate) const BOUNCE_CONTENT: &str = "12345678ABCDEFGH12345678abcdefgh";

/// The two-part message libzmq's `bounce` sends.
pub(crate) fn bounce_content() -> Message {
    msg(&[BOUNCE_CONTENT, BOUNCE_CONTENT])
}

/// libzmq's `bounce` between DEALERs: a two-part message from `client` to
/// `server`, and the same back.
pub(crate) fn bounce_dealer(server: &mut Socket<Dealer>, client: &mut Socket<Dealer>) {
    let content = bounce_content();
    client
        .send_timeout(content.clone(), FIVE)
        .expect("sent to the server");
    assert_eq!(server.recv_timeout(FIVE), Some(content.clone()));
    server
        .send_timeout(content.clone(), FIVE)
        .expect("sent back");
    assert_eq!(client.recv_timeout(FIVE), Some(content));
}

/// libzmq's `bounce` between PAIRs.
pub(crate) fn bounce_pair(server: &mut Socket<Pair>, client: &mut Socket<Pair>) {
    let content = bounce_content();
    client
        .send_timeout(content.clone(), FIVE)
        .expect("sent to the server");
    assert_eq!(server.recv_timeout(FIVE), Some(content.clone()));
    server
        .send_timeout(content.clone(), FIVE)
        .expect("sent back");
    assert_eq!(client.recv_timeout(FIVE), Some(content));
}

/// libzmq's `bounce` between a REQ and a REP: a two-part request from
/// `req`, checked at `rep`, and the same two parts back. Hands `req` back.
pub(crate) fn bounce_req_rep(rep: &mut Socket<Rep>, req: Socket<Req>) -> Socket<Req> {
    let content = bounce_content();
    let requesting = Requesting::start(req, content.clone());
    let request = rep.recv_timeout(FIVE).expect("the request");
    assert_eq!(request.body(), &content);
    request.reply(content.clone());
    let (req, reply) = requesting.finish();
    assert_eq!(reply, content);
    req
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
