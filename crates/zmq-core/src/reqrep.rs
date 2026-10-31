//! REQ and REP (RFC 28): request envelopes, as libzmq's `req_t` and
//! `rep_t` handle them.
//!
//! A request travels as `[request id] "" body...`: an optional 4-octet
//! request id (libzmq's `ZMQ_REQ_CORRELATE`), an empty delimiter, then the
//! body. Routers on the way prepend identity frames, so a REP sees
//! `identity... "" body...`. It keeps everything up to the delimiter as the
//! envelope and sends it back in front of the reply.

use std::collections::VecDeque;
use std::fmt;
use std::task::{Context as TaskContext, Poll};

use zmq_pipe::TrySendError;
use zmtp::{Frame, Message};

use crate::peers::{PeerKey, PeerList, random_u32};
use crate::{FairQueue, LoadBalancer};

/// What happened to a reply REP was asked to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteOutcome {
    Sent,
    /// The requester is gone. The reply was dropped.
    UnknownPeer,
    /// The requester's queue is full. The reply was dropped.
    PeerFull,
}

/// REQ's side: sends each request to the next peer with room and accepts a
/// reply only from that peer.
#[derive(Debug)]
pub struct Requester {
    balancer: LoadBalancer,
    /// Reads only peers that sent something, as libzmq's REQ reads through
    /// its fair queue, so waiting for a reply does not look at every peer.
    replies: FairQueue,
    /// What the peer asked has sent, read along with the other peers'.
    from_asked: VecDeque<Message>,
    correlate: bool,
    next_id: u32,
}

/// A request that has been sent and awaits its reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outstanding {
    peer: PeerKey,
    id: Option<[u8; 4]>,
}

/// The peer a request went to closed before replying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerGone;

/// REP's side: receives requests fairly and replies along their envelope.
#[derive(Debug, Default)]
pub struct Replier {
    queue: FairQueue,
}

/// The frames a request arrived with, up to and including the empty
/// delimiter, which a reply must carry back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    /// Empty, and so unallocated, for the delimiter alone: a request
    /// straight from a REQ.
    frames: Vec<Frame>,
}

#[cfg(test)]
static DELIMITER: [Frame; 1] = [Frame::new()];

/// A request received by a REP: the peer, the envelope and the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Received {
    pub peer: PeerKey,
    pub envelope: Envelope,
    pub body: Message,
}

impl Default for Requester {
    fn default() -> Self {
        Self {
            balancer: LoadBalancer::default(),
            replies: FairQueue::default(),
            from_asked: VecDeque::new(),
            correlate: false,
            // libzmq starts request ids at a random value.
            next_id: random_u32(),
        }
    }
}

impl Requester {
    /// Whether requests carry a request id, so that replies to abandoned
    /// requests are recognised and dropped (`ZMQ_REQ_CORRELATE`).
    pub fn set_correlate(&mut self, correlate: bool) {
        self.correlate = correlate;
    }

    /// Sends `body` as a request to the next peer with room. Hands it back,
    /// with `cx` registered, if no peer has room.
    pub fn poll_send(
        &mut self,
        peers: &mut PeerList,
        body: Message,
        cx: &mut TaskContext<'_>,
    ) -> Result<Outstanding, Message> {
        // Left from the last request's peer: none of it answers this one.
        self.from_asked.clear();
        let id = self
            .correlate
            .then(|| self.next_id.wrapping_add(1).to_be_bytes());
        let (mut request, envelope) = match id {
            Some(id) => {
                let mut request = Message::new(Frame::copy_from_slice(&id));
                request.push(Frame::new());
                (request, 2)
            }
            None => (Message::new(Frame::new()), 1),
        };
        request.extend(body);
        match self.balancer.poll_send(peers, request, cx) {
            Ok(peer) => {
                if id.is_some() {
                    self.next_id = self.next_id.wrapping_add(1);
                }
                Ok(Outstanding { peer, id })
            }
            Err(refused) => {
                let body: Vec<Frame> = refused.into_iter().skip(envelope).collect();
                Err(Message::try_from(body).expect("a request has a body"))
            }
        }
    }

    /// The reply to `outstanding`. Messages from other peers, replies with
    /// the wrong request id and replies without a delimiter are dropped.
    pub fn poll_reply(
        &mut self,
        peers: &mut PeerList,
        outstanding: &Outstanding,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<Message, PeerGone>> {
        let from_asked = &mut self.from_asked;
        let mut gone = false;
        self.replies.poll_drain(peers, cx, |peer, message| {
            // RFC 28: messages from other peers are silently discarded, even
            // those that arrived before this request.
            if peer != outstanding.peer {
                return;
            }
            match message {
                Some(message) => from_asked.push_back(message),
                None => gone = true,
            }
        });
        while let Some(reply) = self.from_asked.pop_front() {
            if let Some(body) = unwrap_reply(reply, outstanding.id) {
                return Poll::Ready(Ok(body));
            }
        }
        if gone || peers.get(outstanding.peer).is_none() {
            return Poll::Ready(Err(PeerGone));
        }
        Poll::Pending
    }
}

impl Replier {
    /// The next well-formed request. Requests without a delimiter, or with
    /// no body after it, are dropped, as libzmq does.
    pub fn poll_recv(&mut self, peers: &mut PeerList, cx: &mut TaskContext<'_>) -> Poll<Received> {
        loop {
            let Poll::Ready((peer, message)) = self.queue.poll_recv(peers, cx) else {
                return Poll::Pending;
            };
            if let Some((envelope, body)) = split_envelope(message) {
                return Poll::Ready(Received {
                    peer,
                    envelope,
                    body,
                });
            }
        }
    }

    /// Sends `reply` to `peer` behind `envelope`. Never blocks: if the peer
    /// is gone or full, the reply is dropped.
    pub fn reply(
        peers: &mut PeerList,
        peer: PeerKey,
        envelope: Envelope,
        reply: Message,
    ) -> RouteOutcome {
        let Some(peer) = peers.get_mut(peer) else {
            return RouteOutcome::UnknownPeer;
        };
        let mut message = match Message::try_from(envelope.frames) {
            Ok(message) => message,
            Err(_) => Message::new(Frame::new()),
        };
        message.extend(reply);
        match peer.connection.tx.try_send(message) {
            Ok(()) => RouteOutcome::Sent,
            Err(TrySendError::Full(_)) => RouteOutcome::PeerFull,
            Err(TrySendError::Closed(_)) => RouteOutcome::UnknownPeer,
        }
    }
}

/// Strips a reply's request id (when correlating) and delimiter. `None` if
/// the reply is malformed or answers a different request.
fn unwrap_reply(reply: Message, id: Option<[u8; 4]>) -> Option<Message> {
    let mut frames = reply.into_iter();
    if let Some(id) = id
        && frames.next()?.as_slice() != id
    {
        return None;
    }
    if !frames.next()?.is_empty() {
        return None;
    }
    collect(frames)
}

/// Splits a request at its first empty frame. `None` if there is no
/// delimiter or nothing follows it.
fn split_envelope(request: Message) -> Option<(Envelope, Message)> {
    let mut frames = request.into_iter();
    let first = frames.next()?;
    if first.is_empty() {
        let body = collect(frames)?;
        return Some((Envelope { frames: Vec::new() }, body));
    }
    let mut envelope = vec![first];
    for frame in frames.by_ref() {
        let delimiter = frame.is_empty();
        envelope.push(frame);
        if delimiter {
            let body = collect(frames)?;
            return Some((Envelope { frames: envelope }, body));
        }
    }
    None
}

/// The frames as a message, without collecting them first. `None` if
/// there are none.
fn collect(mut frames: impl Iterator<Item = Frame>) -> Option<Message> {
    let mut message = Message::new(frames.next()?);
    message.extend(frames);
    Some(message)
}

impl Envelope {
    /// The envelope's frames, ending with the empty delimiter.
    #[cfg(test)]
    pub(crate) fn frames(&self) -> &[Frame] {
        if self.frames.is_empty() {
            &DELIMITER
        } else {
            &self.frames
        }
    }
}

impl fmt::Display for PeerGone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the peer closed before replying")
    }
}

impl std::error::Error for PeerGone {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    fn frames(parts: &[&'static str]) -> Message {
        Message::try_from(parts.iter().map(|p| Frame::from(*p)).collect::<Vec<_>>()).unwrap()
    }

    fn poll<T>(f: impl FnOnce(&mut TaskContext<'_>) -> Poll<T>) -> Poll<T> {
        let (_, waker) = waker();
        f(&mut TaskContext::from_waker(&waker))
    }

    // ---- Requester ----

    #[test]
    fn requests_carry_a_delimiter() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut requester = Requester::default();
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        requester
            .poll_send(&mut peers, msg("question"), &mut cx)
            .unwrap();
        assert_eq!(drain(&mut sa), [frames(&["", "question"])]);
    }

    #[test]
    fn the_reply_comes_without_its_delimiter() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut requester = Requester::default();
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let outstanding = requester.poll_send(&mut peers, msg("q"), &mut cx).unwrap();
        assert_eq!(
            requester.poll_reply(&mut peers, &outstanding, &mut cx),
            Poll::Pending
        );
        sa.to_socket.try_send(frames(&["", "answer"])).unwrap();
        assert_eq!(
            requester.poll_reply(&mut peers, &outstanding, &mut cx),
            Poll::Ready(Ok(msg("answer")))
        );
    }

    #[test]
    fn requests_round_robin_and_only_the_asked_peer_may_reply() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let mut requester = Requester::default();
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let first = requester.poll_send(&mut peers, msg("1"), &mut cx).unwrap();
        // The other peer's message is not a reply.
        sb.to_socket.try_send(frames(&["", "intruder"])).unwrap();
        sa.to_socket.try_send(frames(&["", "reply 1"])).unwrap();
        assert_eq!(
            requester.poll_reply(&mut peers, &first, &mut cx),
            Poll::Ready(Ok(msg("reply 1")))
        );
        let second = requester.poll_send(&mut peers, msg("2"), &mut cx).unwrap();
        assert_eq!(
            drain(&mut sb),
            [frames(&["", "2"])],
            "the next peer is asked"
        );
        assert_eq!(drain(&mut sa), [frames(&["", "1"])]);
        sb.to_socket.try_send(frames(&["", "reply 2"])).unwrap();
        assert_eq!(
            requester.poll_reply(&mut peers, &second, &mut cx),
            Poll::Ready(Ok(msg("reply 2"))),
            "the intruder's earlier message was discarded"
        );
    }

    #[test]
    fn replies_without_a_delimiter_are_dropped() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut requester = Requester::default();
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let outstanding = requester.poll_send(&mut peers, msg("q"), &mut cx).unwrap();
        sa.to_socket.try_send(msg("no delimiter")).unwrap();
        sa.to_socket.try_send(frames(&[""])).unwrap();
        sa.to_socket.try_send(frames(&["", "good"])).unwrap();
        assert_eq!(
            requester.poll_reply(&mut peers, &outstanding, &mut cx),
            Poll::Ready(Ok(msg("good")))
        );
    }

    #[test]
    fn correlated_requests_drop_stale_replies() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut requester = Requester::default();
        requester.set_correlate(true);
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let abandoned = requester
            .poll_send(&mut peers, msg("old"), &mut cx)
            .unwrap();
        let current = requester
            .poll_send(&mut peers, msg("new"), &mut cx)
            .unwrap();
        let sent = drain(&mut sa);
        assert_eq!(sent.len(), 2);
        let id = |m: &Message| m.get(0).unwrap().clone();
        assert_eq!(id(&sent[0]).len(), 4, "a 4-octet request id leads");
        assert_ne!(id(&sent[0]), id(&sent[1]), "each request has its own id");
        assert_eq!(sent[1].get(1), Some(&Frame::new()));
        // The peer answers both, in order.
        for (request, answer) in sent.iter().zip(["old answer", "new answer"]) {
            let mut reply = Message::new(id(request));
            reply.push(Frame::new());
            reply.push(answer);
            sa.to_socket.try_send(reply).unwrap();
        }
        let _ = abandoned;
        assert_eq!(
            requester.poll_reply(&mut peers, &current, &mut cx),
            Poll::Ready(Ok(msg("new answer")))
        );
    }

    #[test]
    fn a_closed_peer_ends_the_wait() {
        let (a, sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut requester = Requester::default();
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let outstanding = requester.poll_send(&mut peers, msg("q"), &mut cx).unwrap();
        drop(sa);
        assert_eq!(
            requester.poll_reply(&mut peers, &outstanding, &mut cx),
            Poll::Ready(Err(PeerGone))
        );
        assert!(peers.is_empty(), "the closed peer is removed");
    }

    #[test]
    fn a_peer_dropped_from_the_list_ends_the_wait() {
        let (a, _sa) = connection(10);
        let mut peers = list(vec![a]);
        let mut requester = Requester::default();
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let outstanding = requester.poll_send(&mut peers, msg("q"), &mut cx).unwrap();
        // As a disconnect drops it, with its session still open.
        peers.retain_mut(|_| false);
        assert_eq!(
            requester.poll_reply(&mut peers, &outstanding, &mut cx),
            Poll::Ready(Err(PeerGone))
        );
    }

    // ---- Replier ----

    #[test]
    fn requests_split_into_envelope_and_body() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        let key = peers.iter().next().unwrap().key();
        sa.to_socket
            .try_send(frames(&["hop2", "hop1", "", "q1", "q2"]))
            .unwrap();
        sa.to_socket.try_send(frames(&["", "plain"])).unwrap();
        let mut replier = Replier::default();
        let Poll::Ready(first) = poll(|cx| replier.poll_recv(&mut peers, cx)) else {
            panic!("a request is waiting");
        };
        assert_eq!(first.peer, key);
        assert_eq!(
            first.envelope.frames(),
            frames(&["hop2", "hop1", ""])
                .iter()
                .cloned()
                .collect::<Vec<_>>()
        );
        assert_eq!(first.body, frames(&["q1", "q2"]));
        let Poll::Ready(second) = poll(|cx| replier.poll_recv(&mut peers, cx)) else {
            panic!("a request is waiting");
        };
        assert_eq!(second.envelope.frames(), [Frame::new()]);
        assert_eq!(second.body, msg("plain"));
    }

    #[test]
    fn malformed_requests_are_dropped() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        sa.to_socket.try_send(msg("no delimiter")).unwrap();
        sa.to_socket.try_send(frames(&["id", ""])).unwrap();
        sa.to_socket.try_send(frames(&["", "good"])).unwrap();
        let mut replier = Replier::default();
        let Poll::Ready(request) = poll(|cx| replier.poll_recv(&mut peers, cx)) else {
            panic!("the good request is waiting");
        };
        assert_eq!(request.body, msg("good"));
        assert_eq!(poll(|cx| replier.poll_recv(&mut peers, cx)), Poll::Pending);
    }

    #[test]
    fn replies_go_back_along_the_envelope() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        sa.to_socket.try_send(frames(&["hop", "", "q"])).unwrap();
        let mut replier = Replier::default();
        let Poll::Ready(request) = poll(|cx| replier.poll_recv(&mut peers, cx)) else {
            panic!("a request is waiting");
        };
        let outcome = Replier::reply(
            &mut peers,
            request.peer,
            request.envelope,
            frames(&["a1", "a2"]),
        );
        assert_eq!(outcome, RouteOutcome::Sent);
        assert_eq!(drain(&mut sa), [frames(&["hop", "", "a1", "a2"])]);
    }

    #[test]
    fn replies_to_a_departed_peer_are_dropped() {
        let (a, mut sa) = connection(10);
        let mut peers = list(vec![a]);
        sa.to_socket.try_send(frames(&["", "q"])).unwrap();
        let mut replier = Replier::default();
        let Poll::Ready(request) = poll(|cx| replier.poll_recv(&mut peers, cx)) else {
            panic!("a request is waiting");
        };
        drop(sa);
        let outcome = Replier::reply(&mut peers, request.peer, request.envelope, msg("late"));
        assert_eq!(outcome, RouteOutcome::UnknownPeer);
    }
}
