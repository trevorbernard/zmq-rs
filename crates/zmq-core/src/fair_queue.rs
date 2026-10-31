//! The routing strategy of RFC 28-31 for receives: round robin over peers
//! with input.

use std::collections::VecDeque;
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context as TaskContext, Poll, Wake, Waker};

use atomic_waker::AtomicWaker;
use zmtp::Message;

use crate::peers::{PeerCursor, PeerKey, PeerList, registers};

/// Round robin over the peers that have a message waiting, like libzmq's
/// `fq_t`. Drops peers whose sessions are gone once drained.
///
/// Like `fq_t`, it reads only peers that may have input. A peer found
/// empty leaves the turn until its pipe wakes it, through a waker of its
/// own that marks it ready, as a pipe's `activate_read` does in libzmq.
/// Polling every peer instead cost a pipe check, a waker registration and a
/// later wakeup per empty peer per message.
#[derive(Debug, Default)]
pub struct FairQueue {
    /// Positions in the peer list of peers that may have input, in turn
    /// order.
    turn: VecDeque<usize>,
    /// Each peer's waker, in the peer list's order: entry `i` is for the
    /// list's peer `i`, and peers past the end are new.
    waiting: Vec<Waiting>,
    ready: Arc<Ready>,
    /// Keys taken from `ready`, kept for its allocation.
    woken: Vec<PeerKey>,
    cursor: PeerCursor,
}

/// The most messages [`FairQueue::poll_drain`] reads from one peer in a call.
const DRAIN_BATCH: usize = 256;

/// How [`FairQueue::read_turn`] left a peer.
enum Drained {
    /// It read a batch; the peer may have more.
    Batch,
    /// The peer has nothing more for now.
    Empty,
    /// The peer's session is gone and everything it sent was read.
    Gone,
}

/// A peer's waker, and whether the peer is in the turn.
#[derive(Debug)]
struct Waiting {
    key: PeerKey,
    wake: Arc<PeerWake>,
    waker: Waker,
    in_turn: bool,
}

/// Peers whose pipes woke them, and the task waiting on any of them.
#[derive(Debug, Default)]
struct Ready {
    keys: Mutex<Vec<PeerKey>>,
    /// Raised after a key is added, so finding none costs one load.
    any: AtomicBool,
    task: AtomicWaker,
}

impl Ready {
    /// Ignores poisoning: keys are only pushed or swapped out whole, so a
    /// panic while holding the lock cannot leave the list half updated.
    fn keys(&self) -> MutexGuard<'_, Vec<PeerKey>> {
        self.keys.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Wakes the task on behalf of one peer, noting which.
#[derive(Debug)]
struct PeerWake {
    key: PeerKey,
    /// Set while the key waits in `Ready::keys`, so it is added once.
    queued: AtomicBool,
    ready: Arc<Ready>,
}

impl Wake for PeerWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.queued.swap(true, Ordering::AcqRel) {
            return;
        }
        let ready = &self.ready;
        ready.keys().push(self.key);
        ready.any.store(true, Ordering::Release);
        ready.task.wake();
    }
}

impl FairQueue {
    /// The next message and the peer it came from, or `Pending` with `cx`
    /// woken once any peer has another.
    pub fn poll_recv(
        &mut self,
        peers: &mut PeerList,
        cx: &mut TaskContext<'_>,
    ) -> Poll<(PeerKey, Message)> {
        self.sync(peers);
        loop {
            self.take_woken();
            while let Some(index) = self.turn.pop_front() {
                let waiting = &mut self.waiting[index];
                let peer = &mut peers.peers[index];
                let polled = peer
                    .connection
                    .rx
                    .poll_recv(&mut TaskContext::from_waker(&waiting.waker));
                match polled {
                    Poll::Ready(Some(message)) => {
                        self.turn.push_back(index);
                        return Poll::Ready((waiting.key, message));
                    }
                    // Session gone and everything it delivered has been read.
                    Poll::Ready(None) => self.forget(peers, index),
                    Poll::Pending => waiting.in_turn = false,
                }
            }
            // Registered before looking again: a peer woken after the look
            // finds the task registered, and one woken before is seen.
            if registers(cx) {
                self.ready.task.register(cx.waker());
            }
            if !self.ready.any.load(Ordering::Acquire) {
                return Poll::Pending;
            }
        }
    }

    /// Hands `f` everything peers have sent, a peer at a time, as libzmq's
    /// XPUB reads a pipe through once it is activated, then `None` for a
    /// peer whose session is gone. Peers are read in the order they woke.
    /// Registers no task, so one registered earlier stays in place.
    pub(crate) fn drain(&mut self, peers: &mut PeerList, f: impl FnMut(PeerKey, Option<Message>)) {
        self.read_turn(peers, usize::MAX, f);
    }

    /// As [`drain`](Self::drain), but reads at most [`DRAIN_BATCH`]
    /// messages from each peer, then registers `cx` to be woken once any
    /// peer sends more, or at once if one has more to read already. A peer
    /// that never stops sending cannot keep the task here, past its
    /// deadline.
    pub(crate) fn poll_drain(
        &mut self,
        peers: &mut PeerList,
        cx: &mut TaskContext<'_>,
        f: impl FnMut(PeerKey, Option<Message>),
    ) {
        self.read_turn(peers, DRAIN_BATCH, f);
        // Registered before looking, as in `poll_recv`.
        if registers(cx) {
            self.ready.task.register(cx.waker());
        }
        if !self.turn.is_empty() || self.ready.any.load(Ordering::Acquire) {
            cx.waker().wake_by_ref();
        }
    }

    /// Reads each peer in the turn once, up to `batch` messages; a peer
    /// with more takes another turn.
    fn read_turn(
        &mut self,
        peers: &mut PeerList,
        batch: usize,
        mut f: impl FnMut(PeerKey, Option<Message>),
    ) {
        self.sync(peers);
        self.take_woken();
        for _ in 0..self.turn.len() {
            let Some(index) = self.turn.pop_front() else {
                break;
            };
            let waiting = &mut self.waiting[index];
            let key = waiting.key;
            let mut cx = TaskContext::from_waker(&waiting.waker);
            let rx = &mut peers.peers[index].connection.rx;
            let mut left = batch;
            let read = loop {
                if left == 0 {
                    break Drained::Batch;
                }
                left -= 1;
                match rx.poll_recv(&mut cx) {
                    Poll::Ready(Some(message)) => f(key, Some(message)),
                    Poll::Ready(None) => break Drained::Gone,
                    Poll::Pending => break Drained::Empty,
                }
            };
            match read {
                Drained::Batch => self.turn.push_back(index),
                Drained::Empty => self.waiting[index].in_turn = false,
                Drained::Gone => {
                    self.forget(peers, index);
                    f(key, None);
                }
            }
        }
    }

    /// Drops the peer at `index`, whose session is gone and everything it
    /// delivered read.
    fn forget(&mut self, peers: &mut PeerList, index: usize) {
        peers.remove(index);
        self.cursor.skip_removals(peers);
        self.waiting.remove(index);
        for position in &mut self.turn {
            if *position > index {
                *position -= 1;
            }
        }
    }

    /// Takes in peers added to the list since the last call, and forgets
    /// those removed.
    fn sync(&mut self, peers: &mut PeerList) {
        if self.cursor.removed_since(peers) {
            let turn: Vec<PeerKey> = self.turn.iter().map(|&i| self.waiting[i].key).collect();
            // Both are in key order, so the survivors keep their order.
            let mut list = peers.peers.iter().peekable();
            self.waiting.retain(|waiting| {
                while list.next_if(|peer| peer.key < waiting.key).is_some() {}
                list.next_if(|peer| peer.key == waiting.key).is_some()
            });
            self.turn = turn
                .into_iter()
                .filter_map(|key| self.position(key))
                .collect();
        }
        // `waiting` lines up with the list, so each new peer's position is
        // the next entry's.
        for peer in self.cursor.added_since(peers) {
            let index = self.waiting.len();
            let wake = Arc::new(PeerWake {
                key: peer.key,
                queued: AtomicBool::new(false),
                ready: self.ready.clone(),
            });
            let waker = Waker::from(wake.clone());
            self.waiting.push(Waiting {
                key: peer.key,
                wake,
                waker,
                in_turn: true,
            });
            self.turn.push_back(index);
        }
    }

    /// Where the peer with `key` is in the list.
    fn position(&self, key: PeerKey) -> Option<usize> {
        self.waiting
            .binary_search_by_key(&key, |waiting| waiting.key)
            .ok()
    }

    /// Returns woken peers to the turn.
    fn take_woken(&mut self) {
        if !self.ready.any.load(Ordering::Relaxed) || !self.ready.any.swap(false, Ordering::Acquire)
        {
            return;
        }
        mem::swap(&mut self.woken, &mut self.ready.keys());
        let mut woken = mem::take(&mut self.woken);
        for key in woken.drain(..) {
            let Some(index) = self.position(key) else {
                continue;
            };
            let waiting = &mut self.waiting[index];
            // Cleared first, so a wake from here on queues the key again.
            waiting.wake.queued.store(false, Ordering::Release);
            if !waiting.in_turn {
                waiting.in_turn = true;
                self.turn.push_back(index);
            }
        }
        self.woken = woken;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peers::Peer;
    use crate::test_support::*;

    #[test]
    fn fair_queue_alternates_between_peers() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        for text in ["a1", "a2", "a3"] {
            sa.to_socket.try_send(msg(text)).unwrap();
        }
        for text in ["b1", "b2"] {
            sb.to_socket.try_send(msg(text)).unwrap();
        }
        let mut peers = list(vec![a, b]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        let order: Vec<(PeerKey, Message)> = (0..5)
            .map(|_| match fq.poll_recv(&mut peers, &mut cx) {
                Poll::Ready(received) => received,
                Poll::Pending => panic!("messages are waiting"),
            })
            .collect();
        let (a, b) = (keys[0], keys[1]);
        assert_eq!(
            order,
            [
                (a, msg("a1")),
                (b, msg("b1")),
                (a, msg("a2")),
                (b, msg("b2")),
                (a, msg("a3"))
            ]
        );
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Pending);
    }

    #[test]
    fn fair_queue_is_woken_by_any_peer() {
        let (a, _sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Pending);
        sb.to_socket.try_send(msg("late")).unwrap();
        assert!(count.0.load(Ordering::SeqCst) > 0);
        assert!(
            matches!(fq.poll_recv(&mut peers, &mut cx), Poll::Ready((_, m)) if m == msg("late"))
        );
    }

    #[test]
    fn a_drain_reads_a_batch_from_each_peer_and_wakes_for_the_rest() {
        let (a, mut sa) = connection(1000);
        let (b, mut sb) = connection(1000);
        for _ in 0..DRAIN_BATCH + 10 {
            sa.to_socket.try_send(msg("a")).unwrap();
        }
        sb.to_socket.try_send(msg("b")).unwrap();
        let mut peers = list(vec![a, b]);
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        let mut read = Vec::new();
        fq.poll_drain(&mut peers, &mut cx, |_, message| read.extend(message));
        assert_eq!(read.len(), DRAIN_BATCH + 1, "a batch of a, then b");
        assert_eq!(read.last(), Some(&msg("b")));
        assert!(count.0.load(Ordering::SeqCst) > 0, "woken for the rest");
        read.clear();
        fq.poll_drain(&mut peers, &mut cx, |_, message| read.extend(message));
        assert_eq!(read.len(), 10);
    }

    #[test]
    fn fair_queue_drains_then_drops_a_closed_peer() {
        let (a, mut sa) = connection(10);
        let (b, _sb) = connection(10);
        sa.to_socket.try_send(msg("last words")).unwrap();
        sa.to_socket.try_send(msg("bye")).unwrap();
        drop(sa);
        let mut peers = list(vec![a, b]);
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        let mut next = || match fq.poll_recv(&mut peers, &mut cx) {
            Poll::Ready((_, message)) => Some(message),
            Poll::Pending => None,
        };
        assert_eq!(next(), Some(msg("last words")));
        assert_eq!(next(), Some(msg("bye")));
        assert_eq!(next(), None);
        assert_eq!(peers.len(), 1, "the closed peer is dropped once drained");
    }

    #[test]
    fn fair_queue_skips_empty_peers_until_their_pipes_wake_them() {
        let (a, mut sa) = connection(10);
        let (b, _sb) = connection(10);
        let (c, _sc) = connection(10);
        let mut peers = list(vec![a, b, c]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Pending);
        assert!(fq.turn.is_empty(), "every empty peer leaves the turn");
        sa.to_socket.try_send(msg("a1")).unwrap();
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            fq.poll_recv(&mut peers, &mut cx),
            Poll::Ready((keys[0], msg("a1")))
        );
        assert_eq!(fq.turn, [0], "only the woken peer is back");
        sa.to_socket.try_send(msg("a2")).unwrap();
        assert_eq!(
            fq.poll_recv(&mut peers, &mut cx),
            Poll::Ready((keys[0], msg("a2")))
        );
    }

    #[test]
    fn fair_queue_drops_a_waiting_peer_whose_session_ends() {
        let (a, sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Pending);
        drop(sa);
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Pending);
        assert_eq!(peers.len(), 1);
        sb.to_socket.try_send(msg("b1")).unwrap();
        assert!(matches!(fq.poll_recv(&mut peers, &mut cx), Poll::Ready((_, m)) if m == msg("b1")));
    }

    #[test]
    fn fair_queue_follows_peers_its_owner_removes() {
        let (a, mut sa) = connection(10);
        let (b, sb) = connection(10);
        let (c, mut sc) = connection(10);
        for text in ["a1", "a2"] {
            sa.to_socket.try_send(msg(text)).unwrap();
        }
        for text in ["c1", "c2"] {
            sc.to_socket.try_send(msg(text)).unwrap();
        }
        let mut peers = list(vec![a, b, c]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        assert_eq!(
            fq.poll_recv(&mut peers, &mut cx),
            Poll::Ready((keys[0], msg("a1")))
        );
        drop(sb);
        peers.retain_mut(|peer| !peer.connection.tx.is_closed());
        let rest: Vec<_> = std::iter::from_fn(|| match fq.poll_recv(&mut peers, &mut cx) {
            Poll::Ready(received) => Some(received),
            Poll::Pending => None,
        })
        .collect();
        assert_eq!(
            rest,
            [
                (keys[2], msg("c1")),
                (keys[0], msg("a2")),
                (keys[2], msg("c2"))
            ]
        );
        let (d, mut sd) = connection(10);
        sd.to_socket.try_send(msg("d1")).unwrap();
        let key = peers.push(d);
        assert_eq!(
            fq.poll_recv(&mut peers, &mut cx),
            Poll::Ready((key, msg("d1")))
        );
    }

    #[test]
    fn fair_queue_takes_in_peers_added_between_polls() {
        let (a, _sa) = connection(10);
        let mut peers = list(vec![a]);
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Pending);
        let (b, mut sb) = connection(10);
        sb.to_socket.try_send(msg("b1")).unwrap();
        let key = peers.push(b);
        assert_eq!(
            fq.poll_recv(&mut peers, &mut cx),
            Poll::Ready((key, msg("b1")))
        );
    }
}
