//! What every socket pattern shares: the peers a socket talks to, and the
//! routing strategies patterns apply to them (RFC 28-31).
//!
//! Waiting on many pipes at once works by polling each with the same waker:
//! any pipe that becomes ready wakes the task, which polls again.

use std::io;
use std::net::SocketAddr;
use std::task::{Context as TaskContext, Poll};

use zmq_pipe::{TryRecvError, TrySendError};
use zmtp::Message;

use crate::{Connection, ConnectionConfig, Context, Listener};

/// The connections a socket talks to: those it connected, and those its
/// listeners accepted.
#[derive(Debug)]
pub struct Peers {
    context: Context,
    config: ConnectionConfig,
    connections: Vec<Connection>,
    listeners: Vec<Listener>,
}

impl Peers {
    /// No peers yet; new connections use `config`.
    pub fn new(context: Context, config: ConnectionConfig) -> Self {
        Self {
            context,
            config,
            connections: Vec::new(),
            listeners: Vec::new(),
        }
    }

    /// The configuration new connections use.
    pub fn config_mut(&mut self) -> &mut ConnectionConfig {
        &mut self.config
    }

    /// Connects to `addr`. The peer is usable at once: messages queue until
    /// the connection is up, as in libzmq.
    pub fn connect(&mut self, addr: SocketAddr) {
        let connection = self.context.connect(addr, self.config.clone());
        self.connections.push(connection);
    }

    /// Binds `addr` and accepts peers on it. Returns the bound address.
    pub fn bind(&mut self, addr: SocketAddr) -> io::Result<SocketAddr> {
        let listener = self.context.bind(addr, self.config.clone())?;
        let local = listener.local_addr();
        self.listeners.push(listener);
        Ok(local)
    }

    /// How many peers the socket currently has.
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    /// Whether the socket has no peers.
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    /// Adds peers that listeners have accepted. Registers `cx` with every
    /// listener, so a new peer wakes a task waiting for one.
    pub fn accept(&mut self, cx: &mut TaskContext<'_>) {
        let connections = &mut self.connections;
        self.listeners.retain_mut(|listener| {
            loop {
                match listener.poll_accept(cx) {
                    Poll::Ready(Some(connection)) => connections.push(connection),
                    // The context stopped: this listener will accept no more.
                    Poll::Ready(None) => break false,
                    Poll::Pending => break true,
                }
            }
        });
    }

    /// Drops peers whose sessions are gone, discarding anything they sent.
    /// For patterns that never receive, such as PUSH.
    pub fn prune_send_only(&mut self) {
        self.connections.retain_mut(|connection| {
            loop {
                match connection.rx.try_recv() {
                    Ok(_) => {}
                    Err(TryRecvError::Empty) => break true,
                    Err(TryRecvError::Closed) => break false,
                }
            }
        });
    }

    /// The peers, for a routing strategy.
    pub fn connections_mut(&mut self) -> &mut Vec<Connection> {
        &mut self.connections
    }
}

/// Round robin over the peers that have room, like libzmq's `lb_t`.
#[derive(Debug, Default)]
pub struct LoadBalancer {
    next: usize,
}

impl LoadBalancer {
    /// Sends `message` to the next peer with room. Hands it back, with `cx`
    /// registered on every peer, if none has room or there are no peers.
    pub fn poll_send(
        &mut self,
        peers: &mut [Connection],
        mut message: Message,
        cx: &mut TaskContext<'_>,
    ) -> Result<(), Message> {
        let count = peers.len();
        for offset in 0..count {
            let index = (self.next + offset) % count;
            let tx = &mut peers[index].tx;
            // Closed peers are skipped; full ones register `cx`.
            if let Poll::Ready(Ok(())) = tx.poll_ready(cx) {
                match tx.try_send(message) {
                    Ok(()) => {
                        self.next = (index + 1) % count;
                        return Ok(());
                    }
                    Err(TrySendError::Full(back) | TrySendError::Closed(back)) => message = back,
                }
            }
        }
        Err(message)
    }
}

/// Round robin over the peers that have a message waiting, like libzmq's
/// `fq_t`. Drops peers whose sessions are gone once drained.
#[derive(Debug, Default)]
pub struct FairQueue {
    next: usize,
}

impl FairQueue {
    /// The next message, or `Pending` with `cx` registered on every peer.
    pub fn poll_recv(
        &mut self,
        peers: &mut Vec<Connection>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Message> {
        let mut tried = 0;
        while tried < peers.len() {
            let index = self.next % peers.len();
            match peers[index].rx.poll_recv(cx) {
                Poll::Ready(Some(message)) => {
                    self.next = index + 1;
                    return Poll::Ready(message);
                }
                // Session gone and everything it delivered has been read.
                // The next peer slides into `index`.
                Poll::Ready(None) => {
                    peers.remove(index);
                    self.next = index;
                }
                Poll::Pending => {
                    self.next = index + 1;
                    tried += 1;
                }
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    use zmq_pipe::{Hwm, Reader, Writer, pipe};
    use zmtp::Frame;

    /// The session's ends of a hand-built connection.
    struct FakeSession {
        to_socket: Writer<Message>,
        from_socket: Reader<Message>,
    }

    fn connection(hwm: usize) -> (Connection, FakeSession) {
        let hwm = Hwm::Messages(NonZeroUsize::new(hwm).unwrap());
        let (tx, from_socket) = pipe(hwm);
        let (to_socket, rx) = pipe(hwm);
        (
            Connection { tx, rx },
            FakeSession {
                to_socket,
                from_socket,
            },
        )
    }

    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn waker() -> (Arc<Count>, Waker) {
        let count = Arc::new(Count::default());
        (count.clone(), Waker::from(count))
    }

    fn msg(text: &'static str) -> Message {
        Message::new(Frame::from(text))
    }

    #[test]
    fn load_balancer_round_robins() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let (c, mut sc) = connection(10);
        let mut peers = vec![a, b, c];
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        for text in ["1", "2", "3", "4", "5", "6"] {
            lb.poll_send(&mut peers, msg(text), &mut cx).unwrap();
        }
        let got = |s: &mut FakeSession| {
            std::iter::from_fn(|| s.from_socket.try_recv().ok()).collect::<Vec<_>>()
        };
        assert_eq!(got(&mut sa), [msg("1"), msg("4")]);
        assert_eq!(got(&mut sb), [msg("2"), msg("5")]);
        assert_eq!(got(&mut sc), [msg("3"), msg("6")]);
    }

    #[test]
    fn load_balancer_skips_full_peers() {
        let (a, mut sa) = connection(1);
        let (b, mut sb) = connection(10);
        let mut peers = vec![a, b];
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        for text in ["1", "2", "3", "4"] {
            lb.poll_send(&mut peers, msg(text), &mut cx).unwrap();
        }
        assert_eq!(sa.from_socket.try_recv(), Ok(msg("1")));
        let rest: Vec<_> = std::iter::from_fn(|| sb.from_socket.try_recv().ok()).collect();
        assert_eq!(rest, [msg("2"), msg("3"), msg("4")]);
    }

    #[test]
    fn load_balancer_waits_when_every_peer_is_full() {
        let (a, mut sa) = connection(1);
        let mut peers = vec![a];
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        lb.poll_send(&mut peers, msg("1"), &mut cx).unwrap();
        assert_eq!(lb.poll_send(&mut peers, msg("2"), &mut cx), Err(msg("2")));
        sa.from_socket.try_recv().unwrap();
        assert!(count.0.load(Ordering::SeqCst) > 0, "room wakes the sender");
        lb.poll_send(&mut peers, msg("2"), &mut cx).unwrap();
    }

    #[test]
    fn load_balancer_without_peers_hands_the_message_back() {
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        assert_eq!(lb.poll_send(&mut [], msg("x"), &mut cx), Err(msg("x")));
    }

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
        let mut peers = vec![a, b];
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        let order: Vec<Message> = (0..5)
            .map(|_| match fq.poll_recv(&mut peers, &mut cx) {
                Poll::Ready(message) => message,
                Poll::Pending => panic!("messages are waiting"),
            })
            .collect();
        assert_eq!(
            order,
            [msg("a1"), msg("b1"), msg("a2"), msg("b2"), msg("a3")]
        );
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Pending);
    }

    #[test]
    fn fair_queue_is_woken_by_any_peer() {
        let (a, _sa) = connection(10);
        let (b, mut sb) = connection(10);
        let mut peers = vec![a, b];
        let (count, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Pending);
        sb.to_socket.try_send(msg("late")).unwrap();
        assert!(count.0.load(Ordering::SeqCst) > 0);
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Ready(msg("late")));
    }

    #[test]
    fn fair_queue_drains_then_drops_a_closed_peer() {
        let (a, mut sa) = connection(10);
        let (b, _sb) = connection(10);
        sa.to_socket.try_send(msg("last words")).unwrap();
        sa.to_socket.try_send(msg("bye")).unwrap();
        drop(sa);
        let mut peers = vec![a, b];
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut fq = FairQueue::default();
        assert_eq!(
            fq.poll_recv(&mut peers, &mut cx),
            Poll::Ready(msg("last words"))
        );
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Ready(msg("bye")));
        assert_eq!(fq.poll_recv(&mut peers, &mut cx), Poll::Pending);
        assert_eq!(peers.len(), 1, "the closed peer is dropped once drained");
    }
}
