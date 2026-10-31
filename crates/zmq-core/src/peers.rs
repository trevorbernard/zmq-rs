//! The peers a socket talks to, which every socket pattern shares.
//!
//! Waiting on many pipes at once works by polling each with the same waker:
//! any pipe that becomes ready wakes the task, which polls again.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher, RandomState};
use std::io;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::task::{Context as TaskContext, Poll, Waker};

use zmq_pipe::{ReaderGone, TryRecvError, TrySendError};
use zmtp::Message;

use crate::context::AcceptQueue;
use crate::transport::Address;
use crate::{Connection, ConnectionConfig, Context};

/// A stable name for a peer within one socket. Keys only grow, so peers
/// stay sorted by key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerKey(u64);

/// One bind or connect of a socket, so that it can be undone: the
/// listener or connection it made, and the peers that came through it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EndpointId(u64);

/// Maps and sets keyed by peer. We assign peer keys from a counter, so no
/// peer can choose one, and they need no protection from flooding.
pub(crate) type PeerMap<V> = HashMap<PeerKey, V, BuildHasherDefault<PeerKeyHasher>>;
pub(crate) type PeerSet = HashSet<PeerKey, BuildHasherDefault<PeerKeyHasher>>;

/// Spreads a peer key's bits with one multiplication (Fibonacci hashing).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PeerKeyHasher(u64);

impl Hasher for PeerKeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &octet in bytes {
            self.write_u64(self.0.rotate_left(8) ^ u64::from(octet));
        }
    }

    fn write_u64(&mut self, key: u64) {
        self.0 = key.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

/// One peer of a socket.
#[derive(Debug)]
pub struct Peer {
    pub(crate) key: PeerKey,
    /// The bind or connect it came through, if any.
    endpoint: Option<EndpointId>,
    pub(crate) connection: Connection,
}

impl Peer {
    /// This peer's key.
    pub fn key(&self) -> PeerKey {
        self.key
    }

    /// Ready once the peer has room, or with `ReaderGone` once its session
    /// has ended. `Pending` registers `cx`, unless it is
    /// [`noop_context`]'s.
    pub(crate) fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), ReaderGone>> {
        if registers(cx) {
            self.connection.tx.poll_ready(cx)
        } else {
            self.connection.tx.ready()
        }
    }

    /// Sends `message` if the peer has room. `Full` hands it back with
    /// `cx` registered to hear when there is room; `Closed`, once the
    /// peer's session has ended.
    pub(crate) fn poll_send(
        &mut self,
        message: Message,
        cx: &mut TaskContext<'_>,
    ) -> Result<(), TrySendError<Message>> {
        match self.poll_ready(cx) {
            Poll::Ready(Ok(())) => self.connection.tx.try_send(message),
            Poll::Ready(Err(ReaderGone)) => Err(TrySendError::Closed(message)),
            Poll::Pending => Err(TrySendError::Full(message)),
        }
    }

    /// Discards everything the peer has sent. Returns whether its session
    /// is still there, for patterns that never receive from it.
    pub(crate) fn discard_input(&mut self) -> bool {
        loop {
            match self.connection.rx.try_recv() {
                Ok(_) => {}
                Err(TryRecvError::Empty) => return true,
                Err(TryRecvError::Closed) => return false,
            }
        }
    }
}

/// A socket's peers, in the order they arrived.
#[derive(Debug, Default)]
pub struct PeerList {
    pub(crate) peers: Vec<Peer>,
    next_key: u64,
    /// Bumped whenever a peer is removed, so tables keyed by peer know to
    /// forget it.
    removals: u64,
}

impl PeerList {
    /// Adds a peer and returns its key.
    pub fn push(&mut self, connection: Connection) -> PeerKey {
        self.push_from(connection, None)
    }

    fn push_from(&mut self, connection: Connection, endpoint: Option<EndpointId>) -> PeerKey {
        let key = PeerKey(self.next_key);
        self.next_key += 1;
        self.peers.push(Peer {
            key,
            endpoint,
            connection,
        });
        key
    }

    /// How many peers there are.
    #[must_use]
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Whether there are no peers.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// The peers, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = &Peer> {
        self.peers.iter()
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut Peer> {
        self.peers.iter_mut()
    }

    /// The peer with `key`, if it is still here.
    pub fn get(&self, key: PeerKey) -> Option<&Peer> {
        let index = self.peers.binary_search_by_key(&key, Peer::key).ok()?;
        Some(&self.peers[index])
    }

    /// The peer with `key`, if it is still here.
    pub fn get_mut(&mut self, key: PeerKey) -> Option<&mut Peer> {
        let index = self.peers.binary_search_by_key(&key, Peer::key).ok()?;
        Some(&mut self.peers[index])
    }

    pub(crate) fn remove(&mut self, index: usize) {
        self.peers.remove(index);
        self.removals += 1;
    }

    pub(crate) fn retain_mut(&mut self, keep: impl FnMut(&mut Peer) -> bool) {
        let before = self.peers.len();
        self.peers.retain_mut(keep);
        if self.peers.len() != before {
            self.removals += 1;
        }
    }

    /// Removes the peer with `key`. Returns whether it was here.
    pub(crate) fn remove_key(&mut self, key: PeerKey) -> bool {
        match self.peers.binary_search_by_key(&key, Peer::key) {
            Ok(index) => {
                self.remove(index);
                true
            }
            Err(_) => false,
        }
    }

    /// Removes the peers with `keys` whose sessions are gone and whose
    /// messages have all been read, as libzmq drops a pipe once read to its
    /// end. Sends that find peers gone call this, so a socket that never
    /// receives does not keep its departed peers.
    pub(crate) fn remove_finished(&mut self, keys: impl IntoIterator<Item = PeerKey>) {
        for key in keys {
            if self
                .get(key)
                .is_some_and(|peer| peer.connection.rx.is_drained())
            {
                self.remove_key(key);
            }
        }
    }

    /// Sends a copy of `message` to every peer, past the HWM: for
    /// subscriptions and joins, which are never dropped. Skips peers that
    /// are gone.
    pub(crate) fn broadcast(&mut self, message: &Message) {
        for peer in &mut self.peers {
            let _ = peer.connection.tx.force_send(message.clone());
        }
    }

    /// Sends a copy of `message` to every peer below its HWM, as libzmq's
    /// `dist` does: a peer that is full or gone misses it.
    pub(crate) fn send_to_all(&mut self, message: &Message) {
        for peer in &mut self.peers {
            let _ = peer.connection.tx.try_send(message.clone());
        }
    }
}

/// How far something kept beside a [`PeerList`], such as a table keyed by
/// peer, has followed it: which peers it has seen arrive, and how many
/// removals it has caught up with.
#[derive(Debug, Default)]
pub(crate) struct PeerCursor {
    /// Peers with keys below this have been seen. Keys only grow.
    seen_below: u64,
    /// `PeerList::removals` when last looked at.
    removals: u64,
}

impl PeerCursor {
    /// Whether peers have been removed since the last call, so entries for
    /// peers no longer in the list should go.
    pub(crate) fn removed_since(&mut self, peers: &PeerList) -> bool {
        let removed = peers.removals != self.removals;
        self.removals = peers.removals;
        removed
    }

    /// Notes removals the caller made and accounted for itself, so
    /// [`removed_since`](Self::removed_since) does not report them.
    pub(crate) fn skip_removals(&mut self, peers: &PeerList) {
        self.removals = peers.removals;
    }

    /// The peers added since the last call, oldest first.
    pub(crate) fn added_since<'a>(&mut self, peers: &'a mut PeerList) -> &'a mut [Peer] {
        // Called on every receive, where usually nothing is new: the search
        // below touches a cache line per step of a list of large peers.
        if self.seen_below == peers.next_key {
            return &mut [];
        }
        let first_new = peers
            .peers
            .partition_point(|peer| peer.key.0 < self.seen_below);
        self.seen_below = peers.next_key;
        &mut peers.peers[first_new..]
    }
}

/// A random starting point for the ids libzmq counts up from a random
/// value: ROUTER's generated routing ids, SERVER's client ids, REQ's
/// request ids. Each `RandomState` gets a new key.
pub(crate) fn random_u32() -> u32 {
    // Truncated on purpose: any 32 of the hash's bits will do.
    RandomState::new().hash_one(()) as u32
}

/// The waker non-blocking calls poll with, through [`noop_context`]. One
/// static, recognised by its address: `Waker::noop` may not compare equal
/// across crates.
static NOOP_WAKER: &Waker = Waker::noop();

/// A context for non-blocking calls. [`Peers::accept`] takes what is
/// queued without registering it, so it does not replace the waker of a
/// task that polled the socket earlier in the same turn, as a proxy does
/// before sending into it.
pub fn noop_context() -> TaskContext<'static> {
    TaskContext::from_waker(NOOP_WAKER)
}

/// Whether to register `cx`: not if it is [`noop_context`]'s, which
/// non-blocking calls pass. Registering it would replace the waker of a
/// task that polled the socket earlier in the same turn, as a proxy does
/// before sending into it, and that task would then miss what it waits
/// for.
pub(crate) fn registers(cx: &TaskContext<'_>) -> bool {
    !std::ptr::eq(cx.waker(), NOOP_WAKER)
}

/// The connections a socket talks to: those it connected, and those its
/// listeners accepted.
#[derive(Debug)]
pub struct Peers {
    context: Context,
    config: ConnectionConfig,
    list: PeerList,
    /// Accept queues of the socket's listeners, and of its `IMMEDIATE`
    /// connects, each with the bind or connect that made it.
    listeners: Vec<(EndpointId, AcceptQueue)>,
    next_endpoint: u64,
    /// What each new peer is sent first, if anything.
    greeting: Option<Message>,
}

impl Peers {
    /// No peers yet; new connections use `config`.
    pub fn new(context: Context, config: ConnectionConfig) -> Self {
        Self {
            context,
            config,
            list: PeerList::default(),
            listeners: Vec::new(),
            next_endpoint: 0,
            greeting: None,
        }
    }

    /// Sends each peer added from now on `greeting` before anything else,
    /// such as libzmq's `ZMQ_PROBE_ROUTER` empty message or
    /// `ZMQ_XPUB_WELCOME_MSG`.
    pub fn set_greeting(&mut self, mut greeting: Option<Message>) {
        if let Some(greeting) = &mut greeting {
            greeting.set_metadata(None);
        }
        self.greeting = greeting;
    }

    fn add(&mut self, connection: Connection, endpoint: EndpointId) -> PeerKey {
        add(&mut self.list, self.greeting.as_ref(), connection, endpoint)
    }

    fn next_endpoint(&mut self) -> EndpointId {
        let endpoint = EndpointId(self.next_endpoint);
        self.next_endpoint += 1;
        endpoint
    }

    fn listen(&mut self, queue: AcceptQueue) -> EndpointId {
        let endpoint = self.next_endpoint();
        self.listeners.push((endpoint, queue));
        endpoint
    }

    /// The configuration new connections use.
    pub fn config_mut(&mut self) -> &mut ConnectionConfig {
        &mut self.config
    }

    /// Connects to `addr`. The peer is usable at once: messages queue until
    /// the connection is up, as in libzmq. With
    /// [`immediate`](ConnectionConfig::immediate) there is no peer until
    /// then, so no key.
    pub fn connect(&mut self, addr: SocketAddr) -> (EndpointId, Option<PeerKey>) {
        self.connect_to(Address::Tcp(addr))
    }

    /// Connects to `host`, a name or an address, resolving it for each
    /// attempt, as libzmq does: a name that does not resolve yet is retried
    /// like a refused connection. Connects from `source`, if given, as
    /// libzmq's `tcp://source;host:port`. Otherwise as
    /// [`connect`](Self::connect).
    pub fn connect_host(
        &mut self,
        host: &str,
        port: u16,
        source: Option<SocketAddr>,
    ) -> (EndpointId, Option<PeerKey>) {
        self.connect_to(Address::TcpHost {
            host: host.to_owned(),
            port,
            source,
        })
    }

    fn connect_to(&mut self, addr: Address) -> (EndpointId, Option<PeerKey>) {
        if self.config.immediate {
            let connections = self.context.connect_immediate(addr, self.config.clone());
            let queue = AcceptQueue {
                connections,
                closed: None,
            };
            return (self.listen(queue), None);
        }
        let connection = self.context.connect_to(addr, self.config.clone());
        let endpoint = self.next_endpoint();
        (endpoint, Some(self.add(connection, endpoint)))
    }

    /// Binds `addr` and accepts peers on it. Returns the bound address.
    pub fn bind(&mut self, addr: SocketAddr) -> io::Result<(EndpointId, SocketAddr)> {
        let listener = self.context.bind(addr, self.config.clone())?;
        let local = listener.local_addr();
        Ok((self.listen(listener.into_queue()), local))
    }

    /// Binds a Unix domain socket at `path` and accepts peers on it.
    #[cfg(unix)]
    pub fn bind_ipc(&mut self, path: &Path) -> io::Result<EndpointId> {
        let listener = self.context.bind_ipc(path, self.config.clone())?;
        Ok(self.listen(listener.into_queue()))
    }

    /// Binds a Unix domain socket at a fresh temporary path, returning it.
    #[cfg(unix)]
    pub fn bind_ipc_anywhere(&mut self) -> io::Result<(EndpointId, PathBuf)> {
        let listener = self.context.bind_ipc_anywhere(self.config.clone())?;
        let path = listener.path().to_owned();
        Ok((self.listen(listener.into_queue()), path))
    }

    /// Connects to the Unix domain socket at `path`, retrying until it is
    /// bound. No key with [`immediate`](ConnectionConfig::immediate), as
    /// for [`connect`](Self::connect).
    #[cfg(unix)]
    pub fn connect_ipc(&mut self, path: &Path) -> (EndpointId, Option<PeerKey>) {
        self.connect_to(Address::Ipc(path.to_owned()))
    }

    /// Binds `name` in Linux's abstract socket namespace and accepts peers
    /// on it.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn bind_ipc_abstract(&mut self, name: &str) -> io::Result<EndpointId> {
        let listener = self.context.bind_ipc_abstract(name, self.config.clone())?;
        Ok(self.listen(listener.into_queue()))
    }

    /// Connects to `name` in Linux's abstract socket namespace, retrying
    /// until it is bound. No key with
    /// [`immediate`](ConnectionConfig::immediate), as for
    /// [`connect`](Self::connect).
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn connect_ipc_abstract(&mut self, name: &str) -> (EndpointId, Option<PeerKey>) {
        self.connect_to(Address::IpcAbstract(name.to_owned()))
    }

    /// Binds the `inproc` `name` and accepts peers on it.
    pub fn bind_inproc(&mut self, name: &str) -> io::Result<EndpointId> {
        let listener = self.context.bind_inproc(name, self.config.clone())?;
        Ok(self.listen(listener.into_queue()))
    }

    /// Connects to the `inproc` `name`, binding or not. `inproc` ignores
    /// [`immediate`](ConnectionConfig::immediate), as libzmq does.
    pub fn connect_inproc(&mut self, name: &str) -> (EndpointId, PeerKey) {
        let connection = self.context.connect_inproc(name, self.config.clone());
        let endpoint = self.next_endpoint();
        (endpoint, self.add(connection, endpoint))
    }

    /// Undoes the bind or connect that made `endpoint`, as libzmq's
    /// `zmq_unbind` and `zmq_disconnect`: closes its listener, or ends its
    /// connection's retries, and drops the peers that came through it.
    /// What they sent is discarded; what was queued to them is still
    /// delivered within the linger. Returns once a closed listener's
    /// address is free to bind again.
    pub fn close(&mut self, endpoint: EndpointId) {
        let mut closing = Vec::new();
        self.listeners.retain_mut(|(made_by, queue)| {
            let keep = *made_by != endpoint;
            if !keep {
                closing.extend(queue.closed.take());
            }
            keep
        });
        self.list.retain_mut(|peer| peer.endpoint != Some(endpoint));
        wait_closed(closing);
    }

    /// A new monitor of this socket's events, from now on.
    pub fn monitor(&self) -> crate::Monitor {
        self.config.monitor.monitor()
    }

    /// How many peers the socket currently has.
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Whether the socket has no peers.
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Adds peers that listeners have accepted. Registers `cx` with every
    /// listener, so a new peer wakes a task waiting for one, unless `cx` is
    /// [`noop_context`]'s.
    pub fn accept(&mut self, cx: &mut TaskContext<'_>) {
        let register = registers(cx);
        let (list, greeting) = (&mut self.list, self.greeting.as_ref());
        self.listeners.retain_mut(|(endpoint, queue)| {
            loop {
                let next = if register {
                    queue.connections.poll_recv(cx)
                } else {
                    match queue.connections.try_recv() {
                        Ok(connection) => Poll::Ready(Some(connection)),
                        Err(TryRecvError::Empty) => Poll::Pending,
                        Err(TryRecvError::Closed) => Poll::Ready(None),
                    }
                };
                match next {
                    Poll::Ready(Some(connection)) => {
                        add(list, greeting, connection, *endpoint);
                    }
                    // The context stopped: this listener will accept no more.
                    Poll::Ready(None) => break false,
                    Poll::Pending => break true,
                }
            }
        });
    }

    /// The peers, for a routing strategy.
    pub fn list_mut(&mut self) -> &mut PeerList {
        &mut self.list
    }
}

impl Drop for Peers {
    /// Waits, as [`close`](Self::close) does, until the I/O threads have
    /// closed the socket's listeners, so that their addresses can be bound
    /// again once it is dropped. libzmq closes them in the background, but
    /// its ipc bind removes whatever file is at the path.
    fn drop(&mut self) {
        let closing: Vec<_> = self
            .listeners
            .drain(..)
            .filter_map(|(_, mut queue)| queue.closed.take())
            .collect();
        wait_closed(closing);
    }
}

/// Waits until each listener's I/O thread has dropped it, its accept queue
/// already dropped.
fn wait_closed(closing: Vec<mpsc::Receiver<()>>) {
    for closed in closing {
        // Disconnects, never sends, once the I/O thread drops the listener.
        let _ = closed.recv();
    }
}

fn add(
    list: &mut PeerList,
    greeting: Option<&Message>,
    mut connection: Connection,
    endpoint: EndpointId,
) -> PeerKey {
    if let Some(greeting) = greeting {
        // A new pipe has room, unless the peer is already gone.
        let _ = connection.tx.try_send(greeting.clone());
    }
    list.push_from(connection, Some(endpoint))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    #[test]
    fn peers_count_their_connections() {
        let context = crate::Context::new().unwrap();
        let mut peers = Peers::new(context, ConnectionConfig::new(zmtp::SocketType::Push));
        assert!(peers.is_empty());
        let (first, _) = peers.connect_inproc("one");
        peers.connect_inproc("two");
        assert_eq!(peers.len(), 2);
        assert!(!peers.is_empty());
        peers.close(first);
        assert_eq!(peers.len(), 1);
    }

    #[test]
    fn peers_are_found_by_key_after_removals() {
        let (a, _sa) = connection(10);
        let (b, sb) = connection(10);
        let (c, _sc) = connection(10);
        let mut peers = list(vec![a, b, c]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        drop(sb);
        peers.retain_mut(|peer| !peer.connection.tx.is_closed());
        assert!(peers.get_mut(keys[1]).is_none());
        assert_eq!(peers.get_mut(keys[2]).map(|p| p.key()), Some(keys[2]));
    }

    // ---- RouterTable ----
}
