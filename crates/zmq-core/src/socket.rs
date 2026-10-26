//! What every socket pattern shares: the peers a socket talks to, and the
//! routing strategies patterns apply to them (RFC 28-31).
//!
//! Waiting on many pipes at once works by polling each with the same waker:
//! any pipe that becomes ready wakes the task, which polls again.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher, RandomState};
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::task::{Context as TaskContext, Poll, Wake, Waker};

use atomic_waker::AtomicWaker;
use zmq_pipe::{TryRecvError, TrySendError};
use zmtp::{Frame, Message};

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
    key: PeerKey,
    /// The bind or connect it came through, if any.
    endpoint: Option<EndpointId>,
    pub(crate) connection: Connection,
}

impl Peer {
    /// This peer's key.
    pub fn key(&self) -> PeerKey {
        self.key
    }

    /// Sends `message` if the peer has room. `Full` hands it back with
    /// `cx` registered to hear when there is room; `Closed`, once the
    /// peer's session has ended.
    pub(crate) fn poll_send(
        &mut self,
        message: Message,
        cx: &mut TaskContext<'_>,
    ) -> Result<(), TrySendError<Message>> {
        let tx = &mut self.connection.tx;
        match tx.poll_ready(cx) {
            Poll::Ready(Ok(())) => tx.try_send(message),
            Poll::Ready(Err(())) => Err(TrySendError::Closed(message)),
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
    peers: Vec<Peer>,
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

    fn remove(&mut self, index: usize) {
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

    /// Sends a copy of `message` to every peer, skipping those that are
    /// full or gone.
    pub(crate) fn broadcast(&mut self, message: &Message) {
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
    pub fn bind_ipc(&mut self, path: &std::path::Path) -> io::Result<EndpointId> {
        let listener = self.context.bind_ipc(path, self.config.clone())?;
        Ok(self.listen(listener.into_queue()))
    }

    /// Binds a Unix domain socket at a fresh temporary path, returning it.
    #[cfg(unix)]
    pub fn bind_ipc_anywhere(&mut self) -> io::Result<(EndpointId, std::path::PathBuf)> {
        let listener = self.context.bind_ipc_anywhere(self.config.clone())?;
        let path = listener.path().to_owned();
        Ok((self.listen(listener.into_queue()), path))
    }

    /// Connects to the Unix domain socket at `path`, retrying until it is
    /// bound. No key with [`immediate`](ConnectionConfig::immediate), as
    /// for [`connect`](Self::connect).
    #[cfg(unix)]
    pub fn connect_ipc(&mut self, path: &std::path::Path) -> (EndpointId, Option<PeerKey>) {
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
    /// [`noop_context`]'s, as non-blocking calls pass: registering it would replace
    /// the waker of a task that polled the socket earlier in the same turn,
    /// as a proxy does before sending into it, and that task would then
    /// miss the next peer.
    pub fn accept(&mut self, cx: &mut TaskContext<'_>) {
        let register = !std::ptr::eq(cx.waker(), NOOP_WAKER);
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

    /// Drops peers whose sessions are gone, discarding anything they sent.
    /// For patterns that never receive, such as PUSH.
    pub fn prune_send_only(&mut self) {
        self.list.retain_mut(Peer::discard_input);
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

/// Round robin over the peers that have room, like libzmq's `lb_t`.
#[derive(Debug, Default)]
pub struct LoadBalancer {
    next: usize,
}

impl LoadBalancer {
    /// Sends `message` to the next peer with room and returns that peer.
    /// Hands the message back, with `cx` registered on every peer, if none
    /// has room or there are no peers.
    pub fn poll_send(
        &mut self,
        peers: &mut PeerList,
        mut message: Message,
        cx: &mut TaskContext<'_>,
    ) -> Result<PeerKey, Message> {
        message.set_metadata(None);
        let count = peers.peers.len();
        for offset in 0..count {
            let index = (self.next + offset) % count;
            let peer = &mut peers.peers[index];
            // Closed peers are skipped; full ones register `cx`.
            match peer.poll_send(message, cx) {
                Ok(()) => {
                    self.next = (index + 1) % count;
                    return Ok(peer.key);
                }
                Err(TrySendError::Full(back) | TrySendError::Closed(back)) => message = back,
            }
        }
        Err(message)
    }

    /// Whether some peer has room for a message, so that
    /// [`poll_send`](Self::poll_send) would succeed. Registers `cx` with
    /// every peer if none has.
    pub fn poll_ready(&mut self, peers: &mut PeerList, cx: &mut TaskContext<'_>) -> Poll<()> {
        for peer in &mut peers.peers {
            if let Poll::Ready(Ok(())) = peer.connection.tx.poll_ready(cx) {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    }
}

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
        ready
            .keys
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(self.key);
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
            self.ready.task.register(cx.waker());
            if !self.ready.any.load(Ordering::Acquire) {
                return Poll::Pending;
            }
        }
    }

    /// Hands `f` everything peers have sent, a peer at a time, as libzmq's
    /// XPUB reads a pipe through once it is activated, then `None` for a
    /// peer whose session is gone. Peers are read in the order they woke.
    /// Registers no task, so one registered earlier stays in place.
    pub(crate) fn drain(
        &mut self,
        peers: &mut PeerList,
        mut f: impl FnMut(PeerKey, Option<Message>),
    ) {
        self.sync(peers);
        self.take_woken();
        while let Some(index) = self.turn.pop_front() {
            let waiting = &mut self.waiting[index];
            let key = waiting.key;
            let mut cx = TaskContext::from_waker(&waiting.waker);
            let rx = &mut peers.peers[index].connection.rx;
            let closed = loop {
                match rx.poll_recv(&mut cx) {
                    Poll::Ready(Some(message)) => f(key, Some(message)),
                    Poll::Ready(None) => break true,
                    Poll::Pending => break false,
                }
            };
            if closed {
                self.forget(peers, index);
                f(key, None);
            } else {
                self.waiting[index].in_turn = false;
            }
        }
    }

    /// As [`drain`](Self::drain), then registers `cx` to be woken once any
    /// peer sends more.
    pub(crate) fn poll_drain(
        &mut self,
        peers: &mut PeerList,
        cx: &mut TaskContext<'_>,
        mut f: impl FnMut(PeerKey, Option<Message>),
    ) {
        loop {
            self.drain(peers, &mut f);
            // Registered before looking again, as in `poll_recv`.
            self.ready.task.register(cx.waker());
            if !self.ready.any.load(Ordering::Acquire) {
                return;
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
        mem::swap(
            &mut self.woken,
            &mut self
                .ready
                .keys
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
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

/// ROUTER's map between peers and their routing ids, as in libzmq's
/// `router_t`.
///
/// A peer's routing id is the identity it announced in its handshake. A
/// peer that announced none gets a generated one: `0x00` and a 32-bit
/// counter starting at a random value. A peer announcing an identity
/// already in use is ignored: its messages are dropped and nothing can be
/// sent to it. With [`handover`](Self::set_handover) it takes the identity
/// over instead, and the peer that had it is disconnected.
#[derive(Debug)]
pub struct RouterTable {
    by_identity: HashMap<Box<[u8]>, PeerKey>,
    by_peer: PeerMap<Route>,
    /// Peers whose handshake had not completed when last looked at.
    waiting: Vec<PeerKey>,
    cursor: PeerCursor,
    next_generated: u32,
    handover: bool,
}

/// What became of a peer's announced identity.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Route {
    Routable(Frame),
    /// Duplicate identity: libzmq ignores the newcomer.
    Ignored,
}

/// What happened to a reply REP was asked to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteOutcome {
    Sent,
    /// The requester is gone. The reply was dropped.
    NoSuchPeer,
    /// The requester's queue is full. The reply was dropped.
    PeerFull,
}

/// Why a ROUTER could not send without waiting. The message is handed back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterSendError {
    /// No peer has that routing id, or it has gone.
    UnknownRoutingId(Message),
    /// The peer's queue is full.
    Full(Message),
}

impl fmt::Display for RouterSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownRoutingId(_) => "no peer has that routing id",
            Self::Full(_) => "the peer's queue is full",
        })
    }
}

impl std::error::Error for RouterSendError {}

impl Default for RouterTable {
    fn default() -> Self {
        Self {
            by_identity: HashMap::new(),
            by_peer: PeerMap::default(),
            waiting: Vec::new(),
            cursor: PeerCursor::default(),
            // libzmq starts the generated ids at a random value.
            next_generated: random_u32(),
            handover: false,
        }
    }
}

impl RouterTable {
    /// Whether a peer announcing an identity already in use takes it over,
    /// disconnecting the peer that had it (libzmq's `ZMQ_ROUTER_HANDOVER`).
    /// Otherwise the newcomer is ignored.
    pub fn set_handover(&mut self, on: bool) {
        self.handover = on;
    }

    /// Identifies peers whose handshake has completed and forgets peers that
    /// are gone.
    pub fn sync(&mut self, peers: &mut PeerList) {
        if self.cursor.removed_since(peers) {
            let live = |key: &PeerKey| peers.get(*key).is_some();
            self.by_peer.retain(|key, _| live(key));
            self.by_identity.retain(|_, key| live(key));
            self.waiting.retain(live);
        }
        let added = self.cursor.added_since(peers);
        self.waiting.extend(added.iter().map(Peer::key));
        for key in mem::take(&mut self.waiting) {
            let Some(peer) = peers.get(key) else {
                continue;
            };
            match peer.connection.peer() {
                Some(established) => self.identify(peers, key, established.peer_identity),
                None => self.waiting.push(key),
            }
        }
    }

    fn identify(&mut self, peers: &mut PeerList, key: PeerKey, announced: Frame) {
        let routing_id = if announced.is_empty() {
            let mut generated = [0u8; 5];
            generated[1..].copy_from_slice(&self.next_generated.to_be_bytes());
            self.next_generated = self.next_generated.wrapping_add(1);
            Frame::copy_from_slice(&generated)
        } else {
            announced
        };
        if let Some(&holder) = self.by_identity.get(routing_id.as_slice()) {
            if !self.handover {
                self.by_peer.insert(key, Route::Ignored);
                return;
            }
            self.by_peer.remove(&holder);
            peers.remove_key(holder);
        }
        self.by_identity.insert(routing_id.as_slice().into(), key);
        self.by_peer.insert(key, Route::Routable(routing_id));
    }

    /// The routing id of `peer`, if it has been identified and not ignored.
    #[must_use]
    pub fn routing_id(&self, peer: PeerKey) -> Option<&Frame> {
        match self.by_peer.get(&peer) {
            Some(Route::Routable(routing_id)) => Some(routing_id),
            Some(Route::Ignored) | None => None,
        }
    }

    /// Sends `message` to the peer with `routing_id` if it is known and has
    /// room. Never blocks.
    pub fn try_send(
        &mut self,
        peers: &mut PeerList,
        routing_id: &[u8],
        mut message: Message,
    ) -> Result<(), RouterSendError> {
        message.set_metadata(None);
        let Some(peer) = self
            .by_identity
            .get(routing_id)
            .and_then(|&key| peers.get_mut(key))
        else {
            return Err(RouterSendError::UnknownRoutingId(message));
        };
        peer.connection
            .tx
            .try_send(message)
            .map_err(|err| match err {
                TrySendError::Full(message) => RouterSendError::Full(message),
                TrySendError::Closed(message) => RouterSendError::UnknownRoutingId(message),
            })
    }

    /// Sends the message in `slot` to the peer with `routing_id`, or
    /// returns `Pending` with `cx` registered while that peer is full. Fails
    /// with the message if no peer has that routing id or it has gone.
    pub fn poll_send(
        &mut self,
        peers: &mut PeerList,
        routing_id: &[u8],
        slot: &mut Option<Message>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<(), Message>> {
        if let Some(message) = slot {
            message.set_metadata(None);
        }
        let Some(message) = slot.take() else {
            return Poll::Ready(Ok(()));
        };
        let Some(peer) = self
            .by_identity
            .get(routing_id)
            .and_then(|&key| peers.get_mut(key))
        else {
            return Poll::Ready(Err(message));
        };
        match peer.poll_send(message, cx) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(TrySendError::Closed(message)) => Poll::Ready(Err(message)),
            Err(TrySendError::Full(message)) => {
                *slot = Some(message);
                Poll::Pending
            }
        }
    }
}

/// SERVER's routing (RFC 41), as libzmq's `server_t`: every peer gets a
/// non-zero 32-bit id, from a counter that starts at a random value;
/// replies go to a peer by its id. Single-part messages only.
#[cfg(feature = "draft")]
#[derive(Debug)]
pub struct ServerTable {
    by_id: HashMap<std::num::NonZeroU32, PeerKey>,
    by_peer: PeerMap<std::num::NonZeroU32>,
    cursor: PeerCursor,
    next_id: u32,
    recv: FairQueue,
}

/// Why SERVER could not send to a client.
#[cfg(feature = "draft")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerSendError {
    /// No client has that id, or it has gone. The frame is handed back.
    NoSuchClient(Frame),
    /// The client's queue is full. The frame is handed back.
    Full(Frame),
}

#[cfg(feature = "draft")]
impl fmt::Display for ServerSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NoSuchClient(_) => "no client has that id",
            Self::Full(_) => "the client's queue is full",
        })
    }
}

#[cfg(feature = "draft")]
impl std::error::Error for ServerSendError {}

#[cfg(feature = "draft")]
impl Default for ServerTable {
    fn default() -> Self {
        Self {
            by_id: HashMap::new(),
            by_peer: PeerMap::default(),
            cursor: PeerCursor::default(),
            recv: FairQueue::default(),
            // libzmq starts its ids at a random value.
            next_id: random_u32(),
        }
    }
}

#[cfg(feature = "draft")]
impl ServerTable {
    /// Gives new peers their ids and forgets peers that are gone.
    pub fn sync(&mut self, peers: &mut PeerList) {
        if self.cursor.removed_since(peers) {
            self.by_peer.retain(|key, _| peers.get(*key).is_some());
            self.by_id.retain(|_, key| peers.get(*key).is_some());
        }
        for peer in self.cursor.added_since(peers) {
            let id = loop {
                let candidate = self.next_id;
                self.next_id = self.next_id.wrapping_add(1);
                // Never zero, and never one still in use after wrapping.
                if let Some(id) = std::num::NonZeroU32::new(candidate)
                    && !self.by_id.contains_key(&id)
                {
                    break id;
                }
            };
            self.by_id.insert(id, peer.key);
            self.by_peer.insert(peer.key, id);
        }
    }

    /// The next single-part message and the id of the client that sent it,
    /// or `Pending` with `cx` registered on every peer. Multipart messages
    /// are dropped, as RFC 41 requires.
    pub fn poll_recv(
        &mut self,
        peers: &mut PeerList,
        cx: &mut TaskContext<'_>,
    ) -> Poll<(std::num::NonZeroU32, Frame)> {
        self.sync(peers);
        loop {
            let Poll::Ready((key, message)) = self.recv.poll_recv(peers, cx) else {
                return Poll::Pending;
            };
            let (frame, rest) = message.split_first();
            if !rest.is_empty() {
                continue;
            }
            if let Some(&id) = self.by_peer.get(&key) {
                return Poll::Ready((id, frame.clone()));
            }
        }
    }

    /// Sends `frame` to the client with `id`, without waiting. On `Full`,
    /// `cx` is registered to hear when it has room.
    pub fn try_send(
        &mut self,
        peers: &mut PeerList,
        id: std::num::NonZeroU32,
        frame: Frame,
        cx: &mut TaskContext<'_>,
    ) -> Result<(), ServerSendError> {
        self.sync(peers);
        let Some(peer) = self.by_id.get(&id).and_then(|&key| peers.get_mut(key)) else {
            return Err(ServerSendError::NoSuchClient(frame));
        };
        peer.poll_send(Message::new(frame), cx)
            .map_err(|err| match err {
                TrySendError::Full(message) => ServerSendError::Full(only_frame(message)),
                // The client's session has ended.
                TrySendError::Closed(message) => ServerSendError::NoSuchClient(only_frame(message)),
            })
    }

    /// The id of the peer with `key`, once it is in the list.
    pub fn id_of(&mut self, peers: &mut PeerList, key: PeerKey) -> Option<std::num::NonZeroU32> {
        self.sync(peers);
        self.by_peer.get(&key).copied()
    }

    /// Closes the connection to the client with `id`. Returns whether there
    /// was one.
    pub fn disconnect(&mut self, peers: &mut PeerList, id: std::num::NonZeroU32) -> bool {
        self.sync(peers);
        let Some(key) = self.by_id.remove(&id) else {
            return false;
        };
        self.by_peer.remove(&key);
        peers.remove_key(key);
        true
    }
}

/// The frame of a message built from one.
#[cfg(feature = "draft")]
fn only_frame(message: Message) -> Frame {
    message.into_parts().0
}

#[cfg(test)]
mod properties;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn load_balancer_round_robins() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        let (c, mut sc) = connection(10);
        let mut peers = list(vec![a, b, c]);
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        for text in ["1", "2", "3", "4", "5", "6"] {
            lb.poll_send(&mut peers, msg(text), &mut cx).unwrap();
        }
        assert_eq!(drain(&mut sa), [msg("1"), msg("4")]);
        assert_eq!(drain(&mut sb), [msg("2"), msg("5")]);
        assert_eq!(drain(&mut sc), [msg("3"), msg("6")]);
    }

    #[test]
    fn load_balancer_skips_full_peers() {
        let (a, mut sa) = connection(1);
        let (b, mut sb) = connection(10);
        let mut peers = list(vec![a, b]);
        let (_, waker) = waker();
        let mut cx = TaskContext::from_waker(&waker);
        let mut lb = LoadBalancer::default();
        for text in ["1", "2", "3", "4"] {
            lb.poll_send(&mut peers, msg(text), &mut cx).unwrap();
        }
        assert_eq!(drain(&mut sa), [msg("1")]);
        assert_eq!(drain(&mut sb), [msg("2"), msg("3"), msg("4")]);
    }

    #[test]
    fn load_balancer_waits_when_every_peer_is_full() {
        let (a, mut sa) = connection(1);
        let mut peers = list(vec![a]);
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
        let mut peers = PeerList::default();
        assert_eq!(lb.poll_send(&mut peers, msg("x"), &mut cx), Err(msg("x")));
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

    #[test]
    fn router_routes_by_announced_identity() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        sa.establish("alice");
        sb.establish("bob");
        let mut peers = list(vec![a, b]);
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        assert_eq!(table.try_send(&mut peers, b"bob", msg("hi bob")), Ok(()));
        assert_eq!(
            table.try_send(&mut peers, b"alice", msg("hi alice")),
            Ok(())
        );
        assert_eq!(drain(&mut sa), [msg("hi alice")]);
        assert_eq!(drain(&mut sb), [msg("hi bob")]);
    }

    #[test]
    fn router_hands_back_messages_it_cannot_route() {
        let (a, mut sa) = connection(1);
        sa.establish("alice");
        let mut peers = list(vec![a]);
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        assert_eq!(
            table.try_send(&mut peers, b"nobody", msg("x")),
            Err(RouterSendError::UnknownRoutingId(msg("x")))
        );
        assert_eq!(table.try_send(&mut peers, b"alice", msg("1")), Ok(()));
        assert_eq!(
            table.try_send(&mut peers, b"alice", msg("2")),
            Err(RouterSendError::Full(msg("2")))
        );
        assert_eq!(drain(&mut sa), [msg("1")]);
    }

    #[test]
    fn router_waits_for_the_handshake_before_routing() {
        let (a, sa) = connection(10);
        let mut peers = list(vec![a]);
        let key = peers.iter().next().unwrap().key();
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        assert_eq!(table.routing_id(key), None, "not identified yet");
        sa.establish("late");
        table.sync(&mut peers);
        assert_eq!(table.routing_id(key), Some(&Frame::from("late")));
    }

    #[test]
    fn router_generates_ids_for_anonymous_peers() {
        let (a, sa) = connection(10);
        let (b, sb) = connection(10);
        sa.establish("");
        sb.establish("");
        let mut peers = list(vec![a, b]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        let first = table.routing_id(keys[0]).unwrap().clone();
        let second = table.routing_id(keys[1]).unwrap().clone();
        for id in [&first, &second] {
            assert_eq!(id.len(), 5);
            assert_eq!(id[0], 0, "generated ids start with a zero octet");
        }
        let counter = |id: &Frame| u32::from_be_bytes(id[1..].try_into().unwrap());
        assert_eq!(counter(&second), counter(&first).wrapping_add(1));
    }

    #[test]
    fn router_ignores_a_duplicate_identity() {
        let (a, mut sa) = connection(10);
        let (b, mut sb) = connection(10);
        sa.establish("same");
        sb.establish("same");
        let mut peers = list(vec![a, b]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        assert_eq!(table.routing_id(keys[1]), None, "the newcomer is ignored");
        assert_eq!(table.try_send(&mut peers, b"same", msg("x")), Ok(()));
        assert_eq!(drain(&mut sa), [msg("x")]);
        assert!(drain(&mut sb).is_empty());
    }

    #[test]
    fn with_handover_a_duplicate_identity_replaces_the_holder() {
        let (a, sa) = connection(10);
        let (b, mut sb) = connection(10);
        sa.establish("same");
        sb.establish("same");
        let mut peers = list(vec![a, b]);
        let keys: Vec<PeerKey> = peers.iter().map(Peer::key).collect();
        let mut table = RouterTable::default();
        table.set_handover(true);
        table.sync(&mut peers);
        assert_eq!(table.routing_id(keys[0]), None);
        assert_eq!(table.routing_id(keys[1]), Some(&Frame::from("same")));
        assert!(peers.get(keys[0]).is_none(), "the holder is disconnected");
        assert_eq!(table.try_send(&mut peers, b"same", msg("x")), Ok(()));
        assert_eq!(drain(&mut sb), [msg("x")]);
    }

    #[test]
    fn router_forgets_peers_that_are_gone() {
        let (a, sa) = connection(10);
        sa.establish("gone");
        let mut peers = list(vec![a]);
        let mut table = RouterTable::default();
        table.sync(&mut peers);
        drop(sa);
        assert_eq!(
            table.try_send(&mut peers, b"gone", msg("w")),
            Err(RouterSendError::UnknownRoutingId(msg("w"))),
            "gone but not yet forgotten"
        );
        peers.retain_mut(|peer| !peer.connection.tx.is_closed());
        table.sync(&mut peers);
        assert_eq!(
            table.try_send(&mut peers, b"gone", msg("x")),
            Err(RouterSendError::UnknownRoutingId(msg("x")))
        );
        // A new peer may now take the identity.
        let (b, mut sb) = connection(10);
        sb.establish("gone");
        peers.push(b);
        table.sync(&mut peers);
        assert_eq!(table.try_send(&mut peers, b"gone", msg("y")), Ok(()));
        assert_eq!(drain(&mut sb), [msg("y")]);
    }
}
