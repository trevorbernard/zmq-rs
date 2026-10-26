//! Lock-free single-producer, single-consumer message pipes.
//!
//! The Rust counterpart of libzmq's `ypipe`: a queue between exactly one
//! [`Writer`] and one [`Reader`], with a high-water mark and wakers so either
//! side can wait by blocking the thread or by `.await`, on any executor.
//!
//! Like libzmq, a writer that has hit the high-water mark is woken once the
//! reader has caught up to the low-water mark, half the high-water mark,
//! rather than after every message.

use std::fmt;
use std::future;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::atomic::{self, AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use atomic_waker::AtomicWaker;

mod queue;

/// How many messages a pipe holds before the writer must wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Hwm {
    /// No limit, like a libzmq HWM of zero.
    #[default]
    Unlimited,
    /// At most this many queued messages.
    Messages(NonZeroUsize),
}

/// Creates a pipe.
pub fn pipe<T>(hwm: Hwm) -> (Writer<T>, Reader<T>) {
    let (producer, consumer) = queue::queue();
    let hwm = limit(hwm);
    let shared = Arc::new(Shared {
        hwm: AtomicU64::new(hwm.unwrap_or(UNLIMITED)),
        ..Shared::default()
    });
    let writer = Writer {
        sender: Some(Sender::Queue(producer)),
        shared: shared.clone(),
        written: 0,
        peer_read: 0,
        hwm,
    };
    let reader = Reader {
        receiver: Receiver::Queue(consumer),
        shared,
        read: 0,
        lwm: low_water_mark(hwm),
    };
    (writer, reader)
}

/// How many items the reader takes between waking a waiting writer, from
/// the high-water mark, as libzmq's `pipe_t::compute_lwm`.
fn low_water_mark(hwm: Option<u64>) -> Option<u64> {
    hwm.map(|hwm| hwm.div_ceil(2))
}

/// Creates a pipe that holds only the latest item, like libzmq's
/// `ypipe_conflate`: each send replaces whatever the reader has not taken
/// yet, so the writer never waits.
pub fn pipe_conflated<T>() -> (Writer<T>, Reader<T>) {
    let latest = Arc::new(Mutex::new(None));
    let shared = Arc::new(Shared::default());
    let writer = Writer {
        sender: Some(Sender::Latest(latest.clone())),
        shared: shared.clone(),
        written: 0,
        peer_read: 0,
        hwm: None,
    };
    let reader = Reader {
        receiver: Receiver::Latest(latest),
        shared,
        read: 0,
        lwm: None,
    };
    (writer, reader)
}

/// Where a writer puts items: a queue, or a slot for the latest one.
#[derive(Debug)]
enum Sender<T> {
    Queue(queue::Producer<T>),
    Latest(Arc<Mutex<Option<T>>>),
}

#[derive(Debug)]
enum Receiver<T> {
    Queue(queue::Consumer<T>),
    Latest(Arc<Mutex<Option<T>>>),
}

/// The most items a pipe holds, or `None` for no limit.
fn limit(hwm: Hwm) -> Option<u64> {
    match hwm {
        Hwm::Unlimited => None,
        Hwm::Messages(n) => Some(n.get() as u64),
    }
}

/// `Shared::hwm` for no limit: a limit is never zero.
const UNLIMITED: u64 = 0;

/// Ignores poisoning: the slot is only ever replaced or taken whole, so a
/// panic while holding the lock cannot leave it half updated.
fn lock<T>(slot: &Mutex<Option<T>>) -> std::sync::MutexGuard<'_, Option<T>> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

/// State both ends see. Each hot field gets its own cache line, so the
/// reader's per-message writes to `read` and the wakers do not drag one line
/// back and forth between the two cores (false sharing).
#[derive(Debug, Default)]
struct Shared {
    /// Items the reader has taken, for the writer's high-water mark check.
    read: CachePadded<AtomicU64>,
    /// Set by the reader before it goes away.
    reader_closed: AtomicBool,
    /// Set by the writer before it goes away.
    writer_closed: AtomicBool,
    /// Items the writer sent in all, stored once as it goes away.
    final_written: AtomicU64,
    /// The high-water mark, [`UNLIMITED`] for none. Only ever raised, by
    /// [`Writer::raise_hwm`] or [`Reader::raise_hwm`]; the writer caches it
    /// and reloads it only when the pipe looks full.
    hwm: AtomicU64,
    reader: CachePadded<ReaderWait>,
    writer_waker: CachePadded<AtomicWaker>,
}

impl Shared {
    fn hwm(&self) -> Option<u64> {
        match self.hwm.load(Ordering::Acquire) {
            UNLIMITED => None,
            hwm => Some(hwm),
        }
    }

    fn raise_hwm(&self, hwm: Hwm) {
        let new = limit(hwm).unwrap_or(UNLIMITED);
        let _ = self
            .hwm
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                let higher = current != UNLIMITED && (new == UNLIMITED || new > current);
                higher.then_some(new)
            });
    }
}

/// How a waiting reader is woken. One line: a conflated pipe's writer reads
/// `waiting` on every send, and touches `waker` only when that is set.
#[derive(Debug, Default)]
struct ReaderWait {
    /// Raised by a conflated pipe's reader about to wait, so the writer
    /// wakes it only then. A queue keeps this state in its own counter.
    waiting: AtomicBool,
    waker: AtomicWaker,
}

/// Aligns `T` to its own cache line. 128 octets covers Apple silicon's line
/// and x86-64's adjacent-line prefetcher, as crossbeam does.
#[repr(align(128))]
#[derive(Debug, Default)]
struct CachePadded<T>(T);

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

/// The sending end of a pipe.
#[derive(Debug)]
pub struct Writer<T> {
    /// `None` only while dropping, so the reader can see the disconnect.
    sender: Option<Sender<T>>,
    shared: Arc<Shared>,
    written: u64,
    /// The reader's progress as last loaded from `Shared::read`. It only
    /// lags the truth, so it can make the pipe look full but never let the
    /// writer exceed the high-water mark.
    peer_read: u64,
    hwm: Option<u64>,
}

/// The receiving end of a pipe.
#[derive(Debug)]
pub struct Reader<T> {
    receiver: Receiver<T>,
    shared: Arc<Shared>,
    read: u64,
    lwm: Option<u64>,
}

/// Why [`Writer::try_send`] did not queue the item, which it hands back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The pipe is at its high-water mark.
    Full(T),
    /// The reader is gone.
    Closed(T),
}

/// The reader is gone; the item is handed back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Closed<T>(pub T);

/// Why [`Reader::try_recv`] returned nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// Nothing is queued yet.
    Empty,
    /// The writer is gone and everything it sent has been received.
    Closed,
}

/// Why [`Reader::recv_timeout`] returned nothing, as std's
/// `mpsc::RecvTimeoutError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvTimeoutError {
    /// Nothing arrived in time.
    Timeout,
    /// The writer is gone and everything it sent has been received.
    Disconnected,
}

impl<T> Writer<T> {
    /// Raises the high-water mark to `hwm` if that is higher, as libzmq's
    /// `pipe_t::set_hwms` does when an inproc bind adds the binder's own to
    /// a pipe made before it. Never lowers it.
    pub fn raise_hwm(&mut self, hwm: Hwm) {
        self.shared.raise_hwm(hwm);
        self.hwm = self.shared.hwm();
    }

    /// Queues `item` if the pipe is below its high-water mark.
    pub fn try_send(&mut self, item: T) -> Result<(), TrySendError<T>> {
        if self.is_closed() {
            return Err(TrySendError::Closed(item));
        }
        if self.is_full() {
            return Err(TrySendError::Full(item));
        }
        let reader_waiting = match &mut self.sender {
            Some(Sender::Queue(producer)) => producer.push(item),
            Some(Sender::Latest(latest)) => {
                *lock(latest) = Some(item);
                // Pairs with the fence in `Reader::poll_recv`: either the
                // reader's last check sees this item, or this load sees its
                // flag.
                atomic::fence(Ordering::SeqCst);
                self.shared.reader.waiting.load(Ordering::Relaxed)
                    && self.shared.reader.waiting.swap(false, Ordering::Acquire)
            }
            None => return Err(TrySendError::Closed(item)),
        };
        self.written += 1;
        if reader_waiting {
            self.shared.reader.waker.wake();
        }
        Ok(())
    }

    /// Ready once an item can be queued, or with `Err(())` once the reader
    /// is gone.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        if let Poll::Ready(ready) = self.ready() {
            return Poll::Ready(ready);
        }
        // Register, then check again so a read in between is not missed.
        self.shared.writer_waker.register(cx.waker());
        self.ready()
    }

    /// Queues `item`, waiting while the pipe is full.
    pub async fn send(&mut self, mut item: T) -> Result<(), Closed<T>> {
        loop {
            if future::poll_fn(|cx| self.poll_ready(cx)).await.is_err() {
                return Err(Closed(item));
            }
            match self.try_send(item) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(rejected)) => item = rejected,
                Err(TrySendError::Closed(rejected)) => return Err(Closed(rejected)),
            }
        }
    }

    /// Queues `item`, blocking the thread while the pipe is full.
    pub fn send_blocking(&mut self, mut item: T) -> Result<(), Closed<T>> {
        loop {
            if block_on_poll(|cx| self.poll_ready(cx)).is_err() {
                return Err(Closed(item));
            }
            match self.try_send(item) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(rejected)) => item = rejected,
                Err(TrySendError::Closed(rejected)) => return Err(Closed(rejected)),
            }
        }
    }

    /// Ready once the reader is gone, so a writer can stop waiting for room
    /// it will never get. Useful when the pipe is never full.
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_closed() {
            return Poll::Ready(());
        }
        // Register, then check again so a drop in between is not missed.
        self.shared.writer_waker.register(cx.waker());
        if self.is_closed() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    /// Waits until the reader is gone.
    pub async fn closed(&mut self) {
        future::poll_fn(|cx| self.poll_closed(cx)).await;
    }

    /// Whether the reader is gone.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared.reader_closed.load(Ordering::Acquire)
    }

    /// Checks the high-water mark against the cached reader progress, and
    /// reloads the reader's shared counter only when that looks full. Like
    /// libzmq's `_peers_msgs_read`, this keeps the writer from pulling the
    /// reader's cache line across cores on every send.
    fn is_full(&mut self) -> bool {
        let Some(hwm) = self.hwm else {
            return false;
        };
        if self.written - self.peer_read < hwm {
            return false;
        }
        self.peer_read = self.shared.read.load(Ordering::Acquire);
        if self.written - self.peer_read < hwm {
            return false;
        }
        // The mark may have been raised since it was cached.
        self.hwm = self.shared.hwm();
        self.hwm
            .is_some_and(|hwm| self.written - self.peer_read >= hwm)
    }

    fn ready(&mut self) -> Poll<Result<(), ()>> {
        if self.is_closed() {
            Poll::Ready(Err(()))
        } else if self.is_full() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<T> Drop for Writer<T> {
    fn drop(&mut self) {
        // Disconnect first, so a reader woken here sees the pipe closed.
        drop(self.sender.take());
        self.shared
            .final_written
            .store(self.written, Ordering::Relaxed);
        self.shared.writer_closed.store(true, Ordering::Release);
        self.shared.reader.waker.wake();
    }
}

impl<T> Reader<T> {
    /// Raises the high-water mark to `hwm` if that is higher, as
    /// [`Writer::raise_hwm`] does from this end, and wakes a writer waiting
    /// for room.
    pub fn raise_hwm(&mut self, hwm: Hwm) {
        if matches!(self.receiver, Receiver::Latest(_)) {
            return;
        }
        self.shared.raise_hwm(hwm);
        self.lwm = low_water_mark(self.shared.hwm());
        self.shared.writer_waker.wake();
    }

    /// Takes the next item if one is queued.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.take(false)
    }

    /// Takes the next item. With `sleep`, finding none tells the writer to
    /// wake the registered waker on its next send.
    fn take(&mut self, sleep: bool) -> Result<T, TryRecvError> {
        let item = match &mut self.receiver {
            Receiver::Queue(consumer) => match consumer.pop(sleep) {
                Some(item) => item,
                // Loaded after the miss: once closed, everything the writer
                // sent is published, so a second look is final.
                None if self.shared.writer_closed.load(Ordering::Acquire) => {
                    consumer.pop(false).ok_or(TryRecvError::Closed)?
                }
                None => return Err(TryRecvError::Empty),
            },
            Receiver::Latest(latest) => {
                // Loaded first: once closed, the last item is already stored.
                let closed = self.shared.writer_closed.load(Ordering::Acquire);
                match lock(latest).take() {
                    Some(item) => item,
                    None if closed => return Err(TryRecvError::Closed),
                    None => return Err(TryRecvError::Empty),
                }
            }
        };
        self.read += 1;
        self.shared.read.store(self.read, Ordering::Release);
        if self.lwm.is_some_and(|lwm| self.read.is_multiple_of(lwm)) {
            // The writer may have raised the mark since, which libzmq's
            // set_hwms gives both ends at once: catch up here.
            self.lwm = low_water_mark(self.shared.hwm());
            self.shared.writer_waker.wake();
        }
        Ok(item)
    }

    /// The next item, or `None` once the writer is gone and the pipe drained.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if let Poll::Ready(item) = self.ready(false) {
            return Poll::Ready(item);
        }
        // Register and tell the writer we are waiting, then check again so
        // a send in between is not missed.
        self.shared.reader.waker.register(cx.waker());
        if let Receiver::Latest(_) = self.receiver {
            self.shared.reader.waiting.store(true, Ordering::Release);
            atomic::fence(Ordering::SeqCst);
        }
        self.ready(true)
    }

    /// Whether the writer is gone. Items it sent may still be queued.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared.writer_closed.load(Ordering::Acquire)
    }

    /// Whether the writer is gone and every item it sent has been taken,
    /// found without taking one.
    #[must_use]
    pub fn is_drained(&self) -> bool {
        // Acquire pairs with the writer's release, publishing its count.
        if !self.is_closed() {
            return false;
        }
        match &self.receiver {
            Receiver::Queue(_) => self.read == self.shared.final_written.load(Ordering::Relaxed),
            Receiver::Latest(latest) => lock(latest).is_none(),
        }
    }

    /// Ready once the writer is gone, even if items are still queued.
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_closed() {
            return Poll::Ready(());
        }
        // Register, then check again so a drop in between is not missed.
        self.shared.reader.waker.register(cx.waker());
        if self.is_closed() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    /// The next item, waiting while the pipe is empty.
    pub async fn recv(&mut self) -> Option<T> {
        future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// The next item, blocking the thread while the pipe is empty.
    pub fn recv_blocking(&mut self) -> Option<T> {
        block_on_poll(|cx| self.poll_recv(cx))
    }

    /// The next item, blocking the thread for at most `timeout` while the
    /// pipe is empty.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        match block_on_poll_until(Instant::now().checked_add(timeout), |cx| self.poll_recv(cx)) {
            Some(Some(item)) => Ok(item),
            Some(None) => Err(RecvTimeoutError::Disconnected),
            None => Err(RecvTimeoutError::Timeout),
        }
    }

    fn ready(&mut self, sleep: bool) -> Poll<Option<T>> {
        match self.take(sleep) {
            Ok(item) => Poll::Ready(Some(item)),
            Err(TryRecvError::Closed) => Poll::Ready(None),
            Err(TryRecvError::Empty) => Poll::Pending,
        }
    }
}

impl<T> Drop for Reader<T> {
    fn drop(&mut self) {
        self.shared.reader_closed.store(true, Ordering::Release);
        self.shared.writer_waker.wake();
    }
}

/// Polls until ready, parking the thread in between.
fn block_on_poll<R>(poll: impl FnMut(&mut Context<'_>) -> Poll<R>) -> R {
    block_on_poll_until(None, poll).expect("no deadline")
}

/// Polls until ready or until `deadline`, parking the thread in between.
/// `None` once a poll at or after the deadline is still pending; a `None`
/// deadline waits forever.
fn block_on_poll_until<R>(
    deadline: Option<Instant>,
    mut poll: impl FnMut(&mut Context<'_>) -> Poll<R>,
) -> Option<R> {
    struct Unpark(Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    // One waker per thread: a fresh one per call is allocated here and,
    // once the peer's wake drops the last reference, freed on the peer's
    // thread, a cross-thread free per item that glibc handles badly.
    thread_local! {
        static WAKER: Waker = Waker::from(Arc::new(Unpark(thread::current())));
    }
    // Borrowed, not cloned: a clone and its drop are two atomic operations
    // on every call.
    let mut run = |waker: &Waker| {
        let mut cx = Context::from_waker(waker);
        loop {
            if let Poll::Ready(output) = poll(&mut cx) {
                return Some(output);
            }
            // Spurious wakeups are harmless: the loop polls again.
            match deadline {
                None => thread::park(),
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return None;
                    }
                    thread::park_timeout(deadline - now);
                }
            }
        }
    };
    match WAKER.try_with(|waker| run(waker)) {
        Ok(output) => output,
        Err(_) => run(&Waker::from(Arc::new(Unpark(thread::current())))),
    }
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Empty => "pipe is empty",
            Self::Closed => "pipe is closed",
        })
    }
}

impl std::error::Error for TryRecvError {}

impl fmt::Display for RecvTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Timeout => "timed out waiting on the pipe",
            Self::Disconnected => "pipe is closed",
        })
    }
}

impl std::error::Error for RecvTimeoutError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    fn hwm(n: usize) -> Hwm {
        Hwm::Messages(NonZeroUsize::new(n).unwrap())
    }

    /// A waker that counts how often it was woken.
    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counting_waker() -> (Arc<Count>, Waker) {
        let count = Arc::new(Count::default());
        (count.clone(), Waker::from(count))
    }

    fn wakes(count: &Count) -> usize {
        count.0.load(Ordering::SeqCst)
    }

    #[test]
    fn a_conflated_pipe_keeps_only_the_latest_item() {
        let (mut writer, mut reader) = pipe_conflated();
        for i in 0..100 {
            writer.try_send(i).unwrap();
        }
        assert_eq!(reader.try_recv(), Ok(99));
        assert_eq!(reader.try_recv(), Err(TryRecvError::Empty));
        writer.try_send(100).unwrap();
        assert_eq!(reader.try_recv(), Ok(100));
    }

    #[test]
    fn a_conflated_pipe_delivers_its_last_item_after_the_writer_drops() {
        let (mut writer, mut reader) = pipe_conflated();
        writer.try_send(1).unwrap();
        drop(writer);
        assert!(reader.is_closed());
        assert_eq!(reader.try_recv(), Ok(1));
        assert_eq!(reader.try_recv(), Err(TryRecvError::Closed));
    }

    #[test]
    fn a_conflated_pipe_wakes_its_reader_and_sees_it_drop() {
        let (mut writer, mut reader) = pipe_conflated();
        let (count, waker) = counting_waker();
        assert_eq!(
            reader.poll_recv(&mut Context::from_waker(&waker)),
            Poll::Pending
        );
        writer.try_send(1).unwrap();
        assert_eq!(wakes(&count), 1);
        assert_eq!(
            writer.poll_ready(&mut Context::from_waker(&waker)),
            Poll::Ready(Ok(()))
        );
        drop(reader);
        assert_eq!(writer.try_send(2), Err(TrySendError::Closed(2)));
    }

    #[test]
    fn handles_are_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Writer<Vec<u8>>>();
        assert_send::<Reader<Vec<u8>>>();
    }

    #[test]
    fn items_arrive_in_order() {
        let (mut writer, mut reader) = pipe(Hwm::Unlimited);
        for i in 0..100 {
            writer.try_send(i).unwrap();
        }
        let received: Vec<i32> = std::iter::from_fn(|| reader.try_recv().ok()).collect();
        assert_eq!(received, (0..100).collect::<Vec<_>>());
        assert_eq!(reader.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn unlimited_pipe_is_never_full() {
        let (mut writer, _reader) = pipe(Hwm::Unlimited);
        for i in 0..100_000 {
            writer.try_send(i).unwrap();
        }
    }

    #[test]
    fn writer_stops_at_the_high_water_mark() {
        let (mut writer, mut reader) = pipe(hwm(4));
        for i in 0..4 {
            writer.try_send(i).unwrap();
        }
        assert_eq!(writer.try_send(4), Err(TrySendError::Full(4)));
        assert_eq!(reader.try_recv(), Ok(0));
        writer.try_send(4).unwrap();
        assert_eq!(
            writer.try_send(5),
            Err(TrySendError::Full(5)),
            "never more than 4 queued"
        );
    }

    #[test]
    fn the_reader_can_raise_the_mark_for_a_full_writer() {
        let (mut writer, mut reader) = pipe(hwm(1));
        writer.try_send(0).unwrap();
        let (woken, waker) = counting_waker();
        assert!(
            writer
                .poll_ready(&mut Context::from_waker(&waker))
                .is_pending()
        );
        reader.raise_hwm(hwm(3));
        assert!(wakes(&woken) > 0, "the waiting writer is woken");
        writer.try_send(1).unwrap();
        writer.try_send(2).unwrap();
        assert_eq!(writer.try_send(3), Err(TrySendError::Full(3)));
    }

    #[test]
    fn the_writer_can_raise_its_own_mark() {
        let (mut writer, _reader) = pipe(hwm(1));
        writer.raise_hwm(hwm(2));
        writer.try_send(0).unwrap();
        writer.try_send(1).unwrap();
        assert_eq!(writer.try_send(2), Err(TrySendError::Full(2)));
    }

    #[test]
    fn the_reader_catches_up_with_a_mark_the_writer_raised() {
        let (mut writer, mut reader) = pipe(hwm(2));
        writer.raise_hwm(hwm(8));
        for i in 0..8 {
            writer.try_send(i).unwrap();
        }
        reader.try_recv().unwrap();
        assert_eq!(reader.lwm, Some(4), "as libzmq's set_hwms recomputes it");
    }

    #[test]
    fn raising_to_unlimited_lifts_the_mark() {
        let (mut writer, mut reader) = pipe(hwm(1));
        reader.raise_hwm(Hwm::Unlimited);
        for i in 0..100 {
            writer.try_send(i).unwrap();
        }
    }

    #[test]
    fn the_mark_is_never_lowered() {
        let (mut writer, mut reader) = pipe(hwm(3));
        reader.raise_hwm(hwm(1));
        writer.raise_hwm(hwm(2));
        for i in 0..3 {
            writer.try_send(i).unwrap();
        }
        assert_eq!(writer.try_send(3), Err(TrySendError::Full(3)));
        let (mut writer, mut reader) = pipe(Hwm::Unlimited);
        reader.raise_hwm(hwm(1));
        for i in 0..10 {
            writer.try_send(i).unwrap();
        }
    }

    #[test]
    fn reader_drains_then_sees_closed() {
        let (mut writer, mut reader) = pipe(Hwm::Unlimited);
        writer.try_send(1).unwrap();
        drop(writer);
        assert_eq!(reader.try_recv(), Ok(1));
        assert_eq!(reader.try_recv(), Err(TryRecvError::Closed));
        assert_eq!(reader.recv_blocking(), None);
    }

    #[test]
    fn drained_once_the_writer_is_gone_and_nothing_is_left() {
        let (mut writer, mut reader) = pipe(Hwm::Unlimited);
        assert!(!reader.is_drained());
        writer.try_send(1).unwrap();
        drop(writer);
        assert!(!reader.is_drained());
        assert_eq!(reader.try_recv(), Ok(1));
        assert!(reader.is_drained());

        let (mut writer, mut reader) = pipe_conflated();
        writer.try_send(1).unwrap();
        writer.try_send(2).unwrap();
        drop(writer);
        assert!(!reader.is_drained());
        assert_eq!(reader.try_recv(), Ok(2));
        assert!(reader.is_drained());
    }

    #[test]
    fn send_to_a_dropped_reader_returns_the_item() {
        let (mut writer, reader) = pipe(hwm(1));
        drop(reader);
        assert!(writer.is_closed());
        assert_eq!(writer.try_send(7), Err(TrySendError::Closed(7)));
        assert_eq!(writer.send_blocking(8), Err(Closed(8)));
    }

    #[test]
    fn pending_reader_is_woken_by_send() {
        let (mut writer, mut reader) = pipe(Hwm::Unlimited);
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(reader.poll_recv(&mut cx), Poll::Pending);
        writer.try_send(1).unwrap();
        assert_eq!(wakes(&count), 1);
        assert_eq!(reader.poll_recv(&mut cx), Poll::Ready(Some(1)));
    }

    #[test]
    fn pending_reader_is_woken_when_writer_drops() {
        let (writer, mut reader) = pipe::<i32>(Hwm::Unlimited);
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(reader.poll_recv(&mut cx), Poll::Pending);
        drop(writer);
        assert_eq!(wakes(&count), 1);
        assert_eq!(reader.poll_recv(&mut cx), Poll::Ready(None));
    }

    #[test]
    fn writer_is_woken_when_reader_drops_even_with_room() {
        let (mut writer, reader) = pipe::<i32>(Hwm::Unlimited);
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(writer.poll_closed(&mut cx), Poll::Pending);
        drop(reader);
        assert_eq!(wakes(&count), 1);
        assert_eq!(writer.poll_closed(&mut cx), Poll::Ready(()));
        block_on(writer.closed());
    }

    #[test]
    fn reader_learns_the_writer_is_gone_before_draining() {
        let (mut writer, mut reader) = pipe(Hwm::Unlimited);
        writer.try_send(1).unwrap();
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(!reader.is_closed());
        assert_eq!(reader.poll_closed(&mut cx), Poll::Pending);
        drop(writer);
        assert_eq!(wakes(&count), 1);
        assert!(reader.is_closed());
        assert_eq!(reader.poll_closed(&mut cx), Poll::Ready(()));
        assert_eq!(reader.try_recv(), Ok(1), "queued items are still there");
        assert_eq!(reader.try_recv(), Err(TryRecvError::Closed));
    }

    #[test]
    fn full_writer_is_woken_at_the_low_water_mark() {
        let (mut writer, mut reader) = pipe(hwm(4)); // low-water mark 2
        for i in 0..4 {
            writer.try_send(i).unwrap();
        }
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(writer.poll_ready(&mut cx), Poll::Pending);
        reader.try_recv().unwrap();
        assert_eq!(
            wakes(&count),
            0,
            "one read is not enough to wake the writer"
        );
        reader.try_recv().unwrap();
        assert_eq!(wakes(&count), 1, "woken at the low-water mark");
        assert_eq!(writer.poll_ready(&mut cx), Poll::Ready(Ok(())));
    }

    #[test]
    fn pending_writer_is_woken_when_reader_drops() {
        let (mut writer, reader) = pipe(hwm(1));
        writer.try_send(0).unwrap();
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(writer.poll_ready(&mut cx), Poll::Pending);
        drop(reader);
        assert_eq!(wakes(&count), 1);
        assert_eq!(writer.poll_ready(&mut cx), Poll::Ready(Err(())));
    }

    /// A minimal `block_on`: polls with a waker that unparks this thread.
    fn block_on<F: Future>(future: F) -> F::Output {
        struct Unpark(thread::Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(Unpark(thread::current())));
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
            thread::park();
        }
    }

    #[test]
    fn async_send_and_recv_need_no_runtime() {
        let (mut writer, mut reader) = pipe(hwm(1));
        let consumer = thread::spawn(move || {
            block_on(async {
                let mut received = Vec::new();
                while let Some(item) = reader.recv().await {
                    received.push(item);
                }
                received
            })
        });
        block_on(async {
            for i in 0..1_000 {
                writer.send(i).await.unwrap();
            }
        });
        drop(writer);
        assert_eq!(consumer.join().unwrap(), (0..1_000).collect::<Vec<_>>());
    }

    #[test]
    fn blocking_threads_never_lose_a_wakeup() {
        for hwm_size in [1, 2, 3, 16] {
            let (mut writer, mut reader) = pipe(hwm(hwm_size));
            let consumer = thread::spawn(move || {
                let mut expected = 0u64;
                while let Some(item) = reader.recv_blocking() {
                    assert_eq!(item, expected);
                    expected += 1;
                }
                expected
            });
            let total = if cfg!(miri) { 2_000 } else { 200_000u64 };
            for i in 0..total {
                writer.send_blocking(i).unwrap();
            }
            drop(writer);
            assert_eq!(consumer.join().unwrap(), total, "hwm {hwm_size}");
        }
    }

    #[test]
    fn recv_timeout_times_out_then_receives_then_sees_the_close() {
        let (mut writer, mut reader) = pipe::<u32>(Hwm::Unlimited);
        let short = Duration::from_millis(50);
        let start = Instant::now();
        assert_eq!(reader.recv_timeout(short), Err(RecvTimeoutError::Timeout));
        assert!(start.elapsed() >= short);
        let late = thread::spawn(move || {
            thread::sleep(short);
            writer.try_send(7).unwrap();
        });
        assert_eq!(reader.recv_timeout(Duration::from_secs(60)), Ok(7));
        late.join().unwrap();
        assert_eq!(
            reader.recv_timeout(Duration::MAX),
            Err(RecvTimeoutError::Disconnected)
        );
    }

    #[test]
    fn blocking_recv_waits_for_a_late_writer() {
        let (mut writer, mut reader) = pipe(Hwm::Unlimited);
        let consumer = thread::spawn(move || reader.recv_blocking());
        thread::sleep(Duration::from_millis(20));
        writer.try_send("late").unwrap();
        assert_eq!(consumer.join().unwrap(), Some("late"));
    }

    #[test]
    fn hot_shared_fields_sit_on_separate_cache_lines() {
        // The reader writes `read` on every receive and both sides touch the
        // wakers; sharing a line makes it bounce between cores per message.
        const LINE: usize = 128;
        let shared = Shared::default();
        let lines = [
            std::ptr::from_ref(&shared.read).addr() / LINE,
            std::ptr::from_ref(&shared.reader).addr() / LINE,
            std::ptr::from_ref(&shared.writer_waker).addr() / LINE,
        ];
        assert!(
            lines[0] != lines[1] && lines[1] != lines[2] && lines[0] != lines[2],
            "fields share a cache line: {lines:?}"
        );
    }

    #[test]
    fn error_messages() {
        assert_eq!(TryRecvError::Empty.to_string(), "pipe is empty");
        assert_eq!(TryRecvError::Closed.to_string(), "pipe is closed");
    }
}
