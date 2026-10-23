//! Lock-free single-producer, single-consumer byte log.
//!
//! A cut-down Aeron term buffer: one [`LogWriter`] copies octets into a
//! pre-allocated ring and commits them, and one [`LogReader`] sees the
//! committed octets as at most two slices, ready for `writev`, and releases
//! them once written. Positions only grow, so wrapping is a mask, and each
//! side caches the other's position, reloading it only when the log looks
//! full or empty.
//!
//! The writer also counts messages, so the log can enforce a high-water mark
//! in messages as well as its capacity in octets. A waiting writer is woken
//! once the space it asked for is free and, as in `zmq-pipe`, no sooner
//! than half the log has drained.

use std::cell::UnsafeCell;
use std::fmt;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::Arc;
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;

use sync::{AtomicBool, AtomicU64, Ordering, fence};

#[cfg(loom)]
mod sync {
    pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
}

#[cfg(not(loom))]
mod sync {
    pub(crate) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
}

/// Creates a log of at least `capacity` octets, rounded up to a power of
/// two, holding at most `hwm` unreleased messages (`None` for no limit).
pub fn log(capacity: NonZeroUsize, hwm: Option<NonZeroUsize>) -> (LogWriter, LogReader) {
    let capacity = capacity.get().next_power_of_two();
    let shared = Arc::new(Shared {
        buf: (0..capacity).map(|_| UnsafeCell::new(0)).collect(),
        written: CachePadded(AtomicU64::new(0)),
        released: CachePadded(AtomicU64::new(0)),
        messages_written: CachePadded(AtomicU64::new(0)),
        messages_released: CachePadded(AtomicU64::new(0)),
        writer_closed: AtomicBool::new(false),
        reader_closed: AtomicBool::new(false),
        reader: CachePadded(ReaderWait::default()),
        writer: CachePadded(WriterWait::default()),
    });
    let writer = LogWriter {
        shared: shared.clone(),
        capacity: capacity as u64,
        tail: 0,
        published: 0,
        released: 0,
        messages_written: 0,
        messages_published: 0,
        messages_released: 0,
        hwm: hwm.map(|hwm| hwm.get() as u64),
    };
    let reader = LogReader {
        shared,
        capacity: capacity as u64,
        released: 0,
        written: 0,
        messages_released: 0,
    };
    (writer, reader)
}

struct Shared {
    buf: Box<[UnsafeCell<u8>]>,
    /// Octets committed by the writer.
    written: CachePadded<AtomicU64>,
    /// Octets released by the reader.
    released: CachePadded<AtomicU64>,
    messages_written: CachePadded<AtomicU64>,
    messages_released: CachePadded<AtomicU64>,
    writer_closed: AtomicBool,
    reader_closed: AtomicBool,
    reader: CachePadded<ReaderWait>,
    writer: CachePadded<WriterWait>,
}

// SAFETY: the buffer is shared between exactly one writer and one reader.
// The writer only writes octets in `[written, released + capacity)` and the
// reader only reads octets in `[released, written)`; the two ranges never
// overlap, and each position is published with `Release` and loaded with
// `Acquire` before the other side touches the octets it covers.
unsafe impl Sync for Shared {}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("capacity", &self.buf.len())
            .field("written", &self.written.load(Ordering::Relaxed))
            .field("released", &self.released.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Shared {
    fn base(&self) -> *mut u8 {
        UnsafeCell::raw_get(self.buf.as_ptr())
    }
}

/// How a waiting reader is woken: the writer checks `waiting` on every
/// commit and touches `waker` only when it is set.
#[derive(Debug, Default)]
struct ReaderWait {
    waiting: AtomicBool,
    waker: AtomicWaker,
}

/// How a waiting writer is woken: once `released` and `messages_released`
/// reach what it asked for.
#[derive(Debug, Default)]
struct WriterWait {
    waiting: AtomicBool,
    wanted_released: AtomicU64,
    wanted_messages: AtomicU64,
    waker: AtomicWaker,
}

/// Aligns `T` to its own cache line, as in `zmq-pipe`.
#[repr(align(128))]
#[derive(Debug, Default)]
struct CachePadded<T>(T);

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

/// The reader is gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

/// The writing end of a log.
#[derive(Debug)]
pub struct LogWriter {
    shared: Arc<Shared>,
    capacity: u64,
    /// End of what has been put, committed or not.
    tail: u64,
    /// End of what has been committed.
    published: u64,
    /// The reader's `released` as last loaded. It only lags, so it can make
    /// the log look full but never let the writer overwrite unread octets.
    released: u64,
    messages_written: u64,
    messages_published: u64,
    messages_released: u64,
    hwm: Option<u64>,
}

impl LogWriter {
    /// The log's size in octets.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity as usize
    }

    /// Octets that can be put now, reloading the reader's progress only when
    /// the cached view has less than `want` free.
    #[inline]
    fn free_for(&mut self, want: u64) -> u64 {
        let free = self.capacity - (self.tail - self.released);
        if free >= want {
            return free;
        }
        self.released = self.shared.released.load(Ordering::Acquire);
        self.capacity - (self.tail - self.released)
    }

    /// Octets that can be put now.
    #[inline]
    pub fn free(&mut self) -> usize {
        self.free_for(u64::MAX) as usize
    }

    /// Whether another message is allowed under the high-water mark.
    #[inline]
    fn under_hwm(&mut self) -> bool {
        let Some(hwm) = self.hwm else {
            return true;
        };
        if self.messages_written - self.messages_released < hwm {
            return true;
        }
        self.messages_released = self.shared.messages_released.load(Ordering::Acquire);
        self.messages_written - self.messages_released < hwm
    }

    /// Whether `octets` fit now (all of the log, if more than that) and
    /// another message is allowed.
    #[inline]
    pub fn has_room(&mut self, octets: usize) -> bool {
        let want = (octets as u64).min(self.capacity);
        self.free_for(want) >= want && self.under_hwm()
    }

    /// Ready once `octets` fit (all of the log, if more than that) and
    /// another message is allowed, or with `Err(Closed)` once the reader is
    /// gone. While pending, `cx` is woken when that room is free and at
    /// least half the log, or half the high-water mark, has drained.
    /// Anything put but not committed is committed first, so the reader can
    /// make the room.
    pub fn poll_writable(
        &mut self,
        cx: &mut Context<'_>,
        octets: usize,
    ) -> Poll<Result<(), Closed>> {
        if self.is_closed() {
            return Poll::Ready(Err(Closed));
        }
        if self.has_room(octets) {
            return Poll::Ready(Ok(()));
        }
        self.commit();
        let want = (octets as u64).min(self.capacity);
        let wanted_released = if self.free_for(want) < want {
            (self.tail + want - self.capacity).max(self.tail.saturating_sub(self.capacity / 2))
        } else {
            0
        };
        let blocked_by_messages = !self.under_hwm();
        let wanted_messages = match self.hwm {
            Some(hwm) if blocked_by_messages => self.messages_written.saturating_sub(hwm / 2),
            _ => 0,
        };
        let wait = &self.shared.writer;
        wait.waker.register(cx.waker());
        wait.wanted_released
            .store(wanted_released, Ordering::Relaxed);
        wait.wanted_messages
            .store(wanted_messages, Ordering::Relaxed);
        wait.waiting.store(true, Ordering::Release);
        // Pairs with the fence in `LogReader::release`: either the reader's
        // check sees `waiting`, or these loads see its release.
        fence(Ordering::SeqCst);
        if self.is_closed() {
            return Poll::Ready(Err(Closed));
        }
        if self.has_room(octets) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    /// Copies `data` in after what has been put so far. Nothing reaches the
    /// reader until [`commit`](Self::commit).
    ///
    /// # Panics
    ///
    /// If `data` is longer than [`free`](Self::free).
    #[inline]
    pub fn put(&mut self, data: &[u8]) {
        let len = data.len() as u64;
        assert!(
            self.free_for(len) >= len,
            "put of {len} octets with {} free",
            self.free_for(len)
        );
        let start = (self.tail & (self.capacity - 1)) as usize;
        let first = data.len().min(self.capacity as usize - start);
        let base = self.shared.base();
        // SAFETY: `[tail, tail + len)` lies within `[released, released +
        // capacity)` (checked above), which the reader does not read until
        // `written` passes it. The two copies cover `[start, start + first)`
        // and `[0, len - first)`, both inside the buffer.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), base.add(start), first);
            if first < data.len() {
                std::ptr::copy_nonoverlapping(data.as_ptr().add(first), base, data.len() - first);
            }
        }
        self.tail += len;
    }

    /// Counts one more message, published with the next commit.
    #[inline]
    pub fn end_message(&mut self) {
        self.messages_written += 1;
    }

    /// Publishes everything put and every message ended so far, and wakes
    /// the reader if it is waiting.
    #[inline]
    pub fn commit(&mut self) {
        if self.tail == self.published && self.messages_written == self.messages_published {
            return;
        }
        self.published = self.tail;
        self.messages_published = self.messages_written;
        self.shared
            .messages_written
            .store(self.messages_written, Ordering::Release);
        self.shared.written.store(self.tail, Ordering::Release);
        // Pairs with the fence in `LogReader::poll_committed`.
        fence(Ordering::SeqCst);
        let wait = &self.shared.reader;
        if wait.waiting.load(Ordering::Relaxed) && wait.waiting.swap(false, Ordering::Acquire) {
            wait.waker.wake();
        }
    }

    /// Whether the reader is gone.
    pub fn is_closed(&self) -> bool {
        self.shared.reader_closed.load(Ordering::Acquire)
    }

    /// Ready once the reader is gone.
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_closed() {
            return Poll::Ready(());
        }
        self.shared.writer.waker.register(cx.waker());
        fence(Ordering::SeqCst);
        if self.is_closed() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        self.shared.writer_closed.store(true, Ordering::Release);
        // Pairs with the fence in `LogReader::poll_committed` and `poll_closed`.
        fence(Ordering::SeqCst);
        self.shared.reader.waker.wake();
    }
}

/// The reading end of a log.
#[derive(Debug)]
pub struct LogReader {
    shared: Arc<Shared>,
    capacity: u64,
    released: u64,
    /// The writer's `written` as last loaded.
    written: u64,
    messages_released: u64,
}

impl LogReader {
    /// The committed octets not yet released, in order: the second slice is
    /// non-empty only when they wrap around the end of the buffer.
    pub fn committed(&mut self) -> [&[u8]; 2] {
        self.written = self.shared.written.load(Ordering::Acquire);
        let len = (self.written - self.released) as usize;
        let start = (self.released & (self.capacity - 1)) as usize;
        let first = len.min(self.capacity as usize - start);
        let base = self.shared.base();
        // SAFETY: `[released, written)` was committed by the writer, which
        // will not touch it again until `release` publishes past it; loading
        // `written` with `Acquire` made those octets visible. Both slices lie
        // inside the buffer.
        unsafe {
            [
                std::slice::from_raw_parts(base.add(start), first),
                std::slice::from_raw_parts(base, len - first),
            ]
        }
    }

    /// Octets committed and not yet released, as last seen.
    pub fn pending(&mut self) -> usize {
        let [a, b] = self.committed();
        a.len() + b.len()
    }

    /// Frees `octets` from the front, and counts `messages` as done for the
    /// high-water mark. Wakes the writer if it is waiting for this room.
    ///
    /// # Panics
    ///
    /// If more octets are released than are committed.
    pub fn release(&mut self, octets: usize, messages: u64) {
        let octets = octets as u64;
        if self.released + octets > self.written {
            self.written = self.shared.written.load(Ordering::Acquire);
        }
        assert!(
            self.released + octets <= self.written,
            "release past what is committed"
        );
        if octets == 0 && messages == 0 {
            return;
        }
        self.released += octets;
        self.messages_released += messages;
        self.shared
            .messages_released
            .store(self.messages_released, Ordering::Release);
        self.shared.released.store(self.released, Ordering::Release);
        // Pairs with the fence in `LogWriter::poll_writable`.
        fence(Ordering::SeqCst);
        let wait = &self.shared.writer;
        if wait.waiting.load(Ordering::Acquire)
            && self.released >= wait.wanted_released.load(Ordering::Relaxed)
            && self.messages_released >= wait.wanted_messages.load(Ordering::Relaxed)
            && wait.waiting.swap(false, Ordering::Acquire)
        {
            wait.waker.wake();
        }
    }

    /// Ready with `true` once octets are committed, or `false` once the
    /// writer is gone and everything it committed has been released.
    pub fn poll_committed(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        if let Poll::Ready(ready) = self.ready() {
            return Poll::Ready(ready);
        }
        let wait = &self.shared.reader;
        wait.waker.register(cx.waker());
        wait.waiting.store(true, Ordering::Release);
        // Pairs with the fence in `LogWriter::commit`.
        fence(Ordering::SeqCst);
        self.ready()
    }

    fn ready(&mut self) -> Poll<bool> {
        // Loaded first: once closed, the last commit is already visible.
        let closed = self.is_closed();
        if self.pending() > 0 {
            Poll::Ready(true)
        } else if closed {
            Poll::Ready(false)
        } else {
            Poll::Pending
        }
    }

    /// Whether everything committed so far has been released.
    pub fn caught_up(&mut self) -> bool {
        self.pending() == 0
    }

    /// Whether the writer is gone. Octets it committed may remain.
    pub fn is_closed(&self) -> bool {
        self.shared.writer_closed.load(Ordering::Acquire)
    }

    /// Ready once the writer is gone, even if octets remain.
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_closed() {
            return Poll::Ready(());
        }
        self.shared.reader.waker.register(cx.waker());
        fence(Ordering::SeqCst);
        if self.is_closed() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for LogReader {
    fn drop(&mut self) {
        self.shared.reader_closed.store(true, Ordering::Release);
        // Pairs with the fence in `LogWriter::poll_writable` and `poll_closed`.
        fence(Ordering::SeqCst);
        self.shared.writer.waker.wake();
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Wake, Waker};

    fn n(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn counting_waker() -> (Arc<Count>, Waker) {
        let count = Arc::new(Count::default());
        (count.clone(), Waker::from(count))
    }

    fn wakes(count: &Count) -> usize {
        count.0.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn read_all(reader: &mut LogReader) -> Vec<u8> {
        let [a, b] = reader.committed();
        [a, b].concat()
    }

    #[test]
    fn capacity_rounds_up_to_a_power_of_two() {
        let (writer, _reader) = log(n(100), None);
        assert_eq!(writer.capacity(), 128);
    }

    #[test]
    fn nothing_is_visible_until_committed() {
        let (mut writer, mut reader) = log(n(64), None);
        writer.put(b"hello");
        assert!(read_all(&mut reader).is_empty());
        writer.commit();
        assert_eq!(read_all(&mut reader), b"hello");
    }

    #[test]
    fn octets_wrap_around_the_end() {
        let (mut writer, mut reader) = log(n(16), None);
        writer.put(&[1; 12]);
        writer.commit();
        reader.release(12, 0);
        writer.put(&[2, 3, 4, 5, 6, 7, 8, 9]);
        writer.commit();
        let [a, b] = reader.committed();
        assert_eq!(a, &[2, 3, 4, 5]);
        assert_eq!(b, &[6, 7, 8, 9]);
    }

    #[test]
    fn a_put_can_end_exactly_at_the_end() {
        let (mut writer, mut reader) = log(n(16), None);
        writer.put(&[7; 16]);
        writer.commit();
        let [a, b] = reader.committed();
        assert_eq!((a.len(), b.len()), (16, 0));
        assert_eq!(writer.free(), 0);
        reader.release(16, 0);
        assert_eq!(writer.free(), 16);
    }

    #[test]
    fn room_is_limited_by_octets() {
        let (mut writer, mut reader) = log(n(16), None);
        writer.put(&[0; 10]);
        writer.commit();
        assert!(writer.has_room(6));
        assert!(!writer.has_room(7));
        reader.release(4, 0);
        assert!(writer.has_room(10));
    }

    #[test]
    fn room_is_limited_by_messages() {
        let (mut writer, mut reader) = log(n(64), Some(n(2)));
        for _ in 0..2 {
            writer.put(b"m");
            writer.end_message();
        }
        writer.commit();
        assert!(!writer.has_room(1));
        reader.release(1, 1);
        assert!(writer.has_room(1));
    }

    #[test]
    #[should_panic(expected = "put of 17 octets")]
    fn putting_more_than_is_free_panics() {
        let (mut writer, _reader) = log(n(16), None);
        writer.put(&[0; 17]);
    }

    #[test]
    #[should_panic(expected = "release past what is committed")]
    fn releasing_uncommitted_octets_panics() {
        let (mut writer, mut reader) = log(n(16), None);
        writer.put(&[0; 4]);
        reader.release(1, 0);
    }

    #[test]
    fn a_waiting_reader_is_woken_by_a_commit_and_only_then() {
        let (mut writer, mut reader) = log(n(64), None);
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(reader.poll_committed(&mut cx), Poll::Pending);
        writer.put(b"a");
        assert_eq!(wakes(&count), 0, "a put alone does not wake");
        writer.commit();
        assert_eq!(wakes(&count), 1);
        writer.put(b"b");
        writer.commit();
        assert_eq!(wakes(&count), 1, "the reader was no longer waiting");
        assert_eq!(reader.poll_committed(&mut cx), Poll::Ready(true));
    }

    #[test]
    fn a_full_writer_is_woken_once_half_the_log_drains() {
        let (mut writer, mut reader) = log(n(16), None);
        writer.put(&[0; 16]);
        writer.commit();
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(writer.poll_writable(&mut cx, 1), Poll::Pending);
        reader.release(4, 0);
        assert_eq!(wakes(&count), 0, "not yet at the low-water mark");
        reader.release(4, 0);
        assert_eq!(wakes(&count), 1);
        assert_eq!(writer.poll_writable(&mut cx, 1), Poll::Ready(Ok(())));
    }

    #[test]
    fn a_writer_wanting_more_than_half_waits_for_all_of_it() {
        let (mut writer, mut reader) = log(n(16), None);
        writer.put(&[0; 16]);
        writer.commit();
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(writer.poll_writable(&mut cx, 12), Poll::Pending);
        reader.release(8, 0);
        assert_eq!(wakes(&count), 0);
        reader.release(4, 0);
        assert_eq!(wakes(&count), 1);
        assert_eq!(writer.poll_writable(&mut cx, 12), Poll::Ready(Ok(())));
    }

    #[test]
    fn a_writer_at_the_message_hwm_is_woken_at_half_of_it() {
        let (mut writer, mut reader) = log(n(64), Some(n(4)));
        for _ in 0..4 {
            writer.put(b"m");
            writer.end_message();
        }
        writer.commit();
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(writer.poll_writable(&mut cx, 1), Poll::Pending);
        reader.release(1, 1);
        assert_eq!(wakes(&count), 0);
        reader.release(1, 1);
        assert_eq!(wakes(&count), 1);
    }

    #[test]
    fn waiting_commits_what_was_put() {
        let (mut writer, mut reader) = log(n(16), None);
        writer.put(&[1; 16]);
        let (_count, waker) = counting_waker();
        assert_eq!(
            writer.poll_writable(&mut Context::from_waker(&waker), 1),
            Poll::Pending
        );
        assert_eq!(reader.pending(), 16, "the reader can make room");
    }

    #[test]
    fn dropping_the_reader_wakes_and_closes_the_writer() {
        let (mut writer, reader) = log(n(16), None);
        writer.put(&[0; 16]);
        writer.commit();
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(writer.poll_writable(&mut cx, 1), Poll::Pending);
        drop(reader);
        assert_eq!(wakes(&count), 1);
        assert_eq!(writer.poll_writable(&mut cx, 1), Poll::Ready(Err(Closed)));
        assert_eq!(writer.poll_closed(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn dropping_the_writer_leaves_committed_octets_to_read() {
        let (mut writer, mut reader) = log(n(16), None);
        writer.put(b"last");
        writer.commit();
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(reader.poll_closed(&mut cx), Poll::Pending);
        drop(writer);
        assert_eq!(wakes(&count), 1);
        assert!(reader.is_closed());
        assert_eq!(reader.poll_committed(&mut cx), Poll::Ready(true));
        assert_eq!(read_all(&mut reader), b"last");
        reader.release(4, 0);
        assert!(reader.caught_up());
        assert_eq!(reader.poll_committed(&mut cx), Poll::Ready(false));
    }

    #[test]
    fn handles_are_send() {
        fn assert_send<T: Send>() {}
        assert_send::<LogWriter>();
        assert_send::<LogReader>();
    }

    #[test]
    fn hot_shared_fields_sit_on_separate_cache_lines() {
        const LINE: usize = 128;
        let (writer, _reader) = log(n(16), None);
        let shared = &*writer.shared;
        let lines = [
            std::ptr::from_ref(&shared.written).addr() / LINE,
            std::ptr::from_ref(&shared.released).addr() / LINE,
            std::ptr::from_ref(&shared.reader).addr() / LINE,
            std::ptr::from_ref(&shared.writer).addr() / LINE,
        ];
        for i in 0..lines.len() {
            for j in i + 1..lines.len() {
                assert_ne!(lines[i], lines[j], "fields share a cache line: {lines:?}");
            }
        }
    }

    #[test]
    fn threads_stream_octets_in_order() {
        for capacity in [16, 64, 4096] {
            let (mut writer, mut reader) = log(n(capacity), Some(n(8)));
            let total = 200_000usize;
            let consumer = std::thread::spawn(move || {
                let mut next = 0u8;
                let mut seen = 0usize;
                let waker = thread_waker();
                let mut cx = Context::from_waker(&waker);
                loop {
                    match reader.poll_committed(&mut cx) {
                        Poll::Ready(true) => {
                            let [a, b] = reader.committed();
                            let len = a.len() + b.len();
                            for &octet in a.iter().chain(b) {
                                assert_eq!(octet, next);
                                next = next.wrapping_add(1);
                            }
                            seen += len;
                            reader.release(len, len as u64);
                        }
                        Poll::Ready(false) => return seen,
                        Poll::Pending => std::thread::park(),
                    }
                }
            });
            let waker = thread_waker();
            let mut cx = Context::from_waker(&waker);
            let mut next = 0u8;
            for _ in 0..total {
                while writer.poll_writable(&mut cx, 1) == Poll::Pending {
                    std::thread::park();
                }
                writer.put(&[next]);
                writer.end_message();
                writer.commit();
                next = next.wrapping_add(1);
            }
            drop(writer);
            assert_eq!(consumer.join().unwrap(), total, "capacity {capacity}");
        }
    }

    fn thread_waker() -> Waker {
        struct Unpark(std::thread::Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        Waker::from(Arc::new(Unpark(std::thread::current())))
    }
}
