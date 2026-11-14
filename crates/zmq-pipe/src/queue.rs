//! The unbounded queue under a pipe, after libzmq's `yqueue_t`: items live
//! in chunks of [`CHUNK`] slots, after a first of [`FIRST`], linked as the
//! writer fills them, and the reader hands each chunk it empties back as a
//! spare for the writer to reuse, so a queue that keeps up allocates
//! nothing.
//!
//! Each slot says whether it holds an item. A reader about to wait marks
//! the slot it is waiting on as sleeping, so the writer's exchange that
//! fills that slot also tells it to wake the reader, as `ypipe_t::flush`
//! learns from its compare-and-swap. Keeping that state beside the item,
//! rather than in one shared counter, means a message moves one cache line
//! between the cores, not two, when the reader keeps up.
//!
//! This is the crate's only unsafe code. Built with `--cfg loom`, it runs
//! on [loom](https://docs.rs/loom)'s atomics, which check every
//! interleaving of the models in `loom_tests`.
#![allow(unsafe_code)]

use std::alloc::{self, Layout};
use std::fmt;
use std::marker::PhantomData;
use std::mem::{self, MaybeUninit};
use std::ptr;

use sync::atomic::{AtomicPtr, AtomicU8, AtomicU64, Ordering};
use sync::{Arc, UnsafeCell};

#[cfg(not(loom))]
mod sync {
    pub(super) use std::sync::{Arc, atomic};

    /// `std`'s, with loom's interface.
    #[derive(Debug)]
    pub(super) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

    impl<T> UnsafeCell<T> {
        pub(super) const fn new(value: T) -> Self {
            Self(std::cell::UnsafeCell::new(value))
        }

        #[inline]
        pub(super) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
            f(self.0.get())
        }

        #[inline]
        pub(super) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}

#[cfg(loom)]
mod sync {
    pub(super) use loom::cell::UnsafeCell;
    pub(super) use loom::sync::{Arc, atomic};
}

/// Slots per chunk: libzmq's `message_pipe_granularity`.
#[cfg(not(loom))]
const CHUNK: usize = 256;
/// Two, so that a model crosses chunks and recycles them in a few steps.
#[cfg(loom)]
const CHUNK: usize = 2;

/// Slots in a queue's first chunk, so that a pipe that carries little,
/// as most of a socket's do with many peers, holds little: a full chunk
/// of messages is 22 KiB.
#[cfg(not(loom))]
const FIRST: usize = 16;
#[cfg(loom)]
const FIRST: usize = 1;

/// The first position. The first chunk holds the last [`FIRST`] positions
/// of a full one, so every chunk ends where `position % CHUNK` does.
const START: u64 = (CHUNK - FIRST) as u64;

/// Slot states.
const EMPTY: u8 = 0;
const FULL: u8 = 1;
/// Empty, with the reader waiting for it.
const SLEEPING: u8 = 2;

struct Slot<T> {
    state: AtomicU8,
    item: UnsafeCell<MaybeUninit<T>>,
}

/// A chunk's header. Its slots follow it in the same allocation: [`CHUNK`]
/// of them, or [`FIRST`] in a queue's first chunk.
#[repr(C)]
struct Chunk<T> {
    /// Set by the writer before it fills this chunk's last slot.
    next: AtomicPtr<Chunk<T>>,
    slots: [Slot<T>; 0],
}

impl<T> Chunk<T> {
    fn layout(slots: usize) -> Layout {
        let slots = Layout::array::<Slot<T>>(slots).expect("a chunk's size fits");
        let (layout, offset) = Layout::new::<Self>()
            .extend(slots)
            .expect("a chunk's size fits");
        debug_assert_eq!(offset, mem::offset_of!(Self, slots));
        layout
    }

    fn allocate(slots: usize) -> *mut Self {
        let layout = Self::layout(slots);
        // SAFETY: the layout holds the header, so its size is not zero.
        let chunk = unsafe { alloc::alloc(layout) }.cast::<Self>();
        if chunk.is_null() {
            alloc::handle_alloc_error(layout);
        }
        // SAFETY: the allocation is ours, with room for the header and
        // `slots` slots after it.
        unsafe {
            (&raw mut (*chunk).next).write(AtomicPtr::new(ptr::null_mut()));
            let first = Self::first_slot(chunk);
            for index in 0..slots {
                first.add(index).write(Slot {
                    state: AtomicU8::new(EMPTY),
                    item: UnsafeCell::new(MaybeUninit::uninit()),
                });
            }
        }
        chunk
    }

    /// # Safety
    ///
    /// `chunk` must be live, holding no items, and have been allocated with
    /// `slots` slots.
    unsafe fn free(chunk: *mut Self, slots: usize) {
        // SAFETY: the caller's; the header and the slots need no dropping
        // (loom's do, to stop tracking them).
        unsafe {
            #[cfg(loom)]
            {
                let first = Self::first_slot(chunk);
                for index in 0..slots {
                    first.add(index).drop_in_place();
                }
                (&raw mut (*chunk).next).drop_in_place();
            }
            alloc::dealloc(chunk.cast(), Self::layout(slots));
        }
    }

    /// # Safety
    ///
    /// `chunk` must be live.
    unsafe fn first_slot(chunk: *mut Self) -> *mut Slot<T> {
        // SAFETY: the slots follow the header in the caller's allocation.
        unsafe { chunk.byte_add(mem::offset_of!(Self, slots)) }.cast()
    }

    /// Where the chunk's slot for the position `p` is, less `p % CHUNK`
    /// slots. A chunk of `slots` slots holds the last `slots` positions of
    /// a full one, so for a first chunk this is before its allocation.
    ///
    /// # Safety
    ///
    /// `chunk` must be live.
    unsafe fn slots(chunk: *mut Self, slots: usize) -> *mut Slot<T> {
        // SAFETY: the caller's.
        unsafe { Self::first_slot(chunk) }.wrapping_sub(CHUNK - slots)
    }
}

/// # Safety
///
/// `slots` must be a live chunk's, from [`Chunk::slots`], and `position`
/// one of the positions the chunk holds.
unsafe fn slot<'a, T>(slots: *mut Slot<T>, position: u64) -> &'a Slot<T> {
    let index = (position % CHUNK as u64) as usize;
    // SAFETY: the caller's position is in the chunk, so this is one of its
    // slots, initialized when it was allocated.
    unsafe { &*slots.wrapping_add(index) }
}

fn is_last_slot(position: u64) -> bool {
    position % CHUNK as u64 == CHUNK as u64 - 1
}

/// How many slots the chunk holding `position` has: positions start at
/// [`START`], so that only the first chunk's are below [`CHUNK`].
fn slots_at(position: u64) -> usize {
    if position < CHUNK as u64 {
        FIRST
    } else {
        CHUNK
    }
}

struct Shared<T> {
    /// An emptied chunk for the writer to reuse, or null.
    spare: AtomicPtr<Chunk<T>>,
    /// Where each end stood when it dropped, for freeing what is left.
    /// Written once by each end; the last `Arc` drop orders them.
    front: AtomicPtr<Chunk<T>>,
    read: AtomicU64,
    written: AtomicU64,
    _items: PhantomData<T>,
}

/// The writing end.
pub(crate) struct Producer<T> {
    shared: Arc<Shared<T>>,
    /// The chunk the next item goes into.
    back: *mut Chunk<T>,
    /// Its slots, from [`Chunk::slots`].
    slots: *mut Slot<T>,
    written: u64,
}

/// The reading end.
pub(crate) struct Consumer<T> {
    shared: Arc<Shared<T>>,
    /// The chunk the next item comes from.
    front: *mut Chunk<T>,
    /// Its slots, from [`Chunk::slots`].
    slots: *mut Slot<T>,
    read: u64,
}

impl<T> fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Producer")
            .field("written", &(self.written - START))
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Consumer")
            .field("read", &(self.read - START))
            .finish_non_exhaustive()
    }
}

// SAFETY: each end owns its own pointers; items cross threads only by
// value, so `T: Send` is all they need.
unsafe impl<T: Send> Send for Producer<T> {}
// SAFETY: as for `Producer`.
unsafe impl<T: Send> Send for Consumer<T> {}

pub(crate) fn queue<T>() -> (Producer<T>, Consumer<T>) {
    let first = Chunk::allocate(FIRST);
    // SAFETY: just allocated, with `FIRST` slots.
    let slots = unsafe { Chunk::slots(first, FIRST) };
    let shared = Arc::new(Shared {
        spare: AtomicPtr::new(ptr::null_mut()),
        front: AtomicPtr::new(first),
        read: AtomicU64::new(START),
        written: AtomicU64::new(START),
        _items: PhantomData,
    });
    let producer = Producer {
        shared: shared.clone(),
        back: first,
        slots,
        written: START,
    };
    let consumer = Consumer {
        shared,
        front: first,
        slots,
        read: START,
    };
    (producer, consumer)
}

impl<T> Producer<T> {
    /// Appends `item`. Returns whether the reader is waiting for it and
    /// must be woken.
    pub(crate) fn push(&mut self, item: T) -> bool {
        // SAFETY: `back` is live: the reader frees nothing, and recycles a
        // chunk only once it has moved past it, which needs this end to
        // have linked the next.
        let slot = unsafe { slot(self.slots, self.written) };
        // SAFETY: the slot is empty (or the reader sleeps on it) and stays
        // so until the exchange below, so the reader does not touch it.
        slot.item.with_mut(|cell| unsafe { (*cell).write(item) });
        if is_last_slot(self.written) {
            let next = match self.shared.spare.swap(ptr::null_mut(), Ordering::Acquire) {
                spare if spare.is_null() => Chunk::allocate(CHUNK),
                spare => {
                    // SAFETY: the reader gave up the spare in its swap; it
                    // is ours now. A stale `next` would mislead the final
                    // drop's walk.
                    unsafe { (*spare).next.store(ptr::null_mut(), Ordering::Relaxed) };
                    spare
                }
            };
            // SAFETY: as above. The exchange below publishes the link.
            unsafe { (*self.back).next.store(next, Ordering::Relaxed) };
            self.back = next;
            // SAFETY: a spare or a new chunk, of `CHUNK` slots either way.
            self.slots = unsafe { Chunk::slots(next, CHUNK) };
        }
        self.written += 1;
        // Release publishes the item and any new link; Acquire pairs with a
        // reader's mark, which follows its waker registration.
        slot.state.swap(FULL, Ordering::AcqRel) == SLEEPING
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.shared.written.store(self.written, Ordering::Relaxed);
    }
}

impl<T> Consumer<T> {
    /// Whether the writer has added nothing past what was taken.
    pub(crate) fn is_empty(&self) -> bool {
        // SAFETY: `front` is live: only this end frees or recycles it.
        let slot = unsafe { slot(self.slots, self.read) };
        slot.state.load(Ordering::Acquire) != FULL
    }

    /// Takes the next item if the writer has added it. With `sleep`, finding
    /// none marks the reader as waiting, so the writer's next push reports
    /// that it must be woken: register the waker first.
    pub(crate) fn pop(&mut self, sleep: bool) -> Option<T> {
        // SAFETY: `front` is live: only this end frees or recycles it.
        let slot = unsafe { slot(self.slots, self.read) };
        match slot.state.load(Ordering::Acquire) {
            FULL => {}
            EMPTY if sleep => {
                if slot
                    .state
                    .compare_exchange(EMPTY, SLEEPING, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return None;
                }
                // Filled in between: the failed exchange acquired it.
            }
            // Still SLEEPING from an earlier wait, or not asked to sleep.
            _ => return None,
        }
        // SAFETY: the writer filled the slot and will not touch it again
        // until it is reset and recycled.
        let item = slot.item.with(|cell| unsafe { (*cell).assume_init_read() });
        // Published with the chunk, through the spare swap below.
        slot.state.store(EMPTY, Ordering::Relaxed);
        if is_last_slot(self.read) {
            let emptied = self.front;
            // SAFETY: the writer linked the next chunk before filling the
            // slot just read, and our Acquire load of its state saw that.
            self.front = unsafe { (*emptied).next.load(Ordering::Relaxed) };
            // SAFETY: the writer only links chunks of `CHUNK` slots.
            self.slots = unsafe { Chunk::slots(self.front, CHUNK) };
            // The first chunk is too small to be a spare.
            let unused = match slots_at(self.read) {
                CHUNK => self.shared.spare.swap(emptied, Ordering::AcqRel),
                _ => emptied,
            };
            if !unused.is_null() {
                // SAFETY: the emptied first chunk, which the writer left
                // when it linked the next, or a spare the writer never
                // took: unreachable from either end, holding no items. A
                // spare is never a first chunk.
                unsafe { Chunk::free(unused, slots_at(self.read)) };
            }
        }
        self.read += 1;
        Some(item)
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.shared.front.store(self.front, Ordering::Relaxed);
        self.shared.read.store(self.read, Ordering::Relaxed);
    }
}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        // Both ends are gone; the `Arc`'s release and acquire order their
        // final stores before this.
        let mut chunk = self.front.load(Ordering::Relaxed);
        let written = self.written.load(Ordering::Relaxed);
        for position in self.read.load(Ordering::Relaxed)..written {
            // SAFETY: items between the reader's position and the writer's
            // count were written and never read. Each slot's chunk is
            // reached by following links, as the reader would.
            let slot = unsafe { slot(Chunk::slots(chunk, slots_at(position)), position) };
            // SAFETY: as above.
            slot.item
                .with_mut(|cell| unsafe { (*cell).assume_init_drop() });
            if is_last_slot(position) {
                // SAFETY: as in `Consumer::pop`; no other thread is left.
                let next = unsafe { (*chunk).next.load(Ordering::Relaxed) };
                // SAFETY: its last item was just dropped, and nothing else
                // links to it.
                unsafe { Chunk::free(chunk, slots_at(position)) };
                chunk = next;
            }
        }
        // The writer's last chunk.
        if !chunk.is_null() {
            // SAFETY: unreachable elsewhere, holding no items, and linked to
            // nothing: a link is made only when filling the last slot, which
            // moves `back` past this chunk. It is the first chunk if the
            // writer never filled it.
            unsafe { Chunk::free(chunk, slots_at(written)) };
        }
        let spare = self.spare.load(Ordering::Relaxed);
        if !spare.is_null() {
            // SAFETY: as above; a spare is never a first chunk.
            unsafe { Chunk::free(spare, CHUNK) };
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    #[test]
    fn items_cross_chunks_in_order() {
        let (mut producer, mut consumer) = queue();
        for round in 0..3 {
            for i in 0..CHUNK * 3 + 7 {
                assert!(!producer.push((round, i)));
            }
            for i in 0..CHUNK * 3 + 7 {
                assert_eq!(consumer.pop(false), Some((round, i)));
            }
            assert_eq!(consumer.pop(false), None);
        }
    }

    #[test]
    fn the_writer_moves_on_only_after_a_chunks_last_slot() {
        let (mut producer, _consumer) = queue();
        for slots in [FIRST, CHUNK, CHUNK] {
            let chunk = producer.back;
            for i in 0..slots - 1 {
                producer.push(i);
                assert_eq!(producer.back, chunk, "after {i} of {slots}");
            }
            producer.push(slots - 1);
            assert_ne!(producer.back, chunk, "a chunk of {slots}");
        }
    }

    #[test]
    fn a_waiting_reader_is_reported_once() {
        let (mut producer, mut consumer) = queue();
        assert_eq!(consumer.pop(true), None);
        assert!(producer.push(1), "the reader is waiting");
        assert!(!producer.push(2), "and was reported already");
        assert_eq!(consumer.pop(true), Some(1));
        assert_eq!(consumer.pop(true), Some(2));
        assert_eq!(consumer.pop(true), None);
        assert_eq!(consumer.pop(true), None, "waiting twice is harmless");
        assert_eq!(consumer.pop(false), None);
        assert!(producer.push(3));
    }

    #[test]
    fn a_reader_waiting_on_a_chunks_last_slot_is_reported() {
        let (mut producer, mut consumer) = queue();
        for i in 0..FIRST - 1 {
            producer.push(i);
            assert_eq!(consumer.pop(true), Some(i));
        }
        assert_eq!(consumer.pop(true), None);
        assert!(producer.push(FIRST - 1));
        assert_eq!(consumer.pop(true), Some(FIRST - 1));
        assert_eq!(consumer.pop(true), None);
        assert!(producer.push(FIRST), "and on the next chunk's first");
    }

    #[test]
    fn looking_without_sleeping_leaves_the_writer_quiet() {
        let (mut producer, mut consumer) = queue();
        assert_eq!(consumer.pop(false), None);
        assert!(!producer.push(1));
    }

    #[test]
    fn a_queue_that_keeps_up_reuses_one_spare() {
        let (mut producer, mut consumer) = queue();
        // The first chunk is too small to be one.
        for i in 0..FIRST {
            producer.push(i);
            assert_eq!(consumer.pop(true), Some(i));
        }
        let mut chunks = HashSet::new();
        for i in 0..CHUNK * 20 {
            producer.push(i);
            chunks.insert(producer.back.addr());
            assert_eq!(consumer.pop(true), Some(i));
        }
        assert_eq!(chunks.len(), 2, "the writer alternates two chunks");
    }

    struct Counted<'a>(&'a AtomicUsize);

    impl Drop for Counted<'_> {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn unread_items_are_dropped_with_the_queue() {
        let cases = [
            (0, 0),
            (5, 2),
            (FIRST - 1, 0),
            (FIRST, 0),
            (FIRST, FIRST),
            (FIRST + 1, FIRST),
            (CHUNK - 1, 0),
            (CHUNK, 0),
            (CHUNK, CHUNK),
            (CHUNK * 2 + 3, CHUNK + 1),
        ];
        for (pushed, popped) in cases {
            let dropped = AtomicUsize::new(0);
            let (mut producer, mut consumer) = queue();
            for _ in 0..pushed {
                producer.push(Counted(&dropped));
            }
            for _ in 0..popped {
                drop(consumer.pop(false));
            }
            assert_eq!(dropped.load(Ordering::Relaxed), popped);
            drop(consumer);
            drop(producer);
            assert_eq!(dropped.load(Ordering::Relaxed), pushed, "{pushed}/{popped}");
        }
    }

    #[test]
    fn the_writer_outliving_the_reader_drops_everything() {
        let dropped = AtomicUsize::new(0);
        let (mut producer, consumer) = queue();
        producer.push(Counted(&dropped));
        drop(consumer);
        for _ in 0..CHUNK * 2 {
            producer.push(Counted(&dropped));
        }
        drop(producer);
        assert_eq!(dropped.load(Ordering::Relaxed), CHUNK * 2 + 1);
    }

    #[test]
    fn threads_see_every_item_in_order() {
        const ITEMS: u64 = if cfg!(miri) { 1_000 } else { 2_000_000 };
        let (mut producer, mut consumer) = queue::<Box<u64>>();
        let reader = thread::spawn(move || {
            let mut expected = 0;
            while expected < ITEMS {
                match consumer.pop(expected % 3 == 0) {
                    Some(item) => {
                        assert_eq!(*item, expected);
                        expected += 1;
                    }
                    None => thread::yield_now(),
                }
            }
        });
        for i in 0..ITEMS {
            producer.push(Box::new(i));
        }
        reader.join().unwrap();
    }

    #[test]
    fn the_ends_print_their_progress_and_not_their_items() {
        let (mut producer, mut consumer) = queue();
        producer.push("item");
        producer.push("item");
        consumer.pop(false);
        assert_eq!(format!("{producer:?}"), "Producer { written: 2, .. }");
        assert_eq!(format!("{consumer:?}"), "Consumer { read: 1, .. }");
    }
}

#[cfg(all(test, loom))]
mod loom_tests;
