//! The unbounded queue under a pipe, after libzmq's `yqueue_t`: items live
//! in chunks of [`CHUNK`] slots, linked as the writer fills them, and the
//! reader hands each chunk it empties back as a spare for the writer to
//! reuse, so a queue that keeps up allocates nothing.
//!
//! Each slot says whether it holds an item. A reader about to wait marks
//! the slot it is waiting on as sleeping, so the writer's exchange that
//! fills that slot also tells it to wake the reader, as `ypipe_t::flush`
//! learns from its compare-and-swap. Keeping that state beside the item,
//! rather than in one shared counter, means a message moves one cache line
//! between the cores, not two, when the reader keeps up.
//!
//! This is the crate's only unsafe code.
#![allow(unsafe_code)]

use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicU64, Ordering};

/// Slots per chunk: libzmq's `message_pipe_granularity`.
const CHUNK: usize = 256;

/// Slot states.
const EMPTY: u8 = 0;
const FULL: u8 = 1;
/// Empty, with the reader waiting for it.
const SLEEPING: u8 = 2;

struct Slot<T> {
    state: AtomicU8,
    item: UnsafeCell<MaybeUninit<T>>,
}

struct Chunk<T> {
    slots: [Slot<T>; CHUNK],
    /// Set by the writer before it fills this chunk's last slot.
    next: AtomicPtr<Chunk<T>>,
}

impl<T> Chunk<T> {
    fn allocate() -> *mut Self {
        Box::into_raw(Box::new(Self {
            slots: [const {
                Slot {
                    state: AtomicU8::new(EMPTY),
                    item: UnsafeCell::new(MaybeUninit::uninit()),
                }
            }; CHUNK],
            next: AtomicPtr::new(ptr::null_mut()),
        }))
    }

    /// # Safety
    ///
    /// `chunk` must be live.
    unsafe fn slot<'a>(chunk: *mut Self, position: u64) -> &'a Slot<T> {
        let index = (position % CHUNK as u64) as usize;
        // SAFETY: the caller's chunk is live; `index` is in bounds.
        unsafe { &(*chunk).slots[index] }
    }
}

fn is_last_slot(position: u64) -> bool {
    position % CHUNK as u64 == CHUNK as u64 - 1
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
    written: u64,
}

/// The reading end.
pub(crate) struct Consumer<T> {
    shared: Arc<Shared<T>>,
    /// The chunk the next item comes from.
    front: *mut Chunk<T>,
    read: u64,
}

impl<T> fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Producer")
            .field("written", &self.written)
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Consumer")
            .field("read", &self.read)
            .finish_non_exhaustive()
    }
}

// SAFETY: each end owns its own pointers; items cross threads only by
// value, so `T: Send` is all they need.
unsafe impl<T: Send> Send for Producer<T> {}
// SAFETY: as for `Producer`.
unsafe impl<T: Send> Send for Consumer<T> {}

pub(crate) fn queue<T>() -> (Producer<T>, Consumer<T>) {
    let first = Chunk::allocate();
    let shared = Arc::new(Shared {
        spare: AtomicPtr::new(ptr::null_mut()),
        front: AtomicPtr::new(first),
        read: AtomicU64::new(0),
        written: AtomicU64::new(0),
        _items: PhantomData,
    });
    let producer = Producer {
        shared: shared.clone(),
        back: first,
        written: 0,
    };
    let consumer = Consumer {
        shared,
        front: first,
        read: 0,
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
        let slot = unsafe { Chunk::slot(self.back, self.written) };
        // SAFETY: the slot is empty (or the reader sleeps on it) and stays
        // so until the exchange below, so the reader does not touch it.
        unsafe { (*slot.item.get()).write(item) };
        if is_last_slot(self.written) {
            let next = match self.shared.spare.swap(ptr::null_mut(), Ordering::Acquire) {
                spare if spare.is_null() => Chunk::allocate(),
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
    /// Takes the next item if the writer has added it. With `sleep`, finding
    /// none marks the reader as waiting, so the writer's next push reports
    /// that it must be woken: register the waker first.
    pub(crate) fn pop(&mut self, sleep: bool) -> Option<T> {
        // SAFETY: `front` is live: only this end frees or recycles it.
        let slot = unsafe { Chunk::slot(self.front, self.read) };
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
        let item = unsafe { (*slot.item.get()).assume_init_read() };
        // Published with the chunk, through the spare swap below.
        slot.state.store(EMPTY, Ordering::Relaxed);
        if is_last_slot(self.read) {
            let emptied = self.front;
            // SAFETY: the writer linked the next chunk before filling the
            // slot just read, and our Acquire load of its state saw that.
            self.front = unsafe { (*emptied).next.load(Ordering::Relaxed) };
            let previous = self.shared.spare.swap(emptied, Ordering::AcqRel);
            if !previous.is_null() {
                // SAFETY: a spare the writer never took is unreachable from
                // either end; it holds no items.
                drop(unsafe { Box::from_raw(previous) });
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
        let mut chunk = *self.front.get_mut();
        let written = *self.written.get_mut();
        for position in *self.read.get_mut()..written {
            // SAFETY: items between the reader's position and the writer's
            // count were written and never read. Each slot's chunk is
            // reached by following links, as the reader would.
            unsafe { (*Chunk::slot(chunk, position).item.get()).assume_init_drop() };
            if is_last_slot(position) {
                // SAFETY: as in `Consumer::pop`; no other thread is left.
                let next = unsafe { (*chunk).next.load(Ordering::Relaxed) };
                // SAFETY: its last item was just dropped, and nothing else
                // links to it.
                drop(unsafe { Box::from_raw(chunk) });
                chunk = next;
            }
        }
        // The writer's last chunk.
        if !chunk.is_null() {
            // SAFETY: unreachable elsewhere, holding no items, and linked to
            // nothing: a link is made only when filling the last slot, which
            // moves `back` past this chunk.
            drop(unsafe { Box::from_raw(chunk) });
        }
        let spare = *self.spare.get_mut();
        if !spare.is_null() {
            // SAFETY: as above.
            drop(unsafe { Box::from_raw(spare) });
        }
    }
}

#[cfg(test)]
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
        for i in 0..CHUNK - 1 {
            producer.push(i);
            assert_eq!(consumer.pop(true), Some(i));
        }
        assert_eq!(consumer.pop(true), None);
        assert!(producer.push(CHUNK - 1));
        assert_eq!(consumer.pop(true), Some(CHUNK - 1));
        assert_eq!(consumer.pop(true), None);
        assert!(producer.push(CHUNK), "and on the next chunk's first");
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
}
