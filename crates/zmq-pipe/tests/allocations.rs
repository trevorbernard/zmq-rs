//! A pipe frees what it allocated, and nothing else, however far its ends
//! got and whichever drops first: an allocator that counts what the test's
//! thread has live checks the balance, so a leaked chunk or a freed null
//! shows.
// A global allocator cannot be written without unsafe.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicIsize, Ordering};

use zmq_pipe::{Hwm, pipe};

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

thread_local! {
    /// Whether this thread's allocations count: the harness allocates on
    /// its own thread while the test runs.
    static COUNTED: Cell<bool> = const { Cell::new(false) };
}

fn counted() -> bool {
    COUNTED.try_with(Cell::get).unwrap_or(false)
}

// SAFETY: forwards to `System`, only counting.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if counted() {
            LIVE.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: as the caller promised us.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if counted() {
            LIVE.fetch_sub(1, Ordering::Relaxed);
        }
        // SAFETY: as the caller promised us.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

#[derive(Debug, Clone, Copy)]
enum FirstDropped {
    Writer,
    Reader,
}

/// Sends `sent` items, receives `received` of them, then drops the ends.
fn run(sent: u64, received: u64, first: FirstDropped) {
    let (mut writer, mut reader) = pipe(Hwm::Unlimited);
    for i in 0..sent {
        writer.try_send(i).unwrap();
    }
    for i in 0..received {
        assert_eq!(reader.try_recv(), Ok(i));
    }
    match first {
        FirstDropped::Writer => {
            drop(writer);
            drop(reader);
        }
        FirstDropped::Reader => {
            drop(reader);
            drop(writer);
        }
    }
}

#[test]
fn every_chunk_is_freed_once() {
    // Spanning the queue's 16-slot first chunk and several of its 256-slot
    // ones, read up to each boundary and past it, so the reader hands back
    // spares the writer takes or leaves.
    let cases = [
        (0, 0),
        (1, 0),
        (1, 1),
        (15, 15),
        (16, 16),
        (17, 16),
        (271, 271),
        (272, 272),
        (273, 272),
        (255, 255),
        (256, 256),
        (257, 256),
        (600, 0),
        (600, 300),
        (600, 600),
        (1000, 999),
    ];
    COUNTED.set(true);
    // Whatever the first run sets up once, such as thread-local state.
    run(1, 1, FirstDropped::Writer);
    for (sent, received) in cases {
        for first in [FirstDropped::Writer, FirstDropped::Reader] {
            let before = LIVE.load(Ordering::Relaxed);
            run(sent, received, first);
            assert_eq!(
                LIVE.load(Ordering::Relaxed),
                before,
                "{sent} sent, {received} received, {first:?} dropped first"
            );
        }
    }
}
