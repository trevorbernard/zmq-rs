//! Every interleaving of a writer and a reader, with two-slot chunks so
//! that items cross chunks and chunks are recycled:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p zmq-pipe --lib --release
//! ```

use loom::sync::Notify;
use loom::sync::atomic::AtomicUsize;
use loom::thread;

use super::*;

/// Two chunks' worth and one more: the writer links a new chunk, the
/// reader hands the first back, and the writer takes it as its spare.
const ITEMS: usize = CHUNK * 2 + 1;

#[test]
fn items_arrive_in_order_across_chunks() {
    loom::model(|| {
        let (mut producer, mut consumer) = queue();
        let writer = thread::spawn(move || {
            for i in 0..ITEMS {
                producer.push(i);
            }
        });
        let mut expected = 0;
        while expected < ITEMS {
            match consumer.pop(false) {
                Some(item) => {
                    assert_eq!(item, expected);
                    expected += 1;
                }
                None => thread::yield_now(),
            }
        }
        writer.join().unwrap();
    });
}

#[test]
fn a_waiting_reader_is_always_woken() {
    loom::model(|| {
        let (mut producer, mut consumer) = queue();
        let woken = Arc::new(Notify::new());
        let wake = woken.clone();
        let writer = thread::spawn(move || {
            for i in 0..ITEMS {
                if producer.push(i) {
                    wake.notify();
                }
            }
        });
        let mut expected = 0;
        while expected < ITEMS {
            match consumer.pop(true) {
                Some(item) => {
                    assert_eq!(item, expected);
                    expected += 1;
                }
                // A lost wakeup waits here for good: loom reports a deadlock.
                None => woken.wait(),
            }
        }
        writer.join().unwrap();
    });
}

struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn ends_dropped_on_their_own_threads_drop_every_item_once() {
    loom::model(|| {
        let dropped = Arc::new(AtomicUsize::new(0));
        let (mut producer, mut consumer) = queue();
        let count = dropped.clone();
        let writer = thread::spawn(move || {
            for _ in 0..ITEMS {
                producer.push(Counted(count.clone()));
            }
        });
        let reader = thread::spawn(move || {
            for _ in 0..CHUNK {
                drop(consumer.pop(false));
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();
        assert_eq!(dropped.load(Ordering::Relaxed), ITEMS);
    });
}
