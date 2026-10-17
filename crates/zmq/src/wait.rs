//! Blocking the calling thread on a poll function.

use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

/// Polls until ready, parking the thread in between. Any pipe or listener
/// the poll registered with unparks it.
pub(crate) fn block_on<R>(mut poll: impl FnMut(&mut Context<'_>) -> Poll<R>) -> R {
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
    // once a peer's wake drops the last reference, freed on the peer's
    // thread, a cross-thread free per message that glibc handles badly.
    thread_local! {
        static WAKER: Waker = Waker::from(Arc::new(Unpark(thread::current())));
    }
    let waker = WAKER
        .try_with(Waker::clone)
        .unwrap_or_else(|_| Waker::from(Arc::new(Unpark(thread::current()))));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(output) = poll(&mut cx) {
            return output;
        }
        // Spurious wakeups are harmless: the loop polls again.
        thread::park();
    }
}
