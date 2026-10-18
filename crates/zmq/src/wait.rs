//! Blocking the calling thread on a poll function.

use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

/// Polls until ready, parking the thread in between. Any pipe or listener
/// the poll registered with unparks it.
pub(crate) fn block_on<R>(poll: impl FnMut(&mut Context<'_>) -> Poll<R>) -> R {
    block_on_until(None, poll).expect("no deadline")
}

/// The deadline `timeout` from now, or `None` (never) if that is too far
/// off for an `Instant`.
pub(crate) fn deadline(timeout: Duration) -> Option<Instant> {
    Instant::now().checked_add(timeout)
}

/// Polls until ready or until `deadline`, parking the thread in between.
/// `None` once a poll at or after the deadline is still pending, so a
/// deadline already passed polls exactly once; a `None` deadline waits
/// forever.
pub(crate) fn block_on_until<R>(
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    const SHORT: Duration = Duration::from_millis(100);

    #[test]
    fn spurious_unparks_neither_end_the_wait_early_nor_extend_it() {
        let me = thread::current();
        let done = Arc::new(AtomicBool::new(false));
        let nagger = thread::spawn({
            let done = done.clone();
            move || {
                while !done.load(Ordering::Relaxed) {
                    me.unpark();
                    thread::sleep(Duration::from_millis(2));
                }
            }
        });
        let start = Instant::now();
        let polled = block_on_until(deadline(SHORT), |_| Poll::<()>::Pending);
        let waited = start.elapsed();
        done.store(true, Ordering::Relaxed);
        nagger.join().unwrap();
        assert_eq!(polled, None);
        assert!(waited >= SHORT, "gave up after {waited:?}");
        assert!(waited < SHORT * 10, "gave up only after {waited:?}");
    }

    #[test]
    fn a_wake_before_the_deadline_is_seen() {
        let ready = Arc::new(AtomicBool::new(false));
        let registered: Arc<Mutex<Option<Waker>>> = Arc::default();
        let waker = thread::spawn({
            let (ready, registered) = (ready.clone(), registered.clone());
            move || {
                thread::sleep(SHORT);
                ready.store(true, Ordering::SeqCst);
                if let Some(waker) = registered.lock().unwrap().take() {
                    waker.wake();
                }
            }
        });
        let start = Instant::now();
        let polled = block_on_until(deadline(Duration::from_secs(60)), |cx| {
            *registered.lock().unwrap() = Some(cx.waker().clone());
            if ready.load(Ordering::SeqCst) {
                Poll::Ready(42)
            } else {
                Poll::Pending
            }
        });
        assert_eq!(polled, Some(42));
        assert!(start.elapsed() < Duration::from_secs(30));
        waker.join().unwrap();
    }

    #[test]
    fn a_passed_deadline_polls_exactly_once() {
        let mut polls = 0;
        let polled = block_on_until(Some(Instant::now()), |_| {
            polls += 1;
            Poll::<()>::Pending
        });
        assert_eq!((polled, polls), (None, 1));
    }

    #[test]
    fn a_deadline_too_far_off_is_never() {
        assert_eq!(deadline(Duration::MAX), None);
        assert!(deadline(SHORT).is_some());
    }
}
