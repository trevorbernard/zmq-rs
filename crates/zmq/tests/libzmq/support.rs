//! What the ported tests share, as libzmq's `testutil.hpp`.

#![allow(dead_code)]

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// How long a test waits for something that should happen.
pub(crate) const FIVE: Duration = Duration::from_secs(5);

/// How long a test waits to see that something does not happen, as
/// libzmq's `SETTLE_TIME`.
pub(crate) const SETTLE: Duration = Duration::from_millis(300);

/// Runs `f` on another thread and fails the test if it takes longer than
/// `limit`, so a lost wakeup fails instead of hanging the suite.
pub(crate) fn within<T: Send + 'static>(
    limit: Duration,
    f: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
        .recv_timeout(limit)
        .unwrap_or_else(|_| panic!("timed out after {limit:?}"))
}

/// Waits briefly for messages in flight to arrive, as libzmq's
/// `msleep (SETTLE_TIME)`.
pub(crate) fn settle() {
    thread::sleep(SETTLE);
}
