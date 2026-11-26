//! A file descriptor that a waker makes readable, to wake a thread waiting
//! in `poll(2)`: an eventfd on Linux, a pipe elsewhere.

use std::cell::OnceCell;
use std::io;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::Arc;
use std::task::Wake;

/// Readable from a wake until [`drain`](Self::drain).
#[derive(Debug)]
pub(crate) struct WakeFd {
    /// What a waiting thread polls. On Linux, the eventfd, written too.
    read: OwnedFd,
    /// The pipe's other end.
    #[cfg(not(target_os = "linux"))]
    write: OwnedFd,
}

impl WakeFd {
    /// The calling thread's, made on first use.
    pub(crate) fn for_thread() -> io::Result<Arc<Self>> {
        thread_local! {
            static WAKE: OnceCell<Arc<WakeFd>> = const { OnceCell::new() };
        }
        WAKE.try_with(|wake| match wake.get() {
            Some(wake) => Ok(wake.clone()),
            None => {
                let made = Arc::new(Self::new()?);
                Ok(wake.get_or_init(|| made).clone())
            }
        })
        // The thread is ending and its locals are gone: one of its own.
        .unwrap_or_else(|_| Self::new().map(Arc::new))
    }

    /// Reads what wakes have written, so that only a later wake makes it
    /// readable.
    pub(crate) fn drain(&self) {
        // Only ever fails for being empty already.
        #[cfg(target_os = "linux")]
        let _ = rustix::io::read(&self.read, &mut [0; 8]);
        #[cfg(not(target_os = "linux"))]
        while let Ok(64) = rustix::io::read(&self.read, &mut [0; 64]) {}
    }
}

#[cfg(target_os = "linux")]
impl WakeFd {
    fn new() -> io::Result<Self> {
        use rustix::event::{EventfdFlags, eventfd};

        let read = eventfd(0, EventfdFlags::CLOEXEC | EventfdFlags::NONBLOCK)?;
        Ok(Self { read })
    }

    fn signal(&self) {
        // Only ever fails for a counter that is full, and so readable.
        let _ = rustix::io::write(&self.read, &1u64.to_ne_bytes());
    }
}

#[cfg(not(target_os = "linux"))]
impl WakeFd {
    fn new() -> io::Result<Self> {
        use rustix::fs::{OFlags, fcntl_setfl};
        use rustix::io::{FdFlags, fcntl_setfd};

        // macOS has no `pipe2` to set these as the pipe is made.
        let (read, write) = rustix::pipe::pipe()?;
        for end in [&read, &write] {
            fcntl_setfd(end, FdFlags::CLOEXEC)?;
            fcntl_setfl(end, OFlags::NONBLOCK)?;
        }
        Ok(Self { read, write })
    }

    fn signal(&self) {
        // Only ever fails for a pipe that is full, and so readable.
        let _ = rustix::io::write(&self.write, &[1]);
    }
}

impl AsFd for WakeFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.read.as_fd()
    }
}

impl Wake for WakeFd {
    fn wake(self: Arc<Self>) {
        self.signal();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.signal();
    }
}
