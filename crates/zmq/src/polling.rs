//! Waiting on several sockets at once, as libzmq's `zmq_poll`.

use std::fmt;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::wait;

/// A socket to [`poll`], and what to wait for. Made by
/// [`Socket::readable`](crate::Socket::readable) and
/// [`Socket::writable`](crate::Socket::writable); it borrows the socket
/// only until the poll returns.
pub struct PollItem<'a> {
    target: Target<'a>,
}

enum Target<'a> {
    Readable(&'a mut dyn PollReadable),
    Writable(&'a mut dyn PollWritable),
    /// Left out by [`PollItem::when`]: never ready.
    Skipped,
}

/// A socket that can be polled until a receive would not wait.
pub(crate) trait PollReadable {
    /// Ready once a receive would return at once; registers `cx` if not.
    fn poll_readable(&mut self, cx: &mut Context<'_>) -> Poll<()>;
}

/// A socket that can be polled until a send would not wait.
pub(crate) trait PollWritable {
    /// Ready once a send would succeed at once; registers `cx` if not.
    fn poll_writable(&mut self, cx: &mut Context<'_>) -> Poll<()>;
}

impl<'a> PollItem<'a> {
    /// This item if `condition` holds, and otherwise one that is never
    /// ready, so that a loop can wait on a socket only while it is wanted:
    ///
    /// ```
    /// # use zmq::{Context, Dealer, Router};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let context = Context::new()?;
    /// # let mut frontend = context.socket::<Router>();
    /// # let mut backend = context.socket::<Router>();
    /// # let mut idle = context.socket::<Dealer>();
    /// # frontend.bind("inproc://frontend")?;
    /// # idle.connect("inproc://frontend")?;
    /// # idle.send("request");
    /// let workers_free = false;
    /// // Take requests only while a worker is free to handle them.
    /// let ready = zmq::poll_timeout(
    ///     [backend.readable(), frontend.readable().when(workers_free)],
    ///     std::time::Duration::from_millis(10),
    /// );
    /// assert_eq!(ready, None);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn when(self, condition: bool) -> Self {
        if condition {
            self
        } else {
            Self {
                target: Target::Skipped,
            }
        }
    }

    pub(crate) fn readable(socket: &'a mut dyn PollReadable) -> Self {
        Self {
            target: Target::Readable(socket),
        }
    }

    pub(crate) fn writable(socket: &'a mut dyn PollWritable) -> Self {
        Self {
            target: Target::Writable(socket),
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match &mut self.target {
            Target::Readable(socket) => socket.poll_readable(cx),
            Target::Writable(socket) => socket.poll_writable(cx),
            Target::Skipped => Poll::Pending,
        }
    }
}

impl fmt::Debug for PollItem<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let interest = match self.target {
            Target::Readable(_) => "readable",
            Target::Writable(_) => "writable",
            Target::Skipped => "skipped",
        };
        f.debug_tuple("PollItem").field(&interest).finish()
    }
}

/// Waits until at least one of `items` is ready, as libzmq's `zmq_poll`
/// without a timeout, and returns which are, in order. Every item is
/// looked at each time, so a busy socket never hides another.
///
/// A socket reported readable returns a message from its next
/// `try_recv`; one reported writable takes the next `try_send`, unless the
/// peer with room leaves first.
///
/// ```
/// # use zmq::{Context, Pull, Push};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let context = Context::new()?;
/// let mut jobs = context.socket::<Pull>();
/// let mut control = context.socket::<Pull>();
/// jobs.bind("inproc://jobs")?;
/// control.bind("inproc://control")?;
/// let mut boss = context.socket::<Push>();
/// boss.connect("inproc://control")?;
/// boss.send("stop");
///
/// loop {
///     let [job, stop] = zmq::poll([jobs.readable(), control.readable()]);
///     if stop {
///         break;
///     }
///     if job {
///         let _job = jobs.try_recv().expect("readable");
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Polling no sockets would wait forever, so it does not compile:
///
/// ```compile_fail
/// zmq::poll([]);
/// ```
pub fn poll<const N: usize>(items: [PollItem<'_>; N]) -> [bool; N] {
    const { assert!(N > 0, "polling no sockets would wait forever") };
    wait_until(items, None).expect("no deadline")
}

/// Waits at most `timeout` for at least one of `items` to be ready, as
/// [`poll`] does. `None` if none is ready in time.
pub fn poll_timeout<const N: usize>(
    items: [PollItem<'_>; N],
    timeout: Duration,
) -> Option<[bool; N]> {
    wait_until(items, wait::deadline(timeout))
}

fn wait_until<const N: usize>(
    mut items: [PollItem<'_>; N],
    deadline: Option<Instant>,
) -> Option<[bool; N]> {
    wait::block_on_until(deadline, |cx| {
        // Each item is polled, ready or not, so that each registers `cx`.
        let ready = items.each_mut().map(|item| item.poll(cx).is_ready());
        if ready.contains(&true) {
            Poll::Ready(ready)
        } else {
            Poll::Pending
        }
    })
}
