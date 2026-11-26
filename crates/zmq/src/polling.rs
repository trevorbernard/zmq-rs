//! Waiting on several sockets at once, as libzmq's `zmq_poll` and
//! `zmq_poller_wait_all`.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::socket::noop;
use crate::socket::sealed::{Sealed, Terminates};
use crate::{Terminated, wait};

/// A socket to [`poll`], and what to wait for. Made by
/// [`Socket::readable`](crate::Socket::readable),
/// [`Socket::writable`](crate::Socket::writable) and
/// [`Socket::readable_and_writable`](crate::Socket::readable_and_writable);
/// it borrows the socket only until the poll returns.
pub struct PollItem<'a> {
    target: Target<'a>,
}

enum Target<'a> {
    Readable(&'a mut dyn PollReadable),
    Writable(&'a mut dyn PollWritable),
    /// One of the two items of a socket polled both ways, which share it.
    SharedReadable(Shared<'a>),
    SharedWritable(Shared<'a>),
    /// Left out by [`PollItem::when`]: never ready.
    Skipped,
}

type Shared<'a> = Rc<RefCell<&'a mut dyn PollBoth>>;

/// A socket that can be polled until a receive would not wait.
pub(crate) trait PollReadable: Terminates {
    /// Ready once a receive would return at once; registers `cx` if not.
    fn poll_readable(&mut self, cx: &mut Context<'_>) -> Poll<()>;
}

/// A socket that can be polled until a send would not wait.
pub(crate) trait PollWritable: Terminates {
    /// Ready once a send would succeed at once; registers `cx` if not.
    fn poll_writable(&mut self, cx: &mut Context<'_>) -> Poll<()>;
}

/// A socket that can be polled both ways.
pub(crate) trait PollBoth: PollReadable + PollWritable {}

impl<T: PollReadable + PollWritable> PollBoth for T {}

impl<'a> PollItem<'a> {
    /// This item if `condition` holds, and otherwise one that is never
    /// ready, so that a loop can wait on a socket only while it is wanted:
    ///
    /// ```
    /// # use zmq::{Context, Dealer, Router};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let context = Context::new()?;
    /// # let mut frontend = context.socket::<Router>()?;
    /// # let mut backend = context.socket::<Router>()?;
    /// # let mut idle = context.socket::<Dealer>()?;
    /// # frontend.bind("inproc://frontend")?;
    /// # idle.connect("inproc://frontend")?;
    /// # idle.send("request")?;
    /// let workers_free = false;
    /// // Take requests only while a worker is free to handle them.
    /// let ready = zmq::poll_timeout(
    ///     [backend.readable(), frontend.readable().when(workers_free)],
    ///     std::time::Duration::from_millis(10),
    /// );
    /// assert_eq!(ready, Ok(None));
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

    /// Two items for one socket: when it is readable, and when writable.
    pub(crate) fn both(socket: &'a mut dyn PollBoth) -> (Self, Self) {
        let shared = Rc::new(RefCell::new(socket));
        (
            Self {
                target: Target::SharedReadable(shared.clone()),
            },
            Self {
                target: Target::SharedWritable(shared),
            },
        )
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match &mut self.target {
            Target::Readable(socket) => socket.poll_readable(cx),
            Target::Writable(socket) => socket.poll_writable(cx),
            Target::SharedReadable(socket) => socket.borrow_mut().poll_readable(cx),
            Target::SharedWritable(socket) => socket.borrow_mut().poll_writable(cx),
            Target::Skipped => Poll::Pending,
        }
    }
}

impl PollItem<'_> {
    /// Whether the socket's context is shut down; if not, a shutdown wakes
    /// `cx`.
    fn poll_terminated(&self, cx: &mut Context<'_>) -> bool {
        match &self.target {
            Target::Readable(socket) => socket.poll_terminated(cx),
            Target::Writable(socket) => socket.poll_terminated(cx),
            Target::SharedReadable(socket) | Target::SharedWritable(socket) => {
                Terminates::poll_terminated(&**socket.borrow(), cx)
            }
            Target::Skipped => false,
        }
    }
}

impl fmt::Debug for PollItem<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let interest = match self.target {
            Target::Readable(_) | Target::SharedReadable(_) => "readable",
            Target::Writable(_) | Target::SharedWritable(_) => "writable",
            Target::Skipped => "skipped",
        };
        f.debug_tuple("PollItem").field(&interest).finish()
    }
}

/// The sockets given to [`poll`] and [`poll_timeout`]: an array of
/// [`PollItem`]s when how many there are is known as the program is
/// written, a `Vec` of them when it is not. Cannot be implemented outside
/// this crate.
pub trait PollItems: Sealed {
    /// Which of the items are ready, in their order: `[bool; N]` for an
    /// array, `Vec<bool>` for a `Vec`.
    type Ready;

    /// Refuses a set that [`poll`] would wait on forever.
    #[doc(hidden)]
    #[track_caller]
    fn assert_some(&self);

    /// No item ready.
    #[doc(hidden)]
    fn none_ready(&self) -> Self::Ready;

    /// Polls every item into `ready`, registering `cx` with each that is
    /// not; whether any is ready.
    #[doc(hidden)]
    fn poll_each(&mut self, ready: &mut Self::Ready, cx: &mut Context<'_>) -> bool;

    /// Whether an item's context is shut down; if none is, a shutdown
    /// wakes `cx`.
    #[doc(hidden)]
    fn poll_terminated(&self, cx: &mut Context<'_>) -> bool;
}

impl<const N: usize> Sealed for [PollItem<'_>; N] {}

impl<const N: usize> PollItems for [PollItem<'_>; N] {
    type Ready = [bool; N];

    fn assert_some(&self) {
        const { assert!(N > 0, "polling no sockets would wait forever") };
    }

    fn none_ready(&self) -> [bool; N] {
        [false; N]
    }

    fn poll_each(&mut self, ready: &mut [bool; N], cx: &mut Context<'_>) -> bool {
        poll_each(self, ready, cx)
    }

    fn poll_terminated(&self, cx: &mut Context<'_>) -> bool {
        any_terminated(self, cx)
    }
}

impl Sealed for Vec<PollItem<'_>> {}

impl PollItems for Vec<PollItem<'_>> {
    type Ready = Vec<bool>;

    fn assert_some(&self) {
        assert!(!self.is_empty(), "polling no sockets would wait forever");
    }

    fn none_ready(&self) -> Vec<bool> {
        vec![false; self.len()]
    }

    fn poll_each(&mut self, ready: &mut Vec<bool>, cx: &mut Context<'_>) -> bool {
        poll_each(self, ready, cx)
    }

    fn poll_terminated(&self, cx: &mut Context<'_>) -> bool {
        any_terminated(self, cx)
    }
}

/// Each item is polled, ready or not, so that each registers `cx`.
fn poll_each(items: &mut [PollItem<'_>], ready: &mut [bool], cx: &mut Context<'_>) -> bool {
    let mut any = false;
    for (item, ready) in items.iter_mut().zip(ready) {
        *ready = item.poll(cx).is_ready();
        any |= *ready;
    }
    any
}

fn any_terminated(items: &[PollItem<'_>], cx: &mut Context<'_>) -> bool {
    items.iter().any(|item| item.poll_terminated(cx))
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
/// let mut jobs = context.socket::<Pull>()?;
/// let mut control = context.socket::<Pull>()?;
/// jobs.bind("inproc://jobs")?;
/// control.bind("inproc://control")?;
/// let mut boss = context.socket::<Push>()?;
/// boss.connect("inproc://control")?;
/// boss.send("stop")?;
///
/// loop {
///     let [job, stop] = zmq::poll([jobs.readable(), control.readable()])?;
///     if stop {
///         break;
///     }
///     if job {
///         let _job = jobs.try_recv()?.expect("readable");
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Sockets whose number is known only as the program runs go in a `Vec`,
/// as libzmq's `zmq_poller_wait_all`. The items borrow the sockets only
/// for the call, so the `Vec` is made anew for each:
///
/// ```
/// # use zmq::{Context, Pull, Push};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let context = Context::new()?;
/// # let workers = 3;
/// let mut results = Vec::new();
/// for worker in 0..workers {
///     let mut pull = context.socket::<Pull>()?;
///     pull.bind(format!("inproc://worker-{worker}"))?;
///     results.push(pull);
/// }
/// # let mut last = context.socket::<Push>()?;
/// # last.connect("inproc://worker-2")?;
/// # last.send("done")?;
/// let ready = zmq::poll(results.iter_mut().map(|pull| pull.readable()).collect::<Vec<_>>())?;
/// for (pull, _) in results.iter_mut().zip(ready).filter(|(_, ready)| *ready) {
///     let _result = pull.try_recv()?.expect("readable");
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
///
/// # Panics
///
/// If `items` is an empty `Vec`, which would wait forever too.
#[track_caller]
pub fn poll<I: PollItems>(items: I) -> Result<I::Ready, Terminated> {
    items.assert_some();
    wait_until(items, None).map(|ready| ready.expect("no deadline"))
}

/// Waits at most `timeout` for at least one of `items` to be ready, as
/// [`poll`] does. `None` if none is ready in time, which with no items is
/// always.
pub fn poll_timeout<I: PollItems>(
    items: I,
    timeout: Duration,
) -> Result<Option<I::Ready>, Terminated> {
    wait_until(items, wait::deadline(timeout))
}

fn wait_until<I: PollItems>(
    mut items: I,
    deadline: Option<Instant>,
) -> Result<Option<I::Ready>, Terminated> {
    let mut ready = items.none_ready();
    wait::block_on_until(deadline, |cx| {
        if items.poll_terminated(&mut noop()) {
            return Poll::Ready(Err(Terminated));
        }
        if items.poll_each(&mut ready, cx) {
            Poll::Ready(Ok(()))
        } else if items.poll_terminated(cx) {
            Poll::Ready(Err(Terminated))
        } else {
            Poll::Pending
        }
    })
    .transpose()
    .map(|polled| polled.map(|()| ready))
}
