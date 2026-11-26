//! Waiting on several sockets at once, as libzmq's `zmq_poll` and
//! `zmq_poller_wait_all`, and on file descriptors with them.

use std::cell::RefCell;
use std::fmt;
use std::io;
use std::os::fd::BorrowedFd;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use rustix::event::{PollFd, PollFlags, Timespec};
use rustix::io::Errno;

use crate::socket::noop;
use crate::socket::sealed::{Sealed, Terminates};
use crate::wake_fd::WakeFd;
use crate::{Terminated, wait};

/// A socket or a file descriptor to [`poll`], and what to wait for. A
/// socket's is made by
/// [`Socket::readable`](crate::Socket::readable),
/// [`Socket::writable`](crate::Socket::writable) and
/// [`Socket::readable_and_writable`](crate::Socket::readable_and_writable),
/// a descriptor's by [`fd_readable`](Self::fd_readable) and
/// [`fd_writable`](Self::fd_writable);
/// it borrows the socket or the descriptor only until the poll returns.
pub struct PollItem<'a> {
    target: Target<'a>,
}

enum Target<'a> {
    Readable(&'a mut dyn PollReadable),
    Writable(&'a mut dyn PollWritable),
    /// One of the two items of a socket polled both ways, which share it.
    SharedReadable(Shared<'a>),
    SharedWritable(Shared<'a>),
    /// A file descriptor, the events asked of it, and what wakes the
    /// thread that waits on it.
    Fd(BorrowedFd<'a>, PollFlags, Arc<WakeFd>),
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

    /// A file descriptor of the application's own, to [`poll`] with its
    /// sockets until a read from it would not wait, as an item of
    /// libzmq's `zmq_poll` with `fd` set and `ZMQ_POLLIN`. It is also
    /// reported once it has failed or its peer has hung up, as libzmq
    /// reports `ZMQ_POLLERR` unasked, so that the read meets the error.
    ///
    /// ```
    /// # use std::io::Write;
    /// # use std::os::fd::AsFd;
    /// # use std::os::unix::net::UnixStream;
    /// # use zmq::{Context, PollItem, Pull};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let context = Context::new()?;
    /// let mut jobs = context.socket::<Pull>()?;
    /// jobs.bind("inproc://jobs")?;
    /// let (mut ours, theirs) = UnixStream::pair()?;
    /// ours.write_all(b"stop")?;
    ///
    /// let [job, stop] = zmq::poll([
    ///     jobs.readable(),
    ///     PollItem::fd_readable(theirs.as_fd())?,
    /// ])?;
    /// assert!(stop && !job);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// If the descriptor that wakes the calling thread from its wait
    /// cannot be made, as when the process has no descriptors left. Each
    /// thread makes one, with its first item.
    pub fn fd_readable(fd: BorrowedFd<'a>) -> io::Result<Self> {
        Self::fd(fd, PollFlags::IN)
    }

    /// A file descriptor of the application's own, to [`poll`] with its
    /// sockets until a write to it would not wait, as
    /// [`fd_readable`](Self::fd_readable) is for a read: libzmq's
    /// `ZMQ_POLLOUT`.
    ///
    /// # Errors
    ///
    /// As [`fd_readable`](Self::fd_readable).
    pub fn fd_writable(fd: BorrowedFd<'a>) -> io::Result<Self> {
        Self::fd(fd, PollFlags::OUT)
    }

    fn fd(fd: BorrowedFd<'a>, events: PollFlags) -> io::Result<Self> {
        Ok(Self {
            target: Target::Fd(fd, events, WakeFd::for_thread()?),
        })
    }

    /// The descriptor that wakes the thread, if this is a file descriptor.
    fn wake_fd(&self) -> Option<Arc<WakeFd>> {
        match &self.target {
            Target::Fd(_, _, wake) => Some(wake.clone()),
            _ => None,
        }
    }

    /// The file descriptor and the events asked of it, if this is one.
    fn descriptor(&self) -> Option<(BorrowedFd<'a>, PollFlags)> {
        match self.target {
            Target::Fd(fd, events, _) => Some((fd, events)),
            _ => None,
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
            // Its readiness is `poll(2)`'s to say, in `wait_in_poll`.
            Target::Fd(..) | Target::Skipped => Poll::Pending,
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
            Target::Fd(..) | Target::Skipped => false,
        }
    }
}

impl fmt::Debug for PollItem<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let interest = match self.target {
            Target::Readable(_) | Target::SharedReadable(_) => "readable",
            Target::Writable(_) | Target::SharedWritable(_) => "writable",
            Target::Fd(_, events, _) if events.contains(PollFlags::IN) => "fd readable",
            Target::Fd(..) => "fd writable",
            Target::Skipped => "skipped",
        };
        f.debug_tuple("PollItem").field(&interest).finish()
    }
}

/// The sockets and file descriptors given to [`poll`] and
/// [`poll_timeout`]: an array of [`PollItem`]s when how many there are is
/// known as the program is written, a `Vec` of them when it is not. Cannot
/// be implemented outside this crate.
pub trait PollItems: Sealed {
    /// Which of the items are ready, in their order: `[bool; N]` for an
    /// array, `Vec<bool>` for a `Vec`.
    type Ready;

    /// Refuses a set that [`poll`] would wait on forever.
    #[doc(hidden)]
    #[track_caller]
    fn assert_some(&self);

    /// Waits until an item is ready, or for `None` until `deadline`.
    #[doc(hidden)]
    fn wait(self, deadline: Option<Instant>) -> Result<Option<Self::Ready>, Terminated>;
}

impl<const N: usize> Sealed for [PollItem<'_>; N] {}

impl<const N: usize> PollItems for [PollItem<'_>; N] {
    type Ready = [bool; N];

    fn assert_some(&self) {
        const { assert!(N > 0, "polling no sockets would wait forever") };
    }

    fn wait(mut self, deadline: Option<Instant>) -> Result<Option<[bool; N]>, Terminated> {
        let mut ready = [false; N];
        Ok(wait_until(&mut self, &mut ready, deadline)?.then_some(ready))
    }
}

impl Sealed for Vec<PollItem<'_>> {}

impl PollItems for Vec<PollItem<'_>> {
    type Ready = Vec<bool>;

    fn assert_some(&self) {
        assert!(!self.is_empty(), "polling no sockets would wait forever");
    }

    fn wait(mut self, deadline: Option<Instant>) -> Result<Option<Vec<bool>>, Terminated> {
        let mut ready = vec![false; self.len()];
        Ok(wait_until(&mut self, &mut ready, deadline)?.then_some(ready))
    }
}

/// Whether an item was ready by `deadline`; `ready` says which.
fn wait_until(
    items: &mut [PollItem<'_>],
    ready: &mut [bool],
    deadline: Option<Instant>,
) -> Result<bool, Terminated> {
    match items.iter().find_map(PollItem::wake_fd) {
        Some(wake) => wait_in_poll(items, ready, deadline, &wake),
        None => wait_parked(items, ready, deadline),
    }
}

/// Waits on sockets alone, parking the thread.
fn wait_parked(
    items: &mut [PollItem<'_>],
    ready: &mut [bool],
    deadline: Option<Instant>,
) -> Result<bool, Terminated> {
    wait::block_on_until(deadline, |cx| {
        if any_terminated(items, &mut noop()) {
            return Poll::Ready(Err(Terminated));
        }
        if poll_each(items, ready, cx) {
            Poll::Ready(Ok(()))
        } else if any_terminated(items, cx) {
            Poll::Ready(Err(Terminated))
        } else {
            Poll::Pending
        }
    })
    .transpose()
    .map(|polled| polled.is_some())
}

/// Waits on sockets and file descriptors, in `poll(2)` on the descriptors
/// and on `wake`, which the sockets make readable.
fn wait_in_poll(
    items: &mut [PollItem<'_>],
    ready: &mut [bool],
    deadline: Option<Instant>,
    wake: &Arc<WakeFd>,
) -> Result<bool, Terminated> {
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut fds: Vec<_> = items
        .iter()
        .filter_map(PollItem::descriptor)
        .map(|(fd, events)| PollFd::from_borrowed_fd(fd, events))
        .chain([PollFd::new(&**wake, PollFlags::IN)])
        .collect();
    loop {
        if any_terminated(items, &mut noop()) {
            return Err(Terminated);
        }
        // Before the sockets are polled: a wake that follows leaves it
        // readable, and ends the wait below.
        wake.drain();
        let mut any = poll_each(items, ready, &mut cx);
        if !any && any_terminated(items, &mut cx) {
            return Err(Terminated);
        }
        // With a socket ready the descriptors are still looked at, without
        // waiting, so that a busy socket never hides one.
        let timeout = if any {
            Some(Duration::ZERO)
        } else {
            deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
        };
        if !poll_fds(&mut fds, timeout) {
            continue;
        }
        let mut polled = fds.iter();
        for (item, ready) in items.iter().zip(ready.iter_mut()) {
            if let Some((_, events)) = item.descriptor() {
                let fd = polled.next().expect("one for each descriptor");
                // An error or a hangup counts, asked for or not, so that
                // the caller's next read or write meets it.
                *ready = fd.revents().intersects(events | FAILED);
                any |= *ready;
            }
        }
        if any {
            return Ok(true);
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Ok(false);
        }
    }
}

/// What `poll(2)` reports of a descriptor whatever was asked of it.
const FAILED: PollFlags = PollFlags::ERR.union(PollFlags::HUP).union(PollFlags::NVAL);

/// `poll(2)`, for at most `timeout`; `false` if a signal interrupted it.
fn poll_fds(fds: &mut [PollFd<'_>], timeout: Option<Duration>) -> bool {
    // No system takes a wait of any length, and the caller waits again
    // until its deadline.
    const LONGEST: Duration = Duration::from_secs(3600);

    let timeout =
        timeout.map(|timeout| Timespec::try_from(timeout.min(LONGEST)).expect("an hour at most"));
    match rustix::event::poll(fds, timeout.as_ref()) {
        Ok(_) => true,
        Err(Errno::INTR) => false,
        Err(error) => panic!("poll(2) failed: {error}"),
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
    items.wait(None).map(|ready| ready.expect("no deadline"))
}

/// Waits at most `timeout` for at least one of `items` to be ready, as
/// [`poll`] does. `None` if none is ready in time, which with no items is
/// always.
pub fn poll_timeout<I: PollItems>(
    items: I,
    timeout: Duration,
) -> Result<Option<I::Ready>, Terminated> {
    items.wait(wait::deadline(timeout))
}
