//! A socket's file descriptor and its events, for an event loop that waits
//! on descriptors: libzmq's `ZMQ_FD` and `ZMQ_EVENTS`.

use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::sync::Arc;
use std::task::{Context, Waker};

use crate::polling::{PollReadable, PollWritable};
use crate::socket::sealed::Terminates;
use crate::socket::{
    Dealer, ManualXPub, Pair, Pull, Push, Receives, Rep, Router, Socket, SocketType, Sub,
    Terminated, WaitsForRoom, XPub, XSub, noop,
};
use crate::wake_fd::WakeFd;

/// A socket's file descriptor, from [`Socket::fd`]: readable when the
/// socket's [`events`](Socket::events) may have changed. It is for waiting
/// on only, and is never to be read or written.
///
/// It is the socket's and not borrowed from it, so that it can stay with
/// an event loop while the socket is used. Clones are the same descriptor.
/// One kept after its socket is dropped stays open, with nothing more to
/// report.
#[derive(Clone, Debug)]
pub struct SocketFd(Arc<WakeFd>);

impl AsFd for SocketFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

impl AsRawFd for SocketFd {
    fn as_raw_fd(&self) -> RawFd {
        self.as_fd().as_raw_fd()
    }
}

/// A socket's descriptor, and the waker that makes it readable.
#[derive(Clone, Debug)]
pub(crate) struct Signal {
    fd: SocketFd,
    pub(crate) waker: Waker,
}

impl Signal {
    /// Empties the descriptor, so that only a later wake makes it
    /// readable.
    pub(crate) fn drain(&self) {
        self.fd.0.drain();
    }
}

/// What a socket is ready for, from [`Socket::events`]: libzmq's
/// `ZMQ_EVENTS`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Events {
    /// A receive would not wait: `ZMQ_POLLIN`. Never, for a type that
    /// does not receive.
    pub readable: bool,
    /// A send would not wait: `ZMQ_POLLOUT`. Always, for a type whose
    /// sends never wait, such as ROUTER; never, for one that does not send.
    pub writable: bool,
}

/// A socket type whose sockets can be waited on: they have
/// [`events`](Socket::events), and a [file descriptor](Socket::fd) that
/// says when to look at them. These are the types that can be
/// [polled](crate::poll); REQ, PUB and RADIO are not among them.
pub trait ReportsEvents: SocketType + Sized {
    /// What the socket is ready for; registers `cx` for what it is not.
    #[doc(hidden)]
    fn poll_events(socket: &mut Socket<Self>, cx: &mut Context<'_>) -> Events;
}

fn receives<T: Receives>(socket: &mut Socket<T>, cx: &mut Context<'_>) -> bool {
    socket.poll_readable(cx).is_ready()
}

fn has_room<T: WaitsForRoom>(socket: &mut Socket<T>, cx: &mut Context<'_>) -> bool {
    socket.poll_writable(cx).is_ready()
}

fn always<T: SocketType>(_: &mut Socket<T>, _: &mut Context<'_>) -> bool {
    true
}

fn never<T: SocketType>(_: &mut Socket<T>, _: &mut Context<'_>) -> bool {
    false
}

/// Each type's events, as its libzmq counterpart's `xhas_in` and
/// `xhas_out` have them.
macro_rules! reports_events {
    ($($type:ty: $readable:ident, $writable:ident;)*) => {$(
        impl ReportsEvents for $type {
            fn poll_events(socket: &mut Socket<Self>, cx: &mut Context<'_>) -> Events {
                Events {
                    readable: $readable(socket, cx),
                    writable: $writable(socket, cx),
                }
            }
        }
    )*};
}

reports_events! {
    Pair: receives, has_room;
    Dealer: receives, has_room;
    Push: never, has_room;
    Pull: receives, never;
    Sub: receives, never;
    XSub: receives, always;
    XPub: receives, always;
    ManualXPub: receives, always;
    Router: receives, always;
    // A reply goes through the request that `try_recv` lends out, so
    // between receives there is none to send.
    Rep: receives, never;
}

#[cfg(feature = "draft")]
reports_events! {
    crate::Client: receives, has_room;
    crate::Channel: receives, has_room;
    crate::Scatter: never, has_room;
    crate::Gather: receives, never;
    crate::Server: receives, always;
    crate::Peer: receives, always;
    // libzmq's DISH is always writable, as an XSUB is, though it does not
    // send.
    crate::Dish: receives, always;
}

impl<T: ReportsEvents> Socket<T> {
    /// The socket's file descriptor, for an event loop that waits on
    /// descriptors and is not an async executor: libzmq's `ZMQ_FD`. It is
    /// made by the first call, and every call returns the same one.
    ///
    /// The descriptor is edge-triggered, as libzmq's is. It turns readable
    /// when the socket's [`events`](Self::events) may have changed, and
    /// says no more than that:
    ///
    /// - Readable is a reason to look at `events`, or to call `try_recv`
    ///   or `try_send`, which may still find nothing to do.
    /// - Once the socket is used it stays quiet until the socket has been
    ///   found not ready: until `events` has shown what is waited for to
    ///   be unset, or a `try_recv` has returned `None`, or a `try_send`
    ///   has been refused for want of room. So a loop receives until
    ///   there is no more, or looks at `events` again, before it waits.
    /// - It starts readable, so that a loop that waits first looks at the
    ///   socket once.
    ///
    /// ```
    /// # use std::os::fd::AsFd;
    /// # use zmq::{Context, PollItem, Pull, Push};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let context = Context::new()?;
    /// let mut jobs = context.socket::<Pull>()?;
    /// jobs.bind("inproc://jobs")?;
    /// # let mut boss = context.socket::<Push>()?;
    /// # boss.connect("inproc://jobs")?;
    /// # boss.send("job")?;
    /// let fd = jobs.fd()?;
    /// let mut done = 0;
    /// while done == 0 {
    ///     // An event loop of your own waits on `fd` here, among its
    ///     // other descriptors.
    ///     zmq::poll([PollItem::fd_readable(fd.as_fd())?])?;
    ///     while let Some(_job) = jobs.try_recv()? {
    ///         done += 1;
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A shutdown of the context makes it readable too, and `events` then
    /// fails.
    ///
    /// # Errors
    ///
    /// If the descriptor cannot be made, as when the process has none
    /// left.
    pub fn fd(&mut self) -> io::Result<SocketFd> {
        if let Some(signal) = &self.signal {
            return Ok(signal.fd.clone());
        }
        let wake = Arc::new(WakeFd::new()?);
        // Nothing is registered to make it readable until the socket is
        // looked at.
        wake.signal();
        let fd = SocketFd(wake.clone());
        self.signal = Some(Signal {
            fd: fd.clone(),
            waker: Waker::from(wake),
        });
        Ok(fd)
    }

    /// Whether a receive and a send would wait: libzmq's `ZMQ_EVENTS`.
    ///
    /// With a [file descriptor](Self::fd), this is what to call when it
    /// turns readable. The descriptor then stays quiet until one of the
    /// events found unset here is set, or the context is shut down.
    pub fn events(&mut self) -> Result<Events, Terminated> {
        self.alive()?;
        let Some(signal) = self.signal.clone() else {
            return Ok(T::poll_events(self, &mut noop()));
        };
        // Before the socket is looked at: a wake that follows leaves the
        // descriptor readable.
        signal.drain();
        let mut cx = Context::from_waker(&signal.waker);
        let events = T::poll_events(self, &mut cx);
        if self.poll_terminated(&mut cx) {
            return Err(Terminated);
        }
        Ok(events)
    }
}
