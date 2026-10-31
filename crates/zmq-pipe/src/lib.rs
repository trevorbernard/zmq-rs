//! Lock-free single-producer, single-consumer message pipes.
//!
//! The Rust counterpart of libzmq's `ypipe`: a queue between exactly one
//! [`Writer`] and one [`Reader`], with a high-water mark and wakers so either
//! side can wait by blocking the thread or by `.await`, on any executor.
//!
//! Like libzmq, a writer that has hit the high-water mark is woken once the
//! reader has caught up to the low-water mark, half the high-water mark,
//! rather than after every message.

mod pipe;
mod queue;

pub use pipe::{
    Closed, Hwm, Reader, ReaderGone, RecvTimeoutError, TryRecvError, TrySendError, Writer, pipe,
    pipe_conflated,
};
