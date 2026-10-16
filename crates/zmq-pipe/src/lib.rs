//! Lock-free single-producer, single-consumer message pipes.
//!
//! The Rust counterpart of libzmq's `ypipe`/`yqueue`: chunked queues with
//! writer-side batching, high/low-water mark flow control, and waker hooks so
//! readers can block or `.await` without depending on an executor.
