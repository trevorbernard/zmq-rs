//! ZeroMQ engine internals.
//!
//! I/O threads, transports (tcp, ipc, inproc), sessions, routing strategies
//! (load balancing, fair queuing, distribution, subscription tries) and the
//! per-socket-type patterns from RFC 28-31, 41, 48, 49, 51 and 52.
//!
//! See `docs/architecture.md` for how the pieces fit together.

mod context;
mod engine;
mod io_thread;

pub use context::{Connection, ConnectionConfig, Context};
