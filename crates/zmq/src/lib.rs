//! Idiomatic, type-safe ZeroMQ for Rust.
//!
//! Socket types expose only the operations their pattern allows, so invalid
//! states such as receiving on a PUB socket or replying twice on REP are
//! compile errors rather than runtime `EFSM`/`ENOTSUP` errors.
//!
//! ```
//! use zmq::{Context, Pull, Push};
//!
//! let context = Context::new()?;
//! let mut pull = context.socket::<Pull>();
//! let addr = pull.bind("tcp://127.0.0.1:*")?;
//! let mut push = context.socket::<Push>();
//! push.connect(&format!("tcp://{addr}"))?;
//! push.send("hello");
//! assert_eq!(pull.recv(), "hello".into());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! A PUSH socket cannot receive:
//!
//! ```compile_fail,E0599
//! let context = zmq::Context::new().unwrap();
//! let mut push = context.socket::<zmq::Push>();
//! push.recv();
//! ```

mod endpoint;
mod socket;
mod wait;

use std::io;

pub use endpoint::Error;
pub use socket::{Pull, Push, Socket, SocketType};
pub use zmtp::{Frame, Message};

/// Owns the I/O threads sockets run on. Cloning shares them.
#[derive(Debug, Clone)]
pub struct Context {
    core: zmq_core::Context,
}

impl Context {
    /// A context with one I/O thread, like libzmq's default.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            core: zmq_core::Context::new()?,
        })
    }

    /// A new socket of type `T`, with no peers yet.
    pub fn socket<T: SocketType>(&self) -> Socket<T> {
        Socket::new(self.core.clone())
    }
}
