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
//! let bound = pull.bind("tcp://127.0.0.1:*")?;
//! let mut push = context.socket::<Push>();
//! push.connect(&bound.to_string())?;
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
//!
//! A REP socket answers one request at a time: it cannot receive another
//! while one is unanswered,
//!
//! ```compile_fail,E0499
//! let context = zmq::Context::new().unwrap();
//! let mut rep = context.socket::<zmq::Rep>();
//! let first = rep.recv();
//! let second = rep.recv();
//! first.reply("a");
//! ```
//!
//! and cannot answer a request twice:
//!
//! ```compile_fail,E0382
//! let context = zmq::Context::new().unwrap();
//! let mut rep = context.socket::<zmq::Rep>();
//! let request = rep.recv();
//! request.reply("a");
//! request.reply("b");
//! ```
//!
//! A ROUTER cannot send without saying to whom:
//!
//! ```compile_fail,E0061
//! let context = zmq::Context::new().unwrap();
//! let mut router = context.socket::<zmq::Router>();
//! router.send("to nobody in particular");
//! ```

mod endpoint;
mod options;
mod socket;
mod wait;

use std::io;

pub use endpoint::{Endpoint, Error};
pub use options::{InvalidRoutingId, RoutingId, SocketOptions};
pub use socket::{Dealer, Pull, Push, Rep, Req, Request, Router, Socket, SocketType};
pub use zmq_core::{
    Authenticator, Heartbeat, Linger, PeerGone, Reconnect, RouteOutcome, Security, ZapResponder,
};
pub use zmq_pipe::Hwm;
pub use zmtp::{
    Credential, Frame, Identity, Message, PlainCredentials, ZapCredentials, ZapReply, ZapRequest,
};

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

    /// A new socket of type `T` with libzmq's default options.
    pub fn socket<T: SocketType>(&self) -> Socket<T> {
        self.socket_with(SocketOptions::default())
    }

    /// A new socket of type `T` with `options`.
    pub fn socket_with<T: SocketType>(&self, options: SocketOptions) -> Socket<T> {
        Socket::new(self.core.clone(), options)
    }
}
