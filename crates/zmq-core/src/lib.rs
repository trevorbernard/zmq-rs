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
mod monitor;
mod pair;
mod pubsub;
#[cfg(feature = "draft")]
mod radiodish;
mod reconnect;
mod reqrep;
mod security;
mod socket;
#[cfg(test)]
mod test_support;
mod transport;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub use context::IpcAbstractListener;
#[cfg(unix)]
pub use context::IpcListener;
pub use context::{
    Connection, ConnectionConfig, Context, Heartbeat, InprocListener, Linger, Listener,
    ShutdownTimedOut,
};
pub use monitor::{Event, EventKind, HandshakeFailure, Monitor, MonitorSink, ProtocolError};
pub use pair::Pair;
pub use pubsub::{Publisher, Subscriber, Topics, Verbosity, XPubEvent};
#[cfg(feature = "draft")]
pub use radiodish::{Dish, Radio};
pub use reconnect::Reconnect;
pub use reqrep::{Envelope, Outstanding, PeerGone, Received, Replier, Requester};
pub use security::{Authenticator, Security, ZapResponder};
pub use socket::{
    FairQueue, LoadBalancer, Peer, PeerKey, PeerList, Peers, RouteOutcome, RouterSendError,
    RouterTable,
};
#[cfg(feature = "draft")]
pub use socket::{ServerSendError, ServerTable};
