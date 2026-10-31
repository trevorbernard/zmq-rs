//! ZeroMQ engine internals.
//!
//! I/O threads, transports (tcp, ipc, inproc), sessions, routing strategies
//! (load balancing, fair queuing, distribution, subscription tries) and the
//! per-socket-type patterns from RFC 28-31, 41, 48, 49, 51 and 52.
//!
//! See `docs/architecture.md` for how the pieces fit together.

mod context;
mod engine;
mod fair_queue;
mod io_thread;
mod load_balancer;
mod monitor;
mod output;
mod pair;
mod peers;
mod pubsub;
#[cfg(feature = "draft")]
mod radiodish;
mod reconnect;
mod reqrep;
mod router_table;
mod security;
#[cfg(feature = "draft")]
mod server_table;
#[cfg(test)]
mod test_support;
mod transport;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub use context::IpcAbstractListener;
#[cfg(unix)]
pub use context::IpcListener;
pub use context::{
    Connection, ConnectionConfig, Context, Heartbeat, Inbound, InprocListener, Linger, Listener,
    MessageProperties, ShutdownTimedOut, TcpKeepalive,
};
pub use fair_queue::FairQueue;
pub use load_balancer::LoadBalancer;
pub use monitor::{Event, EventKind, HandshakeFailure, Monitor, MonitorSink, ProtocolError};
pub use pair::Pair;
pub use peers::{EndpointId, Peer, PeerKey, PeerList, Peers, noop_context};
pub(crate) use pubsub::Topics;
pub use pubsub::{Publisher, Subscriber, Verbosity, XPubEvent};
#[cfg(feature = "draft")]
pub use radiodish::{Dish, Radio};
pub use reconnect::Reconnect;
pub use reqrep::{Envelope, Outstanding, PeerGone, Received, Replier, Requester, RouteOutcome};
pub use router_table::{RouterTable, SendError};
pub use security::{Authenticator, Security, ZapResponder};
#[cfg(feature = "draft")]
pub use server_table::ServerTable;
pub use transport::{interface_addr, resolve_tcp};
pub use zmq_pipe::{RecvTimeoutError, TryRecvError};
