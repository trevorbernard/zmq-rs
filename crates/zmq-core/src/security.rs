//! Security mechanisms for connections, and the authenticator a PLAIN
//! server asks about each client (RFC 24, RFC 27).

use std::fmt;
use std::sync::{Arc, mpsc};
use std::task::Waker;

use zmtp::{Frame, PlainCredentials, ZapReply, ZapRequest};

/// How a connection secures itself. Both ends must agree on the mechanism.
#[derive(Clone, Default)]
pub enum Security {
    /// No security (the default).
    #[default]
    Null,
    /// PLAIN client: sends a username and password in clear text, so use it
    /// only on trusted networks or over a secure transport.
    PlainClient(PlainCredentials),
    /// PLAIN server: asks `authenticator` about each client's credentials.
    PlainServer {
        /// The ZAP domain the authenticator sees; may be empty.
        domain: Frame,
        authenticator: Arc<dyn Authenticator>,
    },
}

impl Security {
    /// A PLAIN server in `domain` that asks `authenticator`.
    pub fn plain_server(domain: impl Into<Frame>, authenticator: impl Authenticator) -> Self {
        Self::PlainServer {
            domain: domain.into(),
            authenticator: Arc::new(authenticator),
        }
    }
}

/// Decides whether a client may connect, like a ZAP handler (RFC 27).
///
/// Called on an I/O thread when a client's credentials arrive. Answer
/// through `respond`, now or later and from any thread: a slow check need
/// not hold up the I/O thread. Dropping `respond` without answering
/// rejects the client as an internal error (ZAP status 500).
///
/// A closure `Fn(&ZapRequest) -> ZapReply` is an authenticator that
/// answers at once.
pub trait Authenticator: Send + Sync + 'static {
    /// Decides on `request` and answers through `respond`.
    fn authenticate(&self, request: ZapRequest, respond: ZapResponder);
}

impl<F> Authenticator for F
where
    F: Fn(&ZapRequest) -> ZapReply + Send + Sync + 'static,
{
    fn authenticate(&self, request: ZapRequest, respond: ZapResponder) {
        respond.respond(self(&request));
    }
}

/// Delivers an [`Authenticator`]'s answer to the waiting connection.
pub struct ZapResponder {
    reply: mpsc::Sender<ZapReply>,
    waker: Waker,
}

impl ZapResponder {
    pub(crate) fn new(reply: mpsc::Sender<ZapReply>, waker: Waker) -> Self {
        Self { reply, waker }
    }

    /// Answers the request.
    pub fn respond(self, reply: ZapReply) {
        // Dropping `self` afterwards wakes the connection.
        let _ = self.reply.send(reply);
    }
}

impl Drop for ZapResponder {
    /// Wakes the connection whether or not an answer was sent: unanswered,
    /// it sees the channel closed and rejects the client.
    fn drop(&mut self) {
        self.waker.wake_by_ref();
    }
}

impl fmt::Debug for Security {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("Null"),
            Self::PlainClient(credentials) => {
                f.debug_tuple("PlainClient").field(credentials).finish()
            }
            Self::PlainServer { domain, .. } => f
                .debug_struct("PlainServer")
                .field("domain", domain)
                .finish_non_exhaustive(),
        }
    }
}

impl fmt::Debug for ZapResponder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ZapResponder")
    }
}
