//! Security mechanisms for connections, and the authenticator a server
//! asks about each client (RFC 24, 25, 26, 27).

use std::fmt;
use std::sync::{Arc, mpsc};
use std::task::Waker;

use zmtp::{Frame, Keypair, PlainCredentials, PublicKey, ZapReply, ZapRequest};

/// How a connection secures itself. Both ends must agree on the mechanism.
#[derive(Clone, Default)]
#[non_exhaustive]
pub enum Security {
    /// No security (the default).
    #[default]
    Null,
    /// No security, but `authenticator` is asked about each peer before the
    /// handshake completes, as libzmq does for a NULL socket with a ZAP
    /// domain. It sees `ZapCredentials::Null` and the peer's address.
    /// ZMTP 1.0 and 2.0 peers are refused, as libzmq refuses them.
    NullAuthenticated {
        /// The ZAP domain the authenticator sees.
        domain: Frame,
        authenticator: Arc<dyn Authenticator>,
    },
    /// PLAIN client: sends a username and password in clear text, so use it
    /// only on trusted networks or over a secure transport.
    PlainClient(PlainCredentials),
    /// PLAIN server: asks `authenticator` about each client's credentials.
    PlainServer {
        /// The ZAP domain the authenticator sees; may be empty.
        domain: Frame,
        authenticator: Arc<dyn Authenticator>,
    },
    /// CURVE client: encrypts and authenticates the connection with its
    /// long-term `keypair`, to the server whose public key is `server_key`.
    CurveClient {
        keypair: Arc<Keypair>,
        server_key: PublicKey,
    },
    /// CURVE server with its long-term `keypair`. Clients must know its
    /// public key. With an `authenticator`, it is asked about each client's
    /// public key; without one, any client is accepted, as libzmq does with
    /// no ZAP handler.
    CurveServer {
        keypair: Arc<Keypair>,
        /// The ZAP domain the authenticator sees; may be empty.
        domain: Frame,
        authenticator: Option<Arc<dyn Authenticator>>,
    },
}

impl Security {
    /// No security, but `authenticator` is asked about each peer, by its
    /// address, before the handshake completes.
    pub fn null_authenticated(domain: impl Into<Frame>, authenticator: impl Authenticator) -> Self {
        Self::NullAuthenticated {
            domain: domain.into(),
            authenticator: Arc::new(authenticator),
        }
    }

    /// A PLAIN server in `domain` that asks `authenticator`.
    pub fn plain_server(domain: impl Into<Frame>, authenticator: impl Authenticator) -> Self {
        Self::PlainServer {
            domain: domain.into(),
            authenticator: Arc::new(authenticator),
        }
    }

    /// A CURVE client with its long-term `keypair`, for the server whose
    /// public key is `server_key`.
    pub fn curve_client(keypair: impl Into<Arc<Keypair>>, server_key: PublicKey) -> Self {
        Self::CurveClient {
            keypair: keypair.into(),
            server_key,
        }
    }

    /// A CURVE server that accepts any client knowing its public key.
    pub fn curve_server(keypair: impl Into<Arc<Keypair>>) -> Self {
        Self::CurveServer {
            keypair: keypair.into(),
            domain: Frame::new(),
            authenticator: None,
        }
    }

    /// A CURVE server in `domain` that asks `authenticator` about each
    /// client's public key.
    pub fn curve_server_authenticated(
        keypair: impl Into<Arc<Keypair>>,
        domain: impl Into<Frame>,
        authenticator: impl Authenticator,
    ) -> Self {
        Self::CurveServer {
            keypair: keypair.into(),
            domain: domain.into(),
            authenticator: Some(Arc::new(authenticator)),
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
            Self::NullAuthenticated { domain, .. } => f
                .debug_struct("NullAuthenticated")
                .field("domain", domain)
                .finish_non_exhaustive(),
            Self::PlainClient(credentials) => {
                f.debug_tuple("PlainClient").field(credentials).finish()
            }
            Self::PlainServer { domain, .. } => f
                .debug_struct("PlainServer")
                .field("domain", domain)
                .finish_non_exhaustive(),
            Self::CurveClient {
                keypair,
                server_key,
            } => f
                .debug_struct("CurveClient")
                .field("public", &keypair.public())
                .field("server_key", server_key)
                .finish(),
            Self::CurveServer {
                keypair,
                domain,
                authenticator,
            } => f
                .debug_struct("CurveServer")
                .field("public", &keypair.public())
                .field("domain", domain)
                .field("authenticated", &authenticator.is_some())
                .finish(),
        }
    }
}

impl fmt::Debug for ZapResponder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ZapResponder")
    }
}
