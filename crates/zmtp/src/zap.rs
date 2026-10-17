//! ZAP, the ZeroMQ Authentication Protocol (RFC 27), as plain data.
//!
//! A server handshake hands the caller a [`ZapRequest`] and waits for a
//! [`ZapReply`], so authentication can be synchronous or asynchronous. RFC 27's
//! wire framing (version, request id, the `inproc://zeromq.zap.01` socket)
//! only matters to an adapter for external ZAP handlers.

use std::fmt;

use zeroize::Zeroizing;

use crate::{Frame, Metadata, PublicKey, TooLong};

/// What a server asks its authenticator about a connecting peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZapRequest {
    /// The server's ZAP domain, possibly empty.
    pub domain: Frame,
    /// The peer's address, e.g. `127.0.0.1`.
    pub address: String,
    /// The server socket's own identity, as libzmq sends it.
    pub identity: Frame,
    /// What the peer presented.
    pub credentials: ZapCredentials,
}

/// The credentials a peer presented, by mechanism.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZapCredentials {
    /// NULL presents nothing.
    Null,
    /// PLAIN presents a username and password.
    Plain(PlainCredentials),
    /// CURVE presents the client's long-term public key, which the
    /// handshake has proven the client holds the secret key for.
    Curve(PublicKey),
}

/// The authenticator's verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZapReply {
    /// 200: accepted, with the user id and metadata to attach to the
    /// connection.
    Ok { user_id: Frame, metadata: Metadata },
    /// 300: try again later. The connection closes without an ERROR.
    TemporaryFailure,
    /// 400: the credentials were rejected. The peer receives ERROR `400`.
    AuthenticationFailure,
    /// 500: the authenticator failed. The peer receives ERROR `500`.
    InternalError,
}

/// Why authentication failed, as reported by [`ZapReply`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZapFailure {
    Temporary,
    Denied,
    Internal,
}

/// What an accepted peer is known as.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Authenticated {
    pub user_id: Frame,
    pub metadata: Metadata,
}

/// A PLAIN username and password.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainCredentials {
    pub username: Credential,
    pub password: Credential,
}

const MAX_CREDENTIAL: usize = u8::MAX as usize;

/// A secret of at most 255 octets, zeroed when dropped and never printed.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct Credential(Zeroizing<Vec<u8>>);

impl ZapCredentials {
    /// The mechanism name RFC 27 puts in the request.
    pub fn mechanism(&self) -> &'static str {
        match self {
            Self::Null => "NULL",
            Self::Plain(_) => "PLAIN",
            Self::Curve(_) => "CURVE",
        }
    }
}

impl Credential {
    /// Validates `value`: at most 255 octets.
    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self, TooLong> {
        let value = Zeroizing::new(value.into());
        if value.len() > MAX_CREDENTIAL {
            return Err(TooLong {
                max: MAX_CREDENTIAL,
            });
        }
        Ok(Self(value))
    }

    /// The secret octets.
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credential(..)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_hold_up_to_255_octets() {
        assert_eq!(
            Credential::new(vec![b'x'; 255]).unwrap().expose().len(),
            255
        );
        assert_eq!(Credential::new(vec![b'x'; 256]), Err(TooLong { max: 255 }));
        assert_eq!(Credential::new("").unwrap().expose(), b"");
    }

    #[test]
    fn credentials_are_never_printed() {
        let credentials = PlainCredentials {
            username: Credential::new("admin").unwrap(),
            password: Credential::new("hunter2").unwrap(),
        };
        let printed = format!("{credentials:?}");
        assert!(
            !printed.contains("admin") && !printed.contains("hunter2"),
            "{printed}"
        );
        assert!(printed.contains("Credential(..)"), "{printed}");
    }

    #[test]
    fn mechanism_names() {
        assert_eq!(ZapCredentials::Null.mechanism(), "NULL");
        let plain = ZapCredentials::Plain(PlainCredentials {
            username: Credential::default(),
            password: Credential::default(),
        });
        assert_eq!(plain.mechanism(), "PLAIN");
    }
}
