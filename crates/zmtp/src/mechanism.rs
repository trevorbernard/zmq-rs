//! Security mechanism selection.

/// Which side of an asymmetric security handshake this peer plays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Client,
    Server,
}

/// Whether a NULL socket accepts ZMTP 1.0 and 2.0 peers.
///
/// Only NULL can downgrade: older protocol versions have no security
/// handshake, so RFC 37 requires every other mechanism to refuse them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum LegacyPeers {
    #[default]
    Allow,
    Refuse,
}

/// The security mechanism announced in the greeting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mechanism {
    /// No security (RFC 23/37). Has no client or server role.
    Null { legacy_peers: LegacyPeers },
    /// PLAIN (RFC 24).
    Plain(Role),
    /// CURVE (RFC 25/26).
    Curve(Role),
}

impl Mechanism {
    /// The mechanism name, NUL-padded to the 20-octet greeting field.
    pub(crate) const fn greeting_name(self) -> [u8; 20] {
        match self {
            Self::Null { .. } => *b"NULL\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
            Self::Plain(_) => *b"PLAIN\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
            Self::Curve(_) => *b"CURVE\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0",
        }
    }

    /// The greeting `as-server` flag. Always false for NULL.
    pub(crate) const fn as_server(self) -> bool {
        matches!(self, Self::Plain(Role::Server) | Self::Curve(Role::Server))
    }

    /// Whether ZMTP 1.0 and 2.0 peers may be accepted.
    pub(crate) const fn allows_legacy_peers(self) -> bool {
        matches!(
            self,
            Self::Null {
                legacy_peers: LegacyPeers::Allow
            }
        )
    }
}
