//! ZMTP socket types.

/// A socket type that speaks ZMTP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SocketType {
    Pair,
    Pub,
    Sub,
    Req,
    Rep,
    Dealer,
    Router,
    Pull,
    Push,
    XPub,
    XSub,
    Server,
    Client,
    Radio,
    Dish,
    Gather,
    Scatter,
    Peer,
    Channel,
}

impl SocketType {
    /// The socket-type octet sent in a ZMTP 2.0 greeting (RFC 15), or `None`
    /// for types that did not exist before ZMTP 3.x.
    ///
    /// XPUB and XSUB are sent as PUB and SUB, as RFC 15 requires.
    pub const fn legacy_code(self) -> Option<u8> {
        match self {
            Self::Pair => Some(0),
            Self::Pub | Self::XPub => Some(1),
            Self::Sub | Self::XSub => Some(2),
            Self::Req => Some(3),
            Self::Rep => Some(4),
            Self::Dealer => Some(5),
            Self::Router => Some(6),
            Self::Pull => Some(7),
            Self::Push => Some(8),
            Self::Server
            | Self::Client
            | Self::Radio
            | Self::Dish
            | Self::Gather
            | Self::Scatter
            | Self::Peer
            | Self::Channel => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classic_types_use_rfc_15_codes() {
        let expected = [
            (SocketType::Pair, 0),
            (SocketType::Pub, 1),
            (SocketType::Sub, 2),
            (SocketType::Req, 3),
            (SocketType::Rep, 4),
            (SocketType::Dealer, 5),
            (SocketType::Router, 6),
            (SocketType::Pull, 7),
            (SocketType::Push, 8),
            (SocketType::XPub, 1),
            (SocketType::XSub, 2),
        ];
        for (ty, code) in expected {
            assert_eq!(ty.legacy_code(), Some(code), "{ty:?}");
        }
    }

    #[test]
    fn thread_safe_types_have_no_legacy_code() {
        for ty in [
            SocketType::Server,
            SocketType::Client,
            SocketType::Radio,
            SocketType::Dish,
            SocketType::Gather,
            SocketType::Scatter,
            SocketType::Peer,
            SocketType::Channel,
        ] {
            assert_eq!(ty.legacy_code(), None, "{ty:?}");
        }
    }
}
