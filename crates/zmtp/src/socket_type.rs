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
    /// Every ZMTP socket type.
    pub const ALL: [Self; 19] = [
        Self::Pair,
        Self::Pub,
        Self::Sub,
        Self::Req,
        Self::Rep,
        Self::Dealer,
        Self::Router,
        Self::Pull,
        Self::Push,
        Self::XPub,
        Self::XSub,
        Self::Server,
        Self::Client,
        Self::Radio,
        Self::Dish,
        Self::Gather,
        Self::Scatter,
        Self::Peer,
        Self::Channel,
    ];

    /// The `Socket-Type` property value (RFC 23, 37).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Pair => "PAIR",
            Self::Pub => "PUB",
            Self::Sub => "SUB",
            Self::Req => "REQ",
            Self::Rep => "REP",
            Self::Dealer => "DEALER",
            Self::Router => "ROUTER",
            Self::Pull => "PULL",
            Self::Push => "PUSH",
            Self::XPub => "XPUB",
            Self::XSub => "XSUB",
            Self::Server => "SERVER",
            Self::Client => "CLIENT",
            Self::Radio => "RADIO",
            Self::Dish => "DISH",
            Self::Gather => "GATHER",
            Self::Scatter => "SCATTER",
            Self::Peer => "PEER",
            Self::Channel => "CHANNEL",
        }
    }

    /// Parses a `Socket-Type` property value. Names are case-sensitive.
    #[must_use]
    pub fn from_name(name: &[u8]) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|ty| ty.name().as_bytes() == name)
    }

    /// Decodes a ZMTP 1.0/2.0 greeting socket-type octet. Accepts RFC 15's
    /// codes and libzmq's, which sends 9 and 10 for XPUB and XSUB.
    #[must_use]
    pub const fn from_legacy_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Pair),
            1 => Some(Self::Pub),
            2 => Some(Self::Sub),
            3 => Some(Self::Req),
            4 => Some(Self::Rep),
            5 => Some(Self::Dealer),
            6 => Some(Self::Router),
            7 => Some(Self::Pull),
            8 => Some(Self::Push),
            9 => Some(Self::XPub),
            10 => Some(Self::XSub),
            _ => None,
        }
    }

    /// Whether a peer of type `peer` may connect to this socket, per RFC 23/37
    /// and libzmq (which also allows DEALER to DEALER).
    #[must_use]
    pub const fn accepts(self, peer: Self) -> bool {
        use SocketType::*;
        matches!(
            (self, peer),
            (Req, Rep | Router)
                | (Rep, Req | Dealer)
                | (Dealer, Rep | Dealer | Router)
                | (Router, Req | Dealer | Router)
                | (Pub | XPub, Sub | XSub)
                | (Sub | XSub, Pub | XPub)
                | (Push, Pull)
                | (Pull, Push)
                | (Pair, Pair)
                | (Server, Client)
                | (Client, Server)
                | (Radio, Dish)
                | (Dish, Radio)
                | (Scatter, Gather)
                | (Gather, Scatter)
                | (Peer, Peer)
                | (Channel, Channel)
        )
    }

    /// Whether READY carries this socket's `Identity` (REQ, DEALER, ROUTER).
    #[must_use]
    pub const fn sends_identity(self) -> bool {
        matches!(self, Self::Req | Self::Dealer | Self::Router)
    }

    /// The socket-type octet sent in a ZMTP 2.0 greeting (RFC 15), or `None`
    /// for types that did not exist before ZMTP 3.x.
    ///
    /// XPUB and XSUB are sent as PUB and SUB, as RFC 15 requires.
    #[must_use]
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
    fn names_round_trip() {
        let names: Vec<&str> = SocketType::ALL.iter().map(|ty| ty.name()).collect();
        assert_eq!(
            names,
            [
                "PAIR", "PUB", "SUB", "REQ", "REP", "DEALER", "ROUTER", "PULL", "PUSH", "XPUB",
                "XSUB", "SERVER", "CLIENT", "RADIO", "DISH", "GATHER", "SCATTER", "PEER",
                "CHANNEL",
            ]
        );
        for ty in SocketType::ALL {
            assert_eq!(SocketType::from_name(ty.name().as_bytes()), Some(ty));
        }
        assert_eq!(SocketType::from_name(b"dealer"), None);
        assert_eq!(SocketType::from_name(b"STREAM"), None);
    }

    #[test]
    fn legacy_codes_decode_rfc_15_and_libzmq_values() {
        for (code, ty) in SocketType::ALL[..11].iter().enumerate() {
            assert_eq!(SocketType::from_legacy_code(code as u8), Some(*ty));
        }
        assert_eq!(SocketType::from_legacy_code(11), None);
        assert_eq!(SocketType::from_legacy_code(255), None);
    }

    #[test]
    fn compatibility_matches_libzmq() {
        use SocketType::*;
        let pairs = [
            (Req, Rep),
            (Req, Router),
            (Rep, Dealer),
            (Dealer, Dealer),
            (Dealer, Router),
            (Router, Router),
            (Pub, Sub),
            (Pub, XSub),
            (XPub, Sub),
            (XPub, XSub),
            (Push, Pull),
            (Pair, Pair),
            (Server, Client),
            (Radio, Dish),
            (Scatter, Gather),
            (Peer, Peer),
            (Channel, Channel),
        ];
        for a in SocketType::ALL {
            for b in SocketType::ALL {
                let expected = pairs.contains(&(a, b)) || pairs.contains(&(b, a));
                assert_eq!(a.accepts(b), expected, "{a:?} accepts {b:?}");
            }
        }
    }

    #[test]
    fn only_routing_types_send_identity() {
        let senders: Vec<_> = SocketType::ALL
            .into_iter()
            .filter(|ty| ty.sends_identity())
            .collect();
        assert_eq!(
            senders,
            [SocketType::Req, SocketType::Dealer, SocketType::Router]
        );
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
