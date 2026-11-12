//! The client side of SOCKS5 (RFC 1928), with username and password
//! authentication (RFC 1929), as libzmq's `socks_connecter_t` speaks it to
//! the proxy of `ZMQ_SOCKS_PROXY` before a connection's ZMTP greeting.

use std::net::{IpAddr, SocketAddr};

use zeroize::Zeroizing;
use zmtp::Credential;

const VERSION: u8 = 5;
const AUTH_VERSION: u8 = 1;
const NO_AUTH: u8 = 0;
const BASIC_AUTH: u8 = 2;
const CONNECT: u8 = 1;
const IPV4: u8 = 1;
const DOMAIN: u8 = 3;
const IPV6: u8 = 4;

/// The SOCKS5 proxy a socket's TCP connections are made through
/// (`ZMQ_SOCKS_PROXY`), and who it is told we are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocksProxy {
    /// The proxy's host name or address, resolved for each connection
    /// attempt.
    pub host: String,
    pub port: u16,
    /// Where to connect to the proxy from, if anywhere in particular.
    pub source: Option<SocketAddr>,
    /// Who to authenticate as (`ZMQ_SOCKS_USERNAME`). Empty for no
    /// authentication, as in libzmq.
    pub username: Credential,
    /// `ZMQ_SOCKS_PASSWORD`.
    pub password: Credential,
}

/// Why a proxy did not connect us. The connection is closed and, as in
/// libzmq, tried again later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocksError {
    /// The destination cannot be asked for: a host name over 255 octets,
    /// or port 0.
    Unaskable,
    /// A reply that is not SOCKS5's.
    Malformed,
    /// The proxy takes neither no authentication nor a user name and
    /// password.
    NoAcceptableMethod,
    AuthenticationFailed,
    /// The proxy's reply code, such as 5 for a refused connection.
    Refused(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Choice,
    AuthResponse,
    Reply,
}

/// A SOCKS5 negotiation, from our greeting to the proxy's reply to
/// CONNECT. Reads exactly the proxy's octets, so that the peer's ZMTP
/// greeting, which may follow at once, is left in the stream.
#[derive(Debug)]
pub struct SocksClient {
    stage: Stage,
    method: u8,
    auth: Zeroizing<Vec<u8>>,
    request: Vec<u8>,
    /// The reply being read.
    input: Vec<u8>,
}

impl SocksClient {
    /// A negotiation asking `proxy` to connect to `host`, a host name the
    /// proxy resolves or an address, and `port`.
    pub fn new(proxy: &SocksProxy, host: &str, port: u16) -> Result<Self, SocksError> {
        let (username, password) = (proxy.username.expose(), proxy.password.expose());
        let Ok(host_len) = u8::try_from(host.len()) else {
            return Err(SocksError::Unaskable);
        };
        if port == 0 {
            return Err(SocksError::Unaskable);
        }
        // A credential is at most 255 octets.
        let mut auth = Zeroizing::new(vec![AUTH_VERSION, username.len() as u8]);
        auth.extend_from_slice(username);
        auth.push(password.len() as u8);
        auth.extend_from_slice(password);

        let mut request = vec![VERSION, CONNECT, 0];
        match host.parse::<IpAddr>() {
            Ok(IpAddr::V4(ip)) => {
                request.push(IPV4);
                request.extend_from_slice(&ip.octets());
            }
            Ok(IpAddr::V6(ip)) => {
                request.push(IPV6);
                request.extend_from_slice(&ip.octets());
            }
            Err(_) => {
                request.push(DOMAIN);
                request.push(host_len);
                request.extend_from_slice(host.as_bytes());
            }
        }
        request.extend_from_slice(&port.to_be_bytes());
        Ok(Self {
            stage: Stage::Choice,
            // libzmq offers the one method its options ask for.
            method: if username.is_empty() {
                NO_AUTH
            } else {
                BASIC_AUTH
            },
            auth,
            request,
            input: Vec::new(),
        })
    }

    /// Writes our greeting, the first thing sent to the proxy.
    pub fn greet(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&[VERSION, 1, self.method]);
    }

    /// How many octets to read next: never more than the proxy's reply
    /// still has, and never none.
    pub fn wanted(&self) -> usize {
        let have = self.input.len();
        match self.stage {
            Stage::Choice | Stage::AuthResponse => 2 - have,
            Stage::Reply if have < 5 => 5 - have,
            Stage::Reply => reply_len(self.input[3], self.input[4]) - have,
        }
    }

    /// Takes `octets` from the proxy, at most [`wanted`](Self::wanted),
    /// writing what we answer to `out`. Returns whether the proxy has
    /// connected us, after which the stream is the peer's.
    pub fn feed(&mut self, octets: &[u8], out: &mut Vec<u8>) -> Result<bool, SocksError> {
        debug_assert!(octets.len() <= self.wanted());
        self.input.extend_from_slice(octets);
        self.check()?;
        if self.wanted() > 0 {
            return Ok(false);
        }
        let reply = std::mem::take(&mut self.input);
        match self.stage {
            Stage::Choice => match reply[1] {
                NO_AUTH => self.ask(out),
                // Even if we offered none, as libzmq.
                BASIC_AUTH => {
                    out.extend_from_slice(&self.auth);
                    self.stage = Stage::AuthResponse;
                }
                _ => return Err(SocksError::NoAcceptableMethod),
            },
            Stage::AuthResponse => {
                if reply[1] != 0 {
                    return Err(SocksError::AuthenticationFailed);
                }
                self.ask(out);
            }
            Stage::Reply => {
                if reply[1] != 0 {
                    return Err(SocksError::Refused(reply[1]));
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn ask(&mut self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.request);
        self.stage = Stage::Reply;
    }

    /// Refuses a reply as soon as an octet of it is wrong, before its
    /// length is taken from it.
    fn check(&self) -> Result<(), SocksError> {
        let input = &self.input;
        let version = match self.stage {
            Stage::Choice | Stage::Reply => VERSION,
            Stage::AuthResponse => AUTH_VERSION,
        };
        if input.first().is_some_and(|&octet| octet != version) {
            return Err(SocksError::Malformed);
        }
        if self.stage == Stage::Reply {
            if input.get(2).is_some_and(|&reserved| reserved != 0) {
                return Err(SocksError::Malformed);
            }
            if input
                .get(3)
                .is_some_and(|atyp| ![IPV4, DOMAIN, IPV6].contains(atyp))
            {
                return Err(SocksError::Malformed);
            }
        }
        Ok(())
    }
}

/// The length of a reply whose address type is `atyp`, `first` being the
/// octet after it: a domain name's length.
fn reply_len(atyp: u8, first: u8) -> usize {
    let address = match atyp {
        IPV4 => 4,
        IPV6 => 16,
        _ => 1 + usize::from(first),
    };
    4 + address + 2
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proxy(username: &str, password: &str) -> SocksProxy {
        SocksProxy {
            host: "proxy".to_owned(),
            port: 1080,
            source: None,
            username: Credential::new(username).unwrap(),
            password: Credential::new(password).unwrap(),
        }
    }

    /// Feeds `reply` as the stream would give it: no more than is wanted.
    fn feed(client: &mut SocksClient, reply: &[u8]) -> Result<(bool, Vec<u8>), SocksError> {
        let mut out = Vec::new();
        let mut done = false;
        let mut rest = reply;
        while !rest.is_empty() {
            assert!(!done, "octets after the proxy connected us");
            let (now, later) = rest.split_at(client.wanted().min(rest.len()));
            done = client.feed(now, &mut out)?;
            rest = later;
        }
        Ok((done, out))
    }

    #[test]
    fn an_address_is_asked_for_without_authentication() {
        let mut client = SocksClient::new(&proxy("", ""), "127.0.0.1", 5555).unwrap();
        let mut greeting = Vec::new();
        client.greet(&mut greeting);
        assert_eq!(greeting, [5, 1, 0]);

        let (done, request) = feed(&mut client, &[5, 0]).unwrap();
        assert!(!done);
        assert_eq!(request, [5, 1, 0, 1, 127, 0, 0, 1, 0x15, 0xb3]);

        let (done, out) = feed(&mut client, &[5, 0, 0, 1, 10, 0, 0, 1, 0x1f, 0x90]).unwrap();
        assert!(done);
        assert!(out.is_empty());
    }

    #[test]
    fn a_host_name_is_left_for_the_proxy_to_resolve() {
        let mut client = SocksClient::new(&proxy("", ""), "example.org", 80).unwrap();
        let (_, request) = feed(&mut client, &[5, 0]).unwrap();
        let mut expected = vec![5, 1, 0, 3, 11];
        expected.extend_from_slice(b"example.org");
        expected.extend_from_slice(&[0, 80]);
        assert_eq!(request, expected);
    }

    #[test]
    fn an_ipv6_address_is_asked_for_as_one() {
        let mut client = SocksClient::new(&proxy("", ""), "::1", 80).unwrap();
        let (_, request) = feed(&mut client, &[5, 0]).unwrap();
        let mut expected = vec![5, 1, 0, 4];
        expected.extend_from_slice(&[0; 15]);
        expected.extend_from_slice(&[1, 0, 80]);
        assert_eq!(request, expected);
    }

    #[test]
    fn a_user_name_offers_only_basic_authentication() {
        let mut client = SocksClient::new(&proxy("user", "pass"), "127.0.0.1", 80).unwrap();
        let mut greeting = Vec::new();
        client.greet(&mut greeting);
        assert_eq!(greeting, [5, 1, 2]);

        let (done, auth) = feed(&mut client, &[5, 2]).unwrap();
        assert!(!done);
        assert_eq!(auth, b"\x01\x04user\x04pass");

        let (done, request) = feed(&mut client, &[1, 0]).unwrap();
        assert!(!done);
        assert_eq!(request[..4], [5, 1, 0, 1]);
    }

    #[test]
    fn a_proxy_asking_for_credentials_we_do_not_have_is_sent_empty_ones() {
        let mut client = SocksClient::new(&proxy("", ""), "127.0.0.1", 80).unwrap();
        let (_, auth) = feed(&mut client, &[5, 2]).unwrap();
        assert_eq!(auth, [1, 0, 0]);
    }

    #[test]
    fn replies_of_every_address_type_are_read_to_their_end() {
        let mut domain = vec![5, 0, 0, 3, 9];
        domain.extend_from_slice(b"localhost");
        domain.extend_from_slice(&[0, 80]);
        let mut ipv6 = vec![5, 0, 0, 4];
        ipv6.extend_from_slice(&[0; 18]);
        for reply in [domain, ipv6] {
            let mut client = SocksClient::new(&proxy("", ""), "127.0.0.1", 80).unwrap();
            feed(&mut client, &[5, 0]).unwrap();
            for octet in &reply[..reply.len() - 1] {
                assert_eq!(client.feed(&[*octet], &mut Vec::new()), Ok(false));
            }
            assert_eq!(client.wanted(), 1);
            let last = &reply[reply.len() - 1..];
            assert_eq!(client.feed(last, &mut Vec::new()), Ok(true));
        }
    }

    #[test]
    fn the_proxys_refusals_end_the_negotiation() {
        let refused = |replies: &[&[u8]]| {
            let mut client = SocksClient::new(&proxy("user", "pass"), "127.0.0.1", 80).unwrap();
            let mut last = Ok((false, Vec::new()));
            for reply in replies {
                last = feed(&mut client, reply);
            }
            last.unwrap_err()
        };
        assert_eq!(refused(&[&[5, 0xff]]), SocksError::NoAcceptableMethod);
        assert_eq!(
            refused(&[&[5, 2], &[1, 1]]),
            SocksError::AuthenticationFailed
        );
        assert_eq!(
            refused(&[&[5, 2], &[1, 0], &[5, 5, 0, 1, 0, 0, 0, 0, 0, 0]]),
            SocksError::Refused(5)
        );
    }

    #[test]
    fn replies_that_are_not_socks5_are_refused_at_the_first_wrong_octet() {
        let malformed = |replies: &[&[u8]]| {
            let mut client = SocksClient::new(&proxy("user", "pass"), "127.0.0.1", 80).unwrap();
            let mut last = Ok((false, Vec::new()));
            for reply in replies {
                last = feed(&mut client, reply);
            }
            assert_eq!(last, Err(SocksError::Malformed), "{replies:?}");
        };
        malformed(&[&[4]]);
        malformed(&[&[5, 2], &[5]]);
        malformed(&[&[5, 2], &[1, 0], &[4]]);
        malformed(&[&[5, 2], &[1, 0], &[5, 0, 1]]);
        malformed(&[&[5, 2], &[1, 0], &[5, 0, 0, 2]]);
    }

    #[test]
    fn what_cannot_be_asked_is_refused_before_connecting() {
        let long = "a".repeat(256);
        let fits = "a".repeat(255);
        assert!(SocksClient::new(&proxy(&fits, &fits), &fits, 80).is_ok());
        for (host, port) in [(long.as_str(), 80), ("host", 0)] {
            assert_eq!(
                SocksClient::new(&proxy("", ""), host, port).unwrap_err(),
                SocksError::Unaskable
            );
        }
    }
}
