//! libzmq's `tests/test_socks.cpp`: `ZMQ_SOCKS_PROXY`, a PULL reaching a
//! PUSH through a SOCKS5 proxy, here a small one of the test's own as in
//! libzmq's, by address, by a name only the proxy resolves, and with a
//! user name and password.
//!
//! libzmq's option tests set and read back strings. Ours are typed: an
//! unset proxy is `None`, and a user name or password is a `Credential`,
//! whose limit of 255 octets is the one libzmq checks.

use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use zmq::{Context, Error, Linger, Message, Pull, Push, Socket, SocketOptions, SocksProxy};
use zmq_test_ports::free_endpoint;

use crate::support::FIVE;

/// A SOCKS5 proxy serving one client, as libzmq's `socks_server_task`. It
/// resolves `somedomainname.org` and `::1` to the IPv4 loopback address,
/// and checks the credentials if a user name is given.
struct SocksServer {
    address: String,
    task: JoinHandle<()>,
}

impl SocksServer {
    fn start(credentials: Option<(&'static str, &'static str)>, delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let task = thread::spawn(move || {
            thread::sleep(delay);
            let (client, _) = listener.accept().unwrap();
            serve(client, credentials);
        });
        Self { address, task }
    }

    /// Waits for the proxy to finish, once its client has gone, so that
    /// what it asserted is checked.
    fn join(self) {
        self.task.join().unwrap();
    }
}

fn read_exact<const N: usize>(stream: &mut TcpStream) -> [u8; N] {
    let mut buf = [0; N];
    stream.read_exact(&mut buf).unwrap();
    buf
}

fn read_short_string(stream: &mut TcpStream) -> Vec<u8> {
    let [len] = read_exact(stream);
    let mut string = vec![0; usize::from(len)];
    stream.read_exact(&mut string).unwrap();
    string
}

fn serve(mut client: TcpStream, credentials: Option<(&str, &str)>) {
    let auth_method = if credentials.is_some() { 2 } else { 0 };

    // Greetings [version, nmethods, methods...].
    let [version, nmethods] = read_exact(&mut client);
    assert_eq!(version, 5);
    let mut methods = vec![0; usize::from(nmethods)];
    client.read_exact(&mut methods).unwrap();
    let method = if methods.contains(&auth_method) {
        auth_method
    } else {
        0xff
    };
    client.write_all(&[5, method]).unwrap();
    if method == 0xff {
        return;
    }

    if let Some((username, password)) = credentials {
        let [version] = read_exact(&mut client);
        let given = (version == 1).then(|| {
            (
                read_short_string(&mut client),
                read_short_string(&mut client),
            )
        });
        let ok = given
            .is_some_and(|(user, pass)| user == username.as_bytes() && pass == password.as_bytes());
        client.write_all(&[1, u8::from(!ok)]).unwrap();
        if !ok {
            return;
        }
    }

    // Request [version, cmd, rsv, atype, dst.addr, dst.port].
    let [version, command, reserved, atype] = read_exact(&mut client);
    assert_eq!((version, command, reserved), (5, 1, 0));
    let resolved = match atype {
        1 => Some(Ipv4Addr::from(read_exact::<4>(&mut client))),
        3 => {
            (read_short_string(&mut client) == b"somedomainname.org").then_some(Ipv4Addr::LOCALHOST)
        }
        4 => (Ipv6Addr::from(read_exact::<16>(&mut client)) == Ipv6Addr::LOCALHOST)
            .then_some(Ipv4Addr::LOCALHOST),
        other => panic!("address type {other}"),
    };
    let port = u16::from_be_bytes(read_exact(&mut client));
    let remote = match resolved {
        // Host unreachable.
        None => Err(4),
        // Connection refused.
        Some(ip) => TcpStream::connect((ip, port)).map_err(|_| 5),
    };

    let mut reply = vec![5, remote.as_ref().err().copied().unwrap_or(0), 0, atype];
    let bound = remote
        .as_ref()
        .ok()
        .and_then(|remote| remote.local_addr().ok());
    match atype {
        1 => match bound {
            Some(SocketAddr::V4(bound)) => reply.extend_from_slice(&bound.ip().octets()),
            _ => reply.extend_from_slice(&[0; 4]),
        },
        // Just to test a reply with a host name.
        3 => {
            reply.push(9);
            reply.extend_from_slice(b"localhost");
        }
        _ => reply.extend_from_slice(&Ipv6Addr::LOCALHOST.octets()),
    }
    reply.extend_from_slice(&bound.map_or(0, |bound| bound.port()).to_be_bytes());
    client.write_all(&reply).unwrap();
    let Ok(remote) = remote else {
        return;
    };

    // Relays until either side closes.
    let relay = |mut from: TcpStream, mut to: TcpStream| {
        thread::spawn(move || {
            let _ = io::copy(&mut from, &mut to);
            let _ = from.shutdown(Shutdown::Both);
            let _ = to.shutdown(Shutdown::Both);
        })
    };
    let upstream = relay(client.try_clone().unwrap(), remote.try_clone().unwrap());
    let downstream = relay(remote, client);
    upstream.join().unwrap();
    downstream.join().unwrap();
}

fn zero_linger() -> SocketOptions {
    SocketOptions {
        linger: Some(Linger::For(Duration::ZERO)),
        ..SocketOptions::default()
    }
}

/// A PUSH bound to a loopback port, and the endpoint to reach it at.
fn setup_push_server(context: &Context) -> (Socket<Push>, String) {
    let mut push = context.socket_with::<Push>(zero_linger());
    let connect_address = push.bind(&free_endpoint()).unwrap().to_string();
    (push, connect_address)
}

fn setup_pull_client(
    context: &Context,
    connect_address: &str,
    socks_proxy: Option<SocksProxy>,
) -> Socket<Pull> {
    let mut pull = context.socket_with::<Pull>(SocketOptions {
        socks_proxy,
        ..zero_linger()
    });
    pull.connect(connect_address).unwrap();
    pull
}

fn communicate(push: &mut Socket<Push>, pull: &mut Socket<Pull>) {
    push.send_timeout("ABC", FIVE).unwrap();
    push.send_timeout("DEF", FIVE).unwrap();
    for expected in ["ABC", "DEF"] {
        let message = pull.recv_timeout(FIVE).unwrap().expect("a message");
        assert_eq!(message, Message::from(expected));
    }
}

/// `connect_address` with its host replaced, keeping the port.
fn with_host(connect_address: &str, host: &str) -> String {
    let (_, port) = connect_address.rsplit_once(':').expect("a port");
    format!("tcp://{host}:{port}")
}

/// A round trip through a proxy asked for `host` in place of the PUSH's
/// address, reached as `proxy` makes of its address.
fn through_proxy(server: SocksServer, host: Option<&str>, proxy: impl FnOnce(&str) -> SocksProxy) {
    let context = Context::new().unwrap();
    let (mut push, connect_address) = setup_push_server(&context);
    let connect_address = match host {
        Some(host) => with_host(&connect_address, host),
        None => connect_address,
    };
    let mut pull = setup_pull_client(&context, &connect_address, Some(proxy(&server.address)));
    communicate(&mut push, &mut pull);
    drop(push);
    drop(pull);
    server.join();
}

fn plain(address: &str) -> SocksProxy {
    SocksProxy::new(address).unwrap()
}

const NO_DELAY: Duration = Duration::ZERO;
const DELAY: Duration = Duration::from_millis(300);

#[test]
fn socks_proxy_options() {
    assert_eq!(SocketOptions::default().socks_proxy, None);
    SocksProxy::new("somehost:1080").unwrap();
    // Empty value not allowed for proxy server
    assert!(matches!(
        SocksProxy::new(""),
        Err(Error::InvalidEndpoint(_))
    ));
}

#[cfg(feature = "draft")]
#[test]
fn socks_userpass_options() {
    use zmq::Credential;

    let buffer: Vec<u8> = (0..1024).map(|i| b'a' + (i % 26) as u8).collect();
    for _option in ["username", "password"] {
        // Empty value is allowed: for the user name, no authentication.
        Credential::new("").unwrap();
        Credential::new("someuser").unwrap();
        Credential::new(&buffer[..255]).unwrap();
        assert!(Credential::new(&buffer[..256]).is_err());
    }
}

#[test]
fn socks_no_socks() {
    let context = Context::new().unwrap();
    let (mut push, connect_address) = setup_push_server(&context);
    let mut pull = setup_pull_client(&context, &connect_address, None);
    communicate(&mut push, &mut pull);
}

#[test]
fn socks() {
    through_proxy(SocksServer::start(None, NO_DELAY), None, plain);
}

#[test]
fn socks_delay() {
    through_proxy(SocksServer::start(None, DELAY), None, plain);
}

#[test]
fn socks_domainname() {
    // Will be resolved by the proxy test server to 127.0.0.1
    let host = Some("somedomainname.org");
    through_proxy(SocksServer::start(None, NO_DELAY), host, plain);
}

#[test]
fn socks_ipv6() {
    // Will be resolved by the proxy test server to 127.0.0.1
    through_proxy(SocksServer::start(None, NO_DELAY), Some("::1"), plain);
}

#[test]
fn socks_ipv6_sb() {
    // Will be resolved by the proxy test server to 127.0.0.1
    through_proxy(SocksServer::start(None, NO_DELAY), Some("[::1]"), plain);
}

#[test]
fn socks_bind_before_connect() {
    // Will do a bind before connect when connecting to proxy
    through_proxy(SocksServer::start(None, NO_DELAY), None, |address| {
        SocksProxy::new(&format!("127.0.0.1:0;{address}")).unwrap()
    });
}

#[cfg(feature = "draft")]
mod basic_auth {
    use zmq::Credential;

    use super::*;

    fn with_auth(
        username: &'static str,
        password: &'static str,
    ) -> impl FnOnce(&str) -> SocksProxy {
        move |address| {
            plain(address).with_credentials(
                Credential::new(username).unwrap(),
                Credential::new(password).unwrap(),
            )
        }
    }

    const SOMEUSER: Option<(&str, &str)> = Some(("someuser", "somepass"));
    const NO_PASS: Option<(&str, &str)> = Some(("someuser", ""));

    #[test]
    fn socks_basic_auth() {
        let server = SocksServer::start(SOMEUSER, NO_DELAY);
        through_proxy(server, None, with_auth("someuser", "somepass"));
    }

    #[test]
    fn socks_basic_auth_delay() {
        let server = SocksServer::start(SOMEUSER, DELAY);
        through_proxy(server, None, with_auth("someuser", "somepass"));
    }

    /// libzmq's `empty_user` and `null_user`, which a typed option does not
    /// tell apart: no user name is no authentication.
    #[test]
    fn socks_basic_auth_empty_user() {
        let server = SocksServer::start(None, NO_DELAY);
        through_proxy(server, None, with_auth("", ""));
    }

    /// libzmq's `empty_pass` and `null_pass`.
    #[test]
    fn socks_basic_auth_empty_pass() {
        let server = SocksServer::start(NO_PASS, NO_DELAY);
        through_proxy(server, None, with_auth("someuser", ""));
    }
}
