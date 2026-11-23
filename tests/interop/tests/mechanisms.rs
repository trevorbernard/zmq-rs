//! PLAIN and CURVE between our sockets and libzmq's: either as the server,
//! with either binding, over every transport. Whichever is the server asks
//! its ZAP handler, which must see what the client offered.

use std::sync::mpsc;
use std::time::Duration;

use zmq::{
    Context, Credential, Frame, Keypair, Message, Metadata, Pair, PlainCredentials, Security,
    SocketOptions, ZapCredentials, ZapReply, ZapRequest,
};
use zmq_interop::libzmq::{self, Kind, Socket, zap_handler};
use zmq_interop::link::{Link, links};

const FIVE: Duration = Duration::from_secs(5);

fn credentials() -> PlainCredentials {
    PlainCredentials {
        username: Credential::new("admin").unwrap(),
        password: Credential::new("hunter2").unwrap(),
    }
}

/// An authenticator of ours that accepts everyone, and what it was shown.
fn accept_all() -> (
    impl Fn(&ZapRequest) -> ZapReply + Send + Sync + 'static,
    mpsc::Receiver<ZapCredentials>,
) {
    let (seen, shown) = mpsc::channel();
    let authenticator = move |request: &ZapRequest| {
        let _ = seen.send(request.credentials.clone());
        ZapReply::Ok {
            user_id: Frame::from("someone"),
            metadata: Metadata::new(),
        }
    };
    (authenticator, shown)
}

/// Joins a PAIR of ours with `security` to `theirs`, and has each send
/// the other a message.
fn exchange(link: Link, security: Security, theirs: &Socket) {
    let context = Context::new().unwrap();
    let mut ours = context
        .socket_with::<Pair>(SocketOptions::new().security(security))
        .unwrap();
    link.join(&mut ours, theirs);
    ours.send("from ours").unwrap();
    assert_eq!(theirs.recv().unwrap(), [b"from ours"], "{link:?}");
    theirs.send(&[b"from libzmq"]).unwrap();
    assert_eq!(
        ours.recv_timeout(FIVE).unwrap(),
        Some(Message::from("from libzmq")),
        "{link:?}"
    );
}

/// The mechanism and credentials frames of the request libzmq's ZAP
/// handler was sent (RFC 27).
fn offered(request: &[Vec<u8>]) -> &[Vec<u8>] {
    &request[5..]
}

#[test]
fn plain_with_ours_as_the_server() {
    let links = links();
    for link in links.iter() {
        let (authenticator, shown) = accept_all();
        let theirs = link.theirs(Kind::Pair);
        theirs.set_plain_client(b"admin", b"hunter2");
        exchange(
            link,
            Security::plain_server("global", authenticator),
            &theirs,
        );
        assert_eq!(
            shown.try_recv(),
            Ok(ZapCredentials::Plain(credentials())),
            "{link:?}"
        );
    }
}

#[test]
fn plain_with_libzmq_as_the_server() {
    let links = links();
    for link in links.iter() {
        let context = libzmq::Context::new();
        let zap = zap_handler(&context, "200", "someone");
        let theirs = link.theirs_in(&context, Kind::Pair);
        theirs.set_plain_server();
        theirs.set_zap_domain(b"global");
        exchange(link, Security::PlainClient(credentials()), &theirs);
        assert_eq!(
            offered(&zap.join().unwrap()),
            [&b"PLAIN"[..], b"admin", b"hunter2"],
            "{link:?}"
        );
    }
}

#[test]
fn curve_with_ours_as_the_server() {
    let links = links();
    for link in links.iter() {
        let (server, client) = (Keypair::generate(), Keypair::generate());
        let (authenticator, shown) = accept_all();
        let theirs = link.theirs(Kind::Pair);
        theirs.set_curve_client(
            client.public().as_bytes(),
            client.secret().expose(),
            server.public().as_bytes(),
        );
        let security = Security::curve_server_authenticated(server, "global", authenticator);
        exchange(link, security, &theirs);
        assert_eq!(
            shown.try_recv(),
            Ok(ZapCredentials::Curve(client.public())),
            "{link:?}"
        );
    }
}

#[test]
fn curve_with_libzmq_as_the_server() {
    let links = links();
    for link in links.iter() {
        let (server, client) = (Keypair::generate(), Keypair::generate());
        let client_key = client.public();
        let context = libzmq::Context::new();
        let zap = zap_handler(&context, "200", "someone");
        let theirs = link.theirs_in(&context, Kind::Pair);
        theirs.set_curve_server(server.secret().expose());
        theirs.set_zap_domain(b"global");
        exchange(
            link,
            Security::curve_client(client, server.public()),
            &theirs,
        );
        assert_eq!(
            offered(&zap.join().unwrap()),
            [&b"CURVE"[..], client_key.as_bytes()],
            "{link:?}"
        );
    }
}
