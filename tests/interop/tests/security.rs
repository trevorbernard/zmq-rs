//! PLAIN (RFC 24) and authentication (RFC 27) on zmq-core connections,
//! against libzmq and against ourselves.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq_core::{
    Connection, ConnectionConfig, Context, Listener, Reconnect, Security, ZapResponder,
};
use zmq_interop::libzmq::{self, Kind, Socket};
use zmtp::{
    Credential, Frame, Identity, Message, Metadata, PlainCredentials, SocketType, ZapCredentials,
    ZapReply, ZapRequest,
};

const FIVE: Duration = Duration::from_secs(5);

/// Runs `f` on another thread and fails the test if it takes longer than
/// `limit`, so a lost wakeup fails instead of hanging the suite.
fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
        .recv_timeout(limit)
        .unwrap_or_else(|_| panic!("timed out after {limit:?}"))
}

fn credentials(password: &str) -> PlainCredentials {
    PlainCredentials {
        username: Credential::new("admin").unwrap(),
        password: Credential::new(password.to_owned()).unwrap(),
    }
}

fn plain_client(password: &str) -> ConnectionConfig {
    ConnectionConfig {
        identity: Identity::new("client").unwrap(),
        security: Security::PlainClient(credentials(password)),
        reconnect: Reconnect::Never,
        ..ConnectionConfig::new(SocketType::Dealer)
    }
}

fn accept_ok(user: &'static str) -> impl Fn(&ZapRequest) -> ZapReply + Send + Sync + 'static {
    move |request: &ZapRequest| {
        if request.credentials == ZapCredentials::Plain(credentials("hunter2")) {
            ZapReply::Ok {
                user_id: Frame::from(user),
                metadata: Metadata::new(),
            }
        } else {
            ZapReply::AuthenticationFailure
        }
    }
}

fn plain_server(security: Security) -> (Context, Listener) {
    let context = Context::new().unwrap();
    let config = ConnectionConfig {
        security,
        ..ConnectionConfig::new(SocketType::Router)
    };
    let listener = context
        .bind("127.0.0.1:0".parse().unwrap(), config)
        .unwrap();
    (context, listener)
}

fn recv(connection: Connection) -> (Connection, Option<Message>) {
    within(FIVE, move || {
        let mut connection = connection;
        let message = connection.rx.recv_blocking();
        (connection, message)
    })
}

#[test]
fn our_plain_client_authenticates_with_libzmq() {
    let context = libzmq::Context::new();
    let zap = libzmq::zap_handler(&context, "200", "user-1");
    let router = Socket::in_context(&context, Kind::Router);
    router.set_plain_server();
    let addr = router.bind_loopback();

    let ours = Context::new().unwrap();
    let mut connection = ours.connect(addr, plain_client("hunter2"));
    connection.tx.send_blocking(Message::from("hello")).unwrap();
    assert_eq!(router.recv().unwrap(), [&b"client"[..], b"hello"]);
    let request = zap.join().unwrap();
    assert_eq!(request[6..], [b"admin".to_vec(), b"hunter2".to_vec()]);
}

#[test]
fn our_plain_client_is_rejected_by_libzmq() {
    let context = libzmq::Context::new();
    let zap = libzmq::zap_handler(&context, "400", "");
    let router = Socket::in_context(&context, Kind::Router);
    router.set_plain_server();
    let addr = router.bind_loopback();

    let ours = Context::new().unwrap();
    let connection = ours.connect(addr, plain_client("wrong"));
    let (_connection, next) = recv(connection);
    assert_eq!(next, None, "a rejected connection closes for good");
    let request = zap.join().unwrap();
    assert_eq!(
        request[6..],
        [b"admin".to_vec(), b"wrong".to_vec()],
        "libzmq's ZAP handler judged our PLAIN credentials"
    );
}

#[test]
fn libzmq_plain_client_authenticates_with_us() {
    let (_context, mut listener) =
        plain_server(Security::plain_server("global", accept_ok("user-1")));
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_plain_client(b"admin", b"hunter2");
    dealer.connect(listener.local_addr());
    dealer.send(&[b"hello"]).unwrap();

    let connection = within(FIVE, move || listener.accept_blocking()).expect("accepted");
    let peer = connection.peer().unwrap();
    assert_eq!(
        peer.authenticated.map(|a| a.user_id),
        Some(Frame::from("user-1"))
    );
    let (_connection, first) = recv(connection);
    assert_eq!(first, Some(Message::from("hello")));
}

#[test]
fn the_authenticator_sees_the_request() {
    let (seen, requests) = mpsc::channel();
    let authenticator = move |request: &ZapRequest| {
        seen.send(request.clone()).unwrap();
        ZapReply::TemporaryFailure
    };
    let (_context, listener) = plain_server(Security::plain_server("global", authenticator));
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_plain_client(b"admin", b"hunter2");
    dealer.connect(listener.local_addr());

    let request = requests.recv_timeout(FIVE).unwrap();
    assert_eq!(request.domain, Frame::from("global"));
    assert_eq!(request.address, "127.0.0.1");
    assert_eq!(
        request.credentials,
        ZapCredentials::Plain(credentials("hunter2"))
    );
}

#[test]
fn rejected_clients_never_arrive() {
    let (asked, questions) = mpsc::channel();
    let judge = accept_ok("user");
    let authenticator = move |request: &ZapRequest| {
        asked.send(request.credentials.clone()).unwrap();
        judge(request)
    };
    let (_context, mut listener) = plain_server(Security::plain_server("", authenticator));
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_plain_client(b"admin", b"wrong");
    dealer.connect(listener.local_addr());
    assert_eq!(
        questions.recv_timeout(FIVE).unwrap(),
        ZapCredentials::Plain(credentials("wrong")),
        "the authenticator was asked"
    );
    thread::sleep(Duration::from_millis(200));
    assert!(
        listener.try_accept().is_err(),
        "the rejected client is never delivered"
    );
}

/// Answers from another thread, after a delay.
struct SlowAuthenticator;

impl zmq_core::Authenticator for SlowAuthenticator {
    fn authenticate(&self, _request: ZapRequest, respond: ZapResponder) {
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            respond.respond(ZapReply::Ok {
                user_id: Frame::from("slow"),
                metadata: Metadata::new(),
            });
        });
    }
}

#[test]
fn authenticators_may_answer_later_from_another_thread() {
    let (_context, mut listener) = plain_server(Security::plain_server("", SlowAuthenticator));
    let dealer = Socket::new(Kind::Dealer);
    dealer.set_plain_client(b"admin", b"hunter2");
    dealer.connect(listener.local_addr());
    let connection = within(FIVE, move || listener.accept_blocking()).expect("accepted");
    assert_eq!(
        connection.peer().unwrap().authenticated.map(|a| a.user_id),
        Some(Frame::from("slow"))
    );
}

/// Forgets to answer.
struct ForgetfulAuthenticator;

impl zmq_core::Authenticator for ForgetfulAuthenticator {
    fn authenticate(&self, _request: ZapRequest, respond: ZapResponder) {
        drop(respond);
    }
}

#[test]
fn an_unanswered_request_rejects_the_client() {
    let (_context, listener) = plain_server(Security::plain_server("", ForgetfulAuthenticator));
    let ours = Context::new().unwrap();
    let connection = ours.connect(listener.local_addr(), plain_client("hunter2"));
    let (_connection, next) = recv(connection);
    assert_eq!(next, None, "treated as an internal error (500)");
    drop(listener);
}

#[test]
fn our_plain_client_and_server_talk_to_each_other() {
    let (_context, mut listener) = plain_server(Security::plain_server("", accept_ok("me")));
    let ours = Context::new().unwrap();
    let mut client = ours.connect(listener.local_addr(), plain_client("hunter2"));
    client.tx.send_blocking(Message::from("hi")).unwrap();
    let server = within(FIVE, move || listener.accept_blocking()).expect("accepted");
    assert_eq!(
        server.peer().unwrap().authenticated.map(|a| a.user_id),
        Some(Frame::from("me")),
        "the client authenticated over PLAIN"
    );
    let (mut server, first) = recv(server);
    assert_eq!(first, Some(Message::from("hi")));
    server.tx.send_blocking(Message::from("hello")).unwrap();
    let (_client, reply) = recv(client);
    assert_eq!(reply, Some(Message::from("hello")));
}

#[test]
fn a_null_client_cannot_reach_a_plain_server() {
    let (_context, mut listener) = plain_server(Security::plain_server("", accept_ok("me")));
    let ours = Context::new().unwrap();
    let config = ConnectionConfig {
        reconnect: Reconnect::Never,
        ..ConnectionConfig::new(SocketType::Dealer)
    };
    let connection = ours.connect(listener.local_addr(), config);
    let (_connection, next) = recv(connection);
    assert_eq!(next, None, "mechanism mismatch");
    assert!(listener.try_accept().is_err());
}
