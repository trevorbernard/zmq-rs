//! `ipc://` against libzmq, including PLAIN over a Unix domain socket.
#![cfg(unix)]

use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{
    Context, Credential, Endpoint, Frame, Message, PlainCredentials, Pull, Push, Security,
    SocketOptions, ZapReply, ZapRequest,
};
use zmq_interop::libzmq::{Kind, Socket};

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

fn socket_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("zmq-interop-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    let _ = std::fs::remove_file(&path);
    path
}

#[test]
fn our_push_reaches_a_libzmq_pull_over_ipc() {
    let path = socket_path("to-libzmq");
    let pull = Socket::new(Kind::Pull);
    pull.set_receive_timeout(FIVE);
    pull.bind(&format!("ipc://{}", path.display()));
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&format!("ipc://{}", path.display())).unwrap();
    push.send("hello libzmq");
    assert_eq!(pull.recv().unwrap(), [b"hello libzmq"]);
}

#[test]
fn a_libzmq_push_reaches_our_pull_over_ipc() {
    let path = socket_path("from-libzmq");
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind(&format!("ipc://{}", path.display())).unwrap();
    let push = Socket::new(Kind::Push);
    push.connect(bound);
    push.send(&[b"hello us"]).unwrap();
    assert_eq!(within(FIVE, move || pull.recv()), Message::from("hello us"));
}

#[test]
fn a_libzmq_push_finds_our_wildcard_path() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("ipc://*").unwrap();
    assert!(matches!(bound, Endpoint::Ipc(_)));
    let push = Socket::new(Kind::Push);
    push.connect(bound);
    push.send(&[b"found"]).unwrap();
    assert_eq!(within(FIVE, move || pull.recv()), Message::from("found"));
}

#[test]
fn plain_over_ipc_reports_an_empty_zap_address() {
    let path = socket_path("plain");
    let (seen, address) = mpsc::channel();
    let server = SocketOptions {
        security: Security::plain_server("", move |request: &ZapRequest| {
            let _ = seen.send(request.address.clone());
            ZapReply::Ok {
                user_id: Frame::from("user"),
                metadata: zmtp::Metadata::new(),
            }
        }),
        ..SocketOptions::default()
    };
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(server);
    pull.bind(&format!("ipc://{}", path.display())).unwrap();
    let client = SocketOptions {
        security: Security::PlainClient(PlainCredentials {
            username: Credential::new("user").unwrap(),
            password: Credential::new("secret").unwrap(),
        }),
        ..SocketOptions::default()
    };
    let mut push = context.socket_with::<Push>(client);
    push.connect(&format!("ipc://{}", path.display())).unwrap();
    push.send("authenticated");
    assert_eq!(
        within(FIVE, move || pull.recv()),
        Message::from("authenticated")
    );
    assert_eq!(address.recv_timeout(FIVE).unwrap(), "", "as libzmq for ipc");
}
