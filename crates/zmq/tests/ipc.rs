//! The `ipc://` transport: Unix domain sockets named by a path.
#![cfg(unix)]

use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use zmq::{Context, Endpoint, Error, Message, Pull, Push};

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

/// A fresh path for a socket, short enough for `sun_path`.
fn socket_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("zmq-rs-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    let _ = std::fs::remove_file(&path);
    path
}

#[test]
fn push_and_pull_over_ipc() {
    let path = socket_path("pipeline");
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind(&format!("ipc://{}", path.display())).unwrap();
    assert_eq!(bound, Endpoint::Ipc(path.clone()));
    assert_eq!(bound.to_string(), format!("ipc://{}", path.display()));
    let mut push = context.socket::<Push>();
    push.connect(&bound.to_string()).unwrap();
    push.send("over a unix socket");
    assert_eq!(
        within(FIVE, move || pull.recv()),
        Message::from("over a unix socket")
    );
}

#[test]
fn the_socket_file_goes_when_the_socket_does() {
    let path = socket_path("tidy");
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.bind(&format!("ipc://{}", path.display())).unwrap();
    assert!(path.exists());
    drop(pull);
    let started = std::time::Instant::now();
    while path.exists() {
        assert!(started.elapsed() < FIVE, "the file was left behind");
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn a_wildcard_binds_a_fresh_path() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("ipc://*").unwrap();
    let Endpoint::Ipc(path) = bound.clone() else {
        panic!("not an ipc endpoint: {bound}");
    };
    assert!(path.exists(), "{path:?}");
    let mut push = context.socket::<Push>();
    push.connect(&bound.to_string()).unwrap();
    push.send("found it");
    let pull = within(FIVE, move || {
        assert_eq!(pull.recv(), Message::from("found it"));
        pull
    });
    drop(pull);
    let dir = path.parent().unwrap().to_owned();
    let started = std::time::Instant::now();
    while path.exists() || dir.exists() {
        assert!(
            started.elapsed() < FIVE,
            "the temporary file or directory was left behind"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn a_path_in_use_cannot_be_bound() {
    let path = socket_path("in-use");
    let context = Context::new().unwrap();
    let mut first = context.socket::<Pull>();
    first.bind(&format!("ipc://{}", path.display())).unwrap();
    let mut second = context.socket::<Pull>();
    assert!(matches!(
        second.bind(&format!("ipc://{}", path.display())),
        Err(Error::Io(err)) if err.kind() == std::io::ErrorKind::AddrInUse
    ));
}

#[test]
fn a_stale_socket_file_is_replaced() {
    let path = socket_path("stale");
    // A socket file whose owner has gone, as a crashed process leaves it.
    drop(UnixListener::bind(&path).unwrap());
    assert!(path.exists());
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.bind(&format!("ipc://{}", path.display())).unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&format!("ipc://{}", path.display())).unwrap();
    push.send("fresh");
    assert_eq!(within(FIVE, move || pull.recv()), Message::from("fresh"));
}

#[test]
fn an_ordinary_file_is_never_replaced() {
    let path = socket_path("precious");
    std::fs::write(&path, "keep me").unwrap();
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    assert!(matches!(
        pull.bind(&format!("ipc://{}", path.display())),
        Err(Error::Io(err)) if err.kind() == std::io::ErrorKind::AddrInUse
    ));
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "keep me");
}

#[test]
fn connecting_before_binding_retries_until_it_works() {
    let path = socket_path("later");
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&format!("ipc://{}", path.display())).unwrap();
    push.send("queued");
    thread::sleep(Duration::from_millis(50));
    let mut pull = context.socket::<Pull>();
    pull.bind(&format!("ipc://{}", path.display())).unwrap();
    assert_eq!(within(FIVE, move || pull.recv()), Message::from("queued"));
}

#[test]
fn an_empty_path_is_invalid() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    assert!(matches!(
        pull.bind("ipc://"),
        Err(Error::InvalidEndpoint(_))
    ));
}

#[test]
fn a_connection_comes_back_when_the_binder_does() {
    let path = socket_path("restart");
    let endpoint = format!("ipc://{}", path.display());
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.bind(&endpoint).unwrap();
    let mut push = context.socket::<Push>();
    push.connect(&endpoint).unwrap();
    push.send("before");
    let pull = within(FIVE, move || {
        assert_eq!(pull.recv(), Message::from("before"));
        pull
    });
    drop(pull);
    thread::sleep(Duration::from_millis(50));
    let mut pull = context.socket::<Pull>();
    pull.bind(&endpoint).unwrap();
    push.send("after");
    assert_eq!(within(FIVE, move || pull.recv()), Message::from("after"));
}

#[test]
fn a_replaced_socket_file_is_left_alone() {
    let path = socket_path("replaced");
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    pull.bind(&format!("ipc://{}", path.display())).unwrap();
    // Someone else takes the path over while we are still bound.
    std::fs::remove_file(&path).unwrap();
    let _theirs = UnixListener::bind(&path).unwrap();
    drop(pull);
    thread::sleep(Duration::from_millis(200));
    assert!(path.exists(), "their socket file was removed");
}
