//! libzmq's `tests/test_term_endpoint.cpp`: `zmq_unbind` and
//! `zmq_disconnect`, as zmq-rs's `Socket::unbind` and `Socket::disconnect`.

use zmq::{Context, Endpoint, Error, Message, Pull, Push};

use crate::support::{FIVE, settle, within};

/// Passes one message from `push` to `pull`, so the connection is up.
fn pass_one(
    push: zmq::Socket<Push>,
    pull: zmq::Socket<Pull>,
) -> (zmq::Socket<Push>, zmq::Socket<Pull>) {
    within(FIVE, move || {
        let (mut push, mut pull) = (push, pull);
        push.send("ABC");
        assert_eq!(pull.recv(), Message::from("ABC"));
        (push, pull)
    })
}

#[test]
fn send_after_unbind_fails() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    let endpoint = push.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut pull = context.socket::<Pull>();
    pull.connect(&endpoint).unwrap();
    let (mut push, _pull) = pass_one(push, pull);

    push.unbind(&endpoint).unwrap();
    settle();
    assert!(push.try_send("ABC").is_err(), "no peer is left");
}

#[test]
fn send_after_disconnect_fails() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let endpoint = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut push = context.socket::<Push>();
    push.connect(&endpoint).unwrap();
    let (mut push, _pull) = pass_one(push, pull);

    push.disconnect(&endpoint).unwrap();
    settle();
    assert!(push.try_send("ABC").is_err(), "no peer is left");
}

#[cfg(unix)]
#[test]
fn unbind_via_last_endpoint() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    let tcp = push.bind("tcp://127.0.0.1:*").unwrap();
    let mut pull = context.socket::<Pull>();
    let ipc = pull.bind("ipc://*").unwrap();
    assert!(matches!(ipc, Endpoint::Ipc(_)));

    push.unbind(&tcp.to_string()).unwrap();
    pull.unbind(&ipc.to_string()).unwrap();
}

#[cfg(unix)]
#[test]
fn wildcard_unbind_fails() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    push.bind("tcp://127.0.0.1:*").unwrap();
    let mut pull = context.socket::<Pull>();
    pull.bind("ipc://*").unwrap();

    // libzmq's ENOENT: a wildcard names no bind; only the address it chose
    // does.
    assert!(matches!(
        push.unbind("tcp://127.0.0.1:*"),
        Err(Error::UnknownEndpoint(_))
    ));
    assert!(matches!(
        pull.unbind("ipc://*"),
        Err(Error::UnknownEndpoint(_))
    ));
}
