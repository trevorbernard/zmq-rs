//! libzmq's `tests/test_term_endpoint.cpp`: `zmq_unbind` and
//! `zmq_disconnect`, as zmq-rs's `Socket::unbind` and `Socket::disconnect`.

use zmq::{Context, Endpoint, Error, Message, Pull, Push, TrySendError};

use crate::support::{FIVE, settle, within};

/// Passes one message from `push` to `pull`, so the connection is up.
fn pass_one(
    push: zmq::Socket<Push>,
    pull: zmq::Socket<Pull>,
) -> (zmq::Socket<Push>, zmq::Socket<Pull>) {
    within(FIVE, move || {
        let (mut push, mut pull) = (push, pull);
        push.send("ABC").unwrap();
        assert_eq!(pull.recv().unwrap(), Message::from("ABC"));
        (push, pull)
    })
}

#[test]
fn send_after_unbind_fails() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>().unwrap();
    let endpoint = push.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.connect(&endpoint).unwrap();
    let (mut push, _pull) = pass_one(push, pull);

    push.unbind(&endpoint).unwrap();
    settle();
    assert!(
        matches!(push.try_send("ABC"), Err(TrySendError::Full(_))),
        "no peer is left"
    );
}

#[test]
fn send_after_disconnect_fails() {
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    let endpoint = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut push = context.socket::<Push>().unwrap();
    push.connect(&endpoint).unwrap();
    let (mut push, _pull) = pass_one(push, pull);

    push.disconnect(&endpoint).unwrap();
    settle();
    assert!(
        matches!(push.try_send("ABC"), Err(TrySendError::Full(_))),
        "no peer is left"
    );
}

#[cfg(unix)]
#[test]
fn unbind_via_last_endpoint() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>().unwrap();
    let tcp = push.bind("tcp://127.0.0.1:*").unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    let ipc = pull.bind("ipc://*").unwrap();
    assert!(matches!(ipc, Endpoint::Ipc(_)));

    push.unbind(&tcp).unwrap();
    pull.unbind(&ipc).unwrap();
}

#[cfg(unix)]
#[test]
fn wildcard_unbind_fails() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.bind("tcp://127.0.0.1:*").unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
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
