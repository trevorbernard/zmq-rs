//! libzmq's `tests/test_rebind_ipc.cpp`: once a socket bound to an `ipc`
//! path is closed, another can bind the same path, and a peer connected
//! to it reconnects.

#![cfg(unix)]

use std::env;
use std::process;

use zmq::{Context, Message, Pull, Push};

use crate::support::{FIVE, within};

#[test]
fn rebind_ipc() {
    let path = env::temp_dir().join(format!("zmq-rs-rebind-{}.ipc", process::id()));
    let endpoint = format!("ipc://{}", path.display());
    let context = Context::new().unwrap();
    let mut first = context.socket::<Push>();
    let mut second = context.socket::<Push>();
    first.bind(&endpoint).unwrap();
    let mut pull = context.socket::<Pull>();
    pull.connect(&endpoint).unwrap();

    first.send("42");
    let mut pull = within(FIVE, move || {
        assert_eq!(pull.recv(), Message::from("42"));
        pull
    });

    drop(first);
    second.bind(&endpoint).unwrap();
    within(FIVE, move || {
        // Waits for the pull to reconnect.
        second.send("42");
        assert_eq!(pull.recv(), Message::from("42"));
    });
}
