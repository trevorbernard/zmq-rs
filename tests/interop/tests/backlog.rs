//! The backlog a listener asks the system for, read back with `ss`, the
//! only place Linux shows it.
#![cfg(target_os = "linux")]

use std::process::Command;

use zmq::{Context, Pull, SocketOptions};
use zmq_interop::libzmq::{Kind, Socket};

/// libzmq's default `ZMQ_BACKLOG`.
const DEFAULT: u32 = 100;

/// The backlog of the TCP listener on `port`: `ss`'s Send-Q column.
fn backlog(port: u16) -> Option<u32> {
    let output = Command::new("ss")
        .args(["-ltnH", &format!("sport = :{port}")])
        .output()
        .ok()?;
    let line = String::from_utf8(output.stdout).ok()?;
    line.split_whitespace().nth(2)?.parse().ok()
}

fn have_ss() -> bool {
    let have = Command::new("ss").arg("-V").output().is_ok();
    if !have {
        zmq_interop::skip("no `ss` to read a listener's backlog with");
    }
    have
}

#[test]
fn libzmq_listens_with_a_backlog_of_100() {
    if !have_ss() {
        return;
    }
    let pull = Socket::new(Kind::Pull);
    let addr = pull.bind_loopback();
    assert_eq!(backlog(addr.port()), Some(DEFAULT));
}

#[test]
fn we_listen_with_a_backlog_of_100() {
    if !have_ss() {
        return;
    }
    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>().unwrap();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap();
    assert_eq!(backlog(bound.tcp_addr().unwrap().port()), Some(DEFAULT));

    // With a type of service the listener is made another way.
    let mut tuned = context
        .socket_with::<Pull>(SocketOptions::new().type_of_service(0x10))
        .unwrap();
    let bound = tuned.bind("tcp://127.0.0.1:*").unwrap();
    assert_eq!(backlog(bound.tcp_addr().unwrap().port()), Some(DEFAULT));

    let mut asked = context
        .socket_with::<Pull>(SocketOptions::new().backlog(7))
        .unwrap();
    let bound = asked.bind("tcp://127.0.0.1:*").unwrap();
    assert_eq!(backlog(bound.tcp_addr().unwrap().port()), Some(7));
}

/// The backlog of the `ipc` listener at `path`.
fn ipc_backlog(path: &str) -> Option<u32> {
    let output = Command::new("ss").args(["-lxH"]).output().ok()?;
    let lines = String::from_utf8(output.stdout).ok()?;
    let line = lines.lines().find(|line| line.contains(path))?;
    line.split_whitespace().nth(3)?.parse().ok()
}

#[test]
fn ipc_listeners_have_the_same_backlog() {
    if !have_ss() {
        return;
    }
    let theirs = Socket::new(Kind::Pull);
    let path = std::env::temp_dir().join(format!("zmq-backlog-{}", std::process::id()));
    let path = path.display().to_string();
    theirs.bind(&format!("ipc://{path}"));
    assert_eq!(ipc_backlog(&path), Some(DEFAULT), "libzmq");

    let context = Context::new().unwrap();
    let mut ours = context.socket::<Pull>().unwrap();
    let bound = ours.bind("ipc://*").unwrap().to_string();
    let path = bound.strip_prefix("ipc://").unwrap();
    assert_eq!(ipc_backlog(path), Some(DEFAULT), "ours");
}
