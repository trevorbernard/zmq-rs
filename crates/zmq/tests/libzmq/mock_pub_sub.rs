//! libzmq's `tests/test_mock_pub_sub.cpp`: a raw TCP peer speaks ZMTP to
//! an XPUB, or a SUB, by hand, subscribing (or being subscribed to) with
//! ZMTP 3.1's SUBSCRIBE command or ZMTP 3.0's `0x01`-prefixed message.
//!
//! libzmq reads the server's READY as exactly the octets its own READY
//! takes; this port reads it as a frame, whatever properties it carries.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use zmq::{Context, EventKind, Message, Monitor, Sub, XPub, XPubEvent};

use crate::support::{FIVE, within};

const GREETING_NULL: [u8; 64] = {
    let mut greeting = [0; 64];
    greeting[0] = 0xff;
    greeting[9] = 0x7f;
    greeting[10] = 3;
    greeting[12] = b'N';
    greeting[13] = b'U';
    greeting[14] = b'L';
    greeting[15] = b'L';
    greeting
};
const READY_XPUB: &[u8] = b"\x04\x1a\x05READY\x0bSocket-Type\x00\x00\x00\x04XPUB";
const READY_SUB: &[u8] = b"\x04\x19\x05READY\x0bSocket-Type\x00\x00\x00\x03SUB";
const COMMAND: u8 = 0x04;
const LONG: u8 = 0x02;

/// Reads one frame: its flags and body.
fn read_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut flags = [0];
    stream.read_exact(&mut flags).unwrap();
    let size = if flags[0] & LONG != 0 {
        let mut size = [0; 8];
        stream.read_exact(&mut size).unwrap();
        u64::from_be_bytes(size) as usize
    } else {
        let mut size = [0];
        stream.read_exact(&mut size).unwrap();
        usize::from(size[0])
    };
    let mut body = vec![0; size];
    stream.read_exact(&mut body).unwrap();
    (flags[0], body)
}

/// Greets as ZMTP 3.1 if `sub_command`, otherwise 3.0, and sends READY
/// as an XPUB if `mock_pub`, otherwise as a SUB.
fn mock_handshake(stream: &mut TcpStream, sub_command: bool, mock_pub: bool) {
    let mut greeting = GREETING_NULL;
    if sub_command {
        greeting[11] = 1;
    }
    stream.write_all(&greeting).unwrap();
    let mut theirs = [0; 64];
    stream.read_exact(&mut theirs).unwrap();

    stream
        .write_all(if mock_pub { READY_XPUB } else { READY_SUB })
        .unwrap();
    let (flags, body) = read_frame(stream);
    assert_eq!(flags & COMMAND, COMMAND);
    assert!(body.starts_with(b"\x05READY"));
}

/// Waits for the monitor to report the mock's connection accepted.
fn expect_accepted(monitor: &mut Monitor) {
    loop {
        let event = monitor.recv_timeout(FIVE).expect("an event");
        if event.kind == EventKind::Accepted {
            return;
        }
    }
}

fn mock_sub(sub_command: bool) {
    let context = Context::new().unwrap();
    let mut server = context.socket::<XPub>();
    let mut monitor = server.monitor();
    let endpoint = server.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut stream = TcpStream::connect(endpoint.trim_start_matches("tcp://")).unwrap();
    stream.set_read_timeout(Some(FIVE)).unwrap();

    mock_handshake(&mut stream, sub_command, false);
    expect_accepted(&mut monitor);

    if sub_command {
        stream.write_all(b"\x04\x0b\x09SUBSCRIBEA").unwrap();
    } else {
        stream.write_all(b"\x00\x02\x01A").unwrap();
    }
    let (event, mut server) = within(FIVE, move || (server.recv(), server));
    assert_eq!(event, XPubEvent::Subscribe("A".into()));

    assert_eq!(server.send("ALOL"), 1);
    assert_eq!(read_frame(&mut stream), (0, b"ALOL".to_vec()));
}

fn mock_pub(sub_command: bool) {
    let context = Context::new().unwrap();
    let mut server = context.socket::<Sub>();
    let mut monitor = server.monitor();
    let endpoint = server.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let mut stream = TcpStream::connect(endpoint.trim_start_matches("tcp://")).unwrap();
    stream.set_read_timeout(Some(FIVE)).unwrap();

    mock_handshake(&mut stream, sub_command, true);
    expect_accepted(&mut monitor);

    server.subscribe("A");
    // A socket takes in new connections only when used, as libzmq's does:
    // the mock may not be its peer yet. A receive takes it in and sends it
    // the subscription, as libzmq's test does with a non-blocking
    // `zmq_recv`.
    let _ = server.recv_timeout(Duration::from_millis(100));
    let (flags, body) = read_frame(&mut stream);
    if sub_command {
        assert_eq!((flags, body), (COMMAND, b"\x09SUBSCRIBEA".to_vec()));
    } else {
        assert_eq!((flags, body), (0, b"\x01A".to_vec()));
    }

    stream.write_all(b"\x00\x04ALOL").unwrap();
    assert_eq!(within(FIVE, move || server.recv()), Message::from("ALOL"));
}

#[test]
fn mock_sub_command() {
    mock_sub(true);
}

#[test]
fn mock_sub_legacy() {
    mock_sub(false);
}

#[test]
fn mock_pub_command() {
    mock_pub(true);
}

#[test]
fn mock_pub_legacy() {
    mock_pub(false);
}
