//! Answering a peer's PINGs (RFC 37), from a hand-written ZMTP peer.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use zmq::{Context, Pull};

/// A ZMTP 3.1 NULL handshake as a PUSH.
fn handshake(stream: &mut TcpStream) {
    let mut greeting = vec![0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 3, 1];
    greeting.extend_from_slice(b"NULL");
    greeting.resize(64, 0);
    stream.write_all(&greeting).unwrap();
    stream.read_exact(&mut [0; 64]).unwrap();
    let ready = b"\x05READY\x0bSocket-Type\x00\x00\x00\x04PUSH";
    stream.write_all(&[4, ready.len() as u8]).unwrap();
    stream.write_all(ready).unwrap();
    let mut header = [0; 2];
    stream.read_exact(&mut header).unwrap();
    stream
        .read_exact(&mut vec![0; usize::from(header[1])])
        .unwrap();
}

#[test]
fn a_peer_that_pings_without_reading_is_owed_one_pong_not_one_for_each() {
    const PINGS: u32 = 4_000_000;
    const PONG: usize = 2 + 5 + 4;

    let context = Context::new().unwrap();
    let mut pull = context.socket::<Pull>();
    let bound = pull.bind("tcp://127.0.0.1:*").unwrap().to_string();
    let done = Arc::new(AtomicBool::new(false));
    // A bound socket takes up its peers when called.
    let receiving = thread::spawn({
        let done = done.clone();
        move || {
            while !done.load(Ordering::Relaxed) {
                assert_eq!(pull.recv_timeout(Duration::from_millis(50)), Ok(None));
            }
        }
    });

    let mut peer = TcpStream::connect(bound.trim_start_matches("tcp://")).unwrap();
    handshake(&mut peer);
    let mut pings = Vec::new();
    for context in 0..PINGS {
        pings.extend_from_slice(b"\x04\x0b\x04PING\x00\x00");
        pings.extend_from_slice(&context.to_be_bytes());
    }
    peer.write_all(&pings).unwrap();

    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let last = (PINGS - 1).to_be_bytes();
    let mut pongs = 0;
    loop {
        let mut pong = [0; PONG];
        peer.read_exact(&mut pong)
            .expect("the last PING is answered");
        assert_eq!(pong[..7], *b"\x04\x09\x04PONG");
        pongs += 1;
        if pong[7..] == last {
            break;
        }
    }
    // What the output buffer and the kernel's hold, a few MiB of the 42.
    assert!(pongs < PINGS / 2, "{pongs} PONGs for {PINGS} PINGs");
    done.store(true, Ordering::Relaxed);
    receiving.join().unwrap();
}
