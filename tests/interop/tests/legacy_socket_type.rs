//! The socket type a ZMTP 1.0 or 2.0 peer announces in its greeting
//! (RFC 15), which libzmq does not look at: a peer of any type is let in.

use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use zmq::{Context, Message, Pull};
use zmq_interop::libzmq::{Kind, Socket};

const FIVE: Duration = Duration::from_secs(5);

/// PUSH, which a PULL expects; PUB and PULL, which it does not; and two
/// numbers no socket type has.
const ANNOUNCED: [u8; 5] = [8, 1, 7, 11, 255];

/// Connects to `addr` as a peer of `revision` 0 (ZMTP 1.0) or 1 (ZMTP 2.0)
/// that announces `socket_type` and has no identity, and sends `hello`.
fn old_peer(addr: SocketAddr, revision: u8, socket_type: u8) -> TcpStream {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.write_all(b"\xff\0\0\0\0\0\0\0\x01\x7f").unwrap();
    stream.write_all(&[revision, socket_type]).unwrap();
    if revision == 0 {
        stream.write_all(b"\x01\0\x06\0hello").unwrap();
    } else {
        stream.write_all(b"\0\0\0\x05hello").unwrap();
    }
    stream
}

#[test]
fn libzmq_pulls_from_an_old_peer_of_any_announced_type() {
    for revision in [0, 1] {
        for socket_type in ANNOUNCED {
            let pull = Socket::new(Kind::Pull);
            pull.set_receive_timeout(FIVE);
            let _peer = old_peer(pull.bind_loopback(), revision, socket_type);
            assert_eq!(
                pull.recv().ok(),
                Some(vec![b"hello".to_vec()]),
                "revision {revision}, type {socket_type}"
            );
        }
    }
}

#[test]
fn we_pull_from_an_old_peer_of_any_announced_type() {
    for revision in [0, 1] {
        for socket_type in ANNOUNCED {
            let context = Context::new().unwrap();
            let mut pull = context.socket::<Pull>();
            let addr = pull.bind("tcp://127.0.0.1:0").unwrap().to_string();
            let addr = addr.strip_prefix("tcp://").unwrap().parse().unwrap();
            let _peer = old_peer(addr, revision, socket_type);
            assert_eq!(
                pull.recv_timeout(FIVE).ok().flatten(),
                Some(Message::from("hello")),
                "revision {revision}, type {socket_type}"
            );
        }
    }
}
