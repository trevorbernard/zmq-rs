//! Connection metadata (`zmq_msg_gets`) between our sockets and libzmq's:
//! what each reads from the properties the other announces.

use std::time::Duration;

use zmq::{Context, Dealer, Identity, SocketOptions};
use zmq_interop::libzmq::{Kind, Msg, Socket};
use zmq_test_ports::{free_endpoint, free_port};

const FIVE: Duration = Duration::from_secs(5);

#[test]
fn we_read_what_a_libzmq_peer_announces() {
    let context = Context::new().unwrap();
    let mut ours = context.socket::<Dealer>();
    let endpoint = ours.bind(&free_endpoint()).unwrap();
    let theirs = Socket::new(Kind::Dealer);
    theirs.set_routing_id(b"theirs");
    theirs.set_metadata("X-Hello", "World");
    theirs.connect(endpoint);
    theirs.send(&[b"hi"]).unwrap();

    let message = ours.recv_timeout(FIVE).expect("their message");
    assert_eq!(message.property("Socket-Type"), Some(b"DEALER".as_slice()));
    assert_eq!(message.property("Identity"), Some(b"theirs".as_slice()));
    assert_eq!(message.property("X-Hello"), Some(b"World".as_slice()));
    assert_eq!(
        message.property("Peer-Address"),
        Some(b"127.0.0.1".as_slice())
    );
}

/// The same lookups through libzmq's own `zmq_msg_gets`, on a message we
/// sent, give what ours give.
#[test]
fn libzmq_reads_what_we_announce_as_we_would() {
    let theirs = Socket::new(Kind::Dealer);
    let addr = theirs.bind_loopback();
    let context = Context::new().unwrap();
    let mut ours = context.socket_with::<Dealer>(SocketOptions {
        identity: Identity::new("ours").unwrap(),
        ..SocketOptions::default()
    });
    ours.connect(&format!("tcp://{addr}")).unwrap();
    ours.send_timeout("hi", FIVE).expect("sent");

    let mut msg = Msg::new();
    theirs.recv_msg(&mut msg).expect("our message");
    assert_eq!(msg.gets("Socket-Type").as_deref(), Some("DEALER"));
    assert_eq!(msg.gets("Identity").as_deref(), Some("ours"));
    assert_eq!(msg.gets("Peer-Address").as_deref(), Some("127.0.0.1"));
    assert_eq!(msg.gets("socket-type"), None, "matched exactly");
    assert_eq!(msg.gets("Routing-Id"), None, "no alias this way round");
}

/// Each connection's messages carry that connection's metadata: after the
/// peer comes back with other properties, the new messages say so.
#[test]
fn messages_after_a_reconnect_carry_the_new_connections_metadata() {
    let first = Socket::new(Kind::Push);
    first.set_metadata("X-Run", "1");
    let addr = free_port();
    first.bind(&format!("tcp://{addr}"));
    let context = Context::new().unwrap();
    let mut ours = context.socket_with::<zmq::Pull>(SocketOptions {
        reconnect: zmq::Reconnect::Fixed {
            interval: Duration::from_millis(10),
        },
        ..SocketOptions::default()
    });
    ours.connect(&format!("tcp://{addr}")).unwrap();
    first.send(&[b"one"]).unwrap();
    let message = ours.recv_timeout(FIVE).expect("from the first run");
    assert_eq!(message.property("X-Run"), Some(b"1".as_slice()));
    drop(first);

    let second = Socket::new(Kind::Push);
    second.set_metadata("X-Run", "2");
    second.bind(&format!("tcp://{addr}"));
    second.send(&[b"two"]).unwrap();
    let message = ours.recv_timeout(FIVE).expect("from the second run");
    assert_eq!(message, zmq::Message::from("two"));
    assert_eq!(message.property("X-Run"), Some(b"2".as_slice()));
}
