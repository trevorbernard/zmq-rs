//! TCP keepalive probes, as libzmq's `ZMQ_TCP_KEEPALIVE` and friends.

use std::time::Duration;

use zmq::{Context, Message, Pull, Push, SocketOptions, TcpKeepalive};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);

#[test]
fn connections_with_keepalive_carry_messages_both_ways_of_connecting() {
    let keepalive = SocketOptions::new().tcp_keepalive(TcpKeepalive {
        idle: Some(Duration::from_secs(30)),
        interval: Some(Duration::from_secs(5)),
        count: Some(3),
    });
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(keepalive.clone()).unwrap();
    let bound = pull.bind(free_endpoint()).unwrap().to_string();
    let mut push = context.socket_with::<Push>(keepalive).unwrap();
    push.connect(&bound).unwrap();
    push.send("probed").unwrap();
    assert_eq!(
        pull.recv_timeout(FIVE).unwrap(),
        Some(Message::from("probed"))
    );
}
