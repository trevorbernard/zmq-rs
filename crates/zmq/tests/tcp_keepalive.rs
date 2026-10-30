//! TCP keepalive probes, as libzmq's `ZMQ_TCP_KEEPALIVE` and friends.

use std::time::Duration;

use zmq::{Context, Message, Pull, Push, SocketOptions, TcpKeepalive};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);

#[test]
fn connections_with_keepalive_carry_messages_both_ways_of_connecting() {
    let keepalive = SocketOptions {
        tcp_keepalive: Some(TcpKeepalive {
            idle: Some(Duration::from_secs(30)),
            interval: Some(Duration::from_secs(5)),
            count: Some(3),
        }),
        ..SocketOptions::default()
    };
    let context = Context::new().unwrap();
    let mut pull = context.socket_with::<Pull>(keepalive.clone());
    let bound = pull.bind(&free_endpoint()).unwrap().to_string();
    let mut push = context.socket_with::<Push>(keepalive);
    push.connect(&bound).unwrap();
    push.send("probed");
    assert_eq!(pull.recv_timeout(FIVE), Some(Message::from("probed")));
}
