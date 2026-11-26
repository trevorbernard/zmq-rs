//! libzmq's `tests/test_zmq_poll_fd.cpp`, onto `zmq::poll_timeout` and
//! `PollItem::fd_readable`. libzmq's `revents` are the array of `bool`s.

use std::net::UdpSocket;
use std::os::fd::AsFd;
use std::time::Duration;

use zmq::{Context, PollItem, Rep};

#[test]
fn poll_fd() {
    let recv_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let saddr = recv_socket.local_addr().unwrap();

    let context = Context::new().unwrap();
    let mut sb = context.socket::<Rep>().unwrap();
    sb.bind("tcp://127.0.0.1:*").unwrap();

    let send_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    send_socket.send_to(&[1; 10], saddr).unwrap();

    let pollitems = [
        sb.readable(),
        PollItem::fd_readable(recv_socket.as_fd()).unwrap(),
    ];
    assert_eq!(
        zmq::poll_timeout(pollitems, Duration::from_millis(1)).unwrap(),
        Some([false, true])
    );
}
