//! libzmq's `tests/test_sockopt_hwm.cpp`: `ZMQ_SNDHWM` and `ZMQ_RCVHWM`
//! changed before and after an `inproc` PUSH and PULL are connected.
//!
//! libzmq also reads each option back. zmq-rs has no getsockopt, so the
//! mark is known here by how many messages the PUSH takes.

use zmq::{Context, Hwm, Message, Pull, Push, Socket};

use crate::support::settle;

const MAX_SENDS: usize = 10000;

fn hwm(messages: usize) -> Hwm {
    Hwm::from(messages)
}

#[test]
fn change_before_connected() {
    let context = Context::new().unwrap();
    let mut bind_socket = context.socket::<Push>().unwrap();
    let mut connect_socket = context.socket::<Pull>().unwrap();

    connect_socket.set_recv_hwm(hwm(2));
    bind_socket.set_send_hwm(hwm(2));

    connect_socket
        .connect("inproc://libzmq-sockopt-hwm-a")
        .unwrap();
    bind_socket.bind("inproc://libzmq-sockopt-hwm-a").unwrap();

    let mut send_count = 0;
    while send_count < MAX_SENDS && bind_socket.try_send("").is_ok() {
        send_count += 1;
    }
    assert_eq!(send_count, 4);
}

#[test]
fn change_after_connected() {
    let context = Context::new().unwrap();
    let mut bind_socket = context.socket::<Push>().unwrap();
    let mut connect_socket = context.socket::<Pull>().unwrap();

    connect_socket.set_recv_hwm(hwm(1));
    bind_socket.set_send_hwm(hwm(1));

    connect_socket
        .connect("inproc://libzmq-sockopt-hwm-b")
        .unwrap();
    bind_socket.bind("inproc://libzmq-sockopt-hwm-b").unwrap();

    bind_socket.set_send_hwm(hwm(5));

    let mut send_count = 0;
    while send_count < MAX_SENDS && bind_socket.try_send("").is_ok() {
        send_count += 1;
    }
    assert_eq!(send_count, 6);
}

fn send_until_wouldblock(socket: &mut Socket<Push>) -> usize {
    let mut send_count = 0;
    while send_count < MAX_SENDS
        && socket
            .try_send(Message::from(send_count.to_ne_bytes().to_vec()))
            .is_ok()
    {
        send_count += 1;
    }
    send_count
}

fn fill_up_to_hwm(socket: &mut Socket<Push>, sndhwm: usize) -> usize {
    let send_count = send_until_wouldblock(socket);
    assert!(send_count <= sndhwm + 1);
    assert!(send_count > sndhwm / 10);
    send_count
}

#[test]
fn decrease_when_full() {
    let context = Context::new().unwrap();
    let mut bind_socket = context.socket::<Push>().unwrap();
    let mut connect_socket = context.socket::<Pull>().unwrap();

    connect_socket.set_recv_hwm(hwm(1));
    let mut sndhwm = 100;
    bind_socket.set_send_hwm(hwm(sndhwm));

    bind_socket.bind("inproc://libzmq-sockopt-hwm-c").unwrap();
    connect_socket
        .connect("inproc://libzmq-sockopt-hwm-c")
        .unwrap();
    settle();

    // Fill up to hwm
    let send_count = fill_up_to_hwm(&mut bind_socket, sndhwm);

    // Decrease snd hwm
    sndhwm = 70;
    bind_socket.set_send_hwm(hwm(sndhwm));
    settle();

    // Read out all data (should get up to previous hwm worth so none were dropped)
    let mut read_count = 0;
    while read_count < MAX_SENDS
        && let Some(message) = connect_socket.try_recv().unwrap()
    {
        assert_eq!(message, Message::from(read_count.to_ne_bytes().to_vec()));
        read_count += 1;
    }
    assert_eq!(read_count, send_count);

    // Give io thread some time to catch up
    settle();

    // Fill up to new hwm
    fill_up_to_hwm(&mut bind_socket, sndhwm);
}
