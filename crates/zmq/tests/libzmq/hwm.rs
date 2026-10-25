//! libzmq's `tests/test_hwm.cpp`: how many messages an `inproc` PUSH can
//! queue to a PULL, by the HWMs on each side and which side came first.
//! As in libzmq, an `inproc` pipe holds what the sender and the receiver
//! would each buffer, and an HWM of 0, here [`Hwm::Unlimited`], is none.

use std::num::NonZeroUsize;

use zmq::{Context, Hwm, Pull, Push, Socket, SocketOptions};

use crate::support::{FIVE, settle};

const MAX_SENDS: usize = 10000;

enum TestType {
    BindFirst,
    ConnectFirst,
}

fn hwm(messages: usize) -> Hwm {
    NonZeroUsize::new(messages).map_or(Hwm::Unlimited, Hwm::Messages)
}

fn sender(context: &Context, send_hwm: usize) -> Socket<Push> {
    context.socket_with::<Push>(SocketOptions {
        send_hwm: hwm(send_hwm),
        ..SocketOptions::default()
    })
}

fn receiver(context: &Context, recv_hwm: usize) -> Socket<Pull> {
    context.socket_with::<Pull>(SocketOptions {
        recv_hwm: hwm(recv_hwm),
        ..SocketOptions::default()
    })
}

/// Sends until the PUSH would block, at most [`MAX_SENDS`].
fn send_until_blocked(socket: &mut Socket<Push>) -> usize {
    let mut send_count = 0;
    while send_count < MAX_SENDS && socket.try_send("").is_ok() {
        send_count += 1;
    }
    send_count
}

fn receive_all(socket: &mut Socket<Pull>) -> usize {
    let mut recv_count = 0;
    while socket.try_recv().is_some() {
        recv_count += 1;
    }
    recv_count
}

#[test]
fn defaults() {
    let context = Context::new().unwrap();
    let mut bind_socket = context.socket::<Pull>();
    bind_socket.bind("inproc://libzmq-hwm-defaults").unwrap();
    let mut connect_socket = context.socket::<Push>();
    connect_socket
        .connect("inproc://libzmq-hwm-defaults")
        .unwrap();

    let send_count = send_until_blocked(&mut connect_socket);
    settle();
    assert_eq!(receive_all(&mut bind_socket), send_count);

    // Default values are 1000 on send and 1000 on receive, so 2000 total
    assert_eq!(send_count, 2000);
}

fn count_msg(send_hwm: usize, recv_hwm: usize, test_type: TestType) -> usize {
    let context = Context::new().unwrap();
    let endpoint = "inproc://libzmq-hwm-count";
    let (mut bind_socket, mut connect_socket) = match test_type {
        TestType::BindFirst => {
            let mut bind_socket = receiver(&context, recv_hwm);
            bind_socket.bind(endpoint).unwrap();
            let mut connect_socket = sender(&context, send_hwm);
            connect_socket.connect(endpoint).unwrap();
            (bind_socket, connect_socket)
        }
        TestType::ConnectFirst => {
            let mut connect_socket = sender(&context, send_hwm);
            connect_socket.connect(endpoint).unwrap();
            let mut bind_socket = receiver(&context, recv_hwm);
            bind_socket.bind(endpoint).unwrap();
            (bind_socket, connect_socket)
        }
    };

    let send_count = send_until_blocked(&mut connect_socket);
    assert_eq!(receive_all(&mut bind_socket), send_count);

    // Now it should be possible to send one more
    connect_socket.send("");
    assert!(bind_socket.recv_timeout(FIVE).is_some());
    send_count
}

fn inproc_bind_first(send_hwm: usize, recv_hwm: usize) -> usize {
    count_msg(send_hwm, recv_hwm, TestType::BindFirst)
}

fn inproc_connect_first(send_hwm: usize, recv_hwm: usize) -> usize {
    count_msg(send_hwm, recv_hwm, TestType::ConnectFirst)
}

fn inproc_connect_and_close_first(send_hwm: usize, recv_hwm: usize) -> usize {
    let context = Context::new().unwrap();
    let endpoint = "inproc://libzmq-hwm-connect-and-close";
    let mut connect_socket = sender(&context, send_hwm);
    connect_socket.connect(endpoint).unwrap();
    let send_count = send_until_blocked(&mut connect_socket);
    drop(connect_socket);

    let mut bind_socket = receiver(&context, recv_hwm);
    bind_socket.bind(endpoint).unwrap();
    assert_eq!(receive_all(&mut bind_socket), send_count);
    send_count
}

fn inproc_bind_and_close_first(send_hwm: usize) -> usize {
    let context = Context::new().unwrap();
    let mut bind_socket = sender(&context, send_hwm);
    bind_socket
        .bind("inproc://libzmq-hwm-bind-and-close")
        .unwrap();
    let send_count = send_until_blocked(&mut bind_socket);
    drop(bind_socket);
    send_count
}

#[test]
fn infinite_both_inproc_bind_first() {
    assert_eq!(inproc_bind_first(0, 0), MAX_SENDS);
}

#[test]
fn infinite_both_inproc_connect_first() {
    assert_eq!(inproc_connect_first(0, 0), MAX_SENDS);
}

#[test]
fn infinite_receive_inproc_bind_first() {
    assert_eq!(inproc_bind_first(1, 0), MAX_SENDS);
}

#[test]
fn infinite_receive_inproc_connect_first() {
    assert_eq!(inproc_connect_first(1, 0), MAX_SENDS);
}

#[test]
fn infinite_send_inproc_bind_first() {
    assert_eq!(inproc_bind_first(0, 1), MAX_SENDS);
}

#[test]
fn infinite_send_inproc_connect_first() {
    assert_eq!(inproc_connect_first(0, 1), MAX_SENDS);
}

#[test]
fn finite_both_bind_first() {
    // Send and receive HWMs of 1, so total that can be queued is 2
    assert_eq!(inproc_bind_first(1, 1), 2);
}

#[test]
fn finite_both_connect_first() {
    // Send and receive HWMs of 1, so total that can be queued is 2
    assert_eq!(inproc_connect_first(1, 1), 2);
}

#[test]
fn infinite_recv_connect_and_close_first() {
    // Send HWM of 1, sent before the bind, so total that can be queued is 1
    assert_eq!(inproc_connect_and_close_first(1, 0), 1);
}

#[test]
fn infinite_recv_bind_and_close_first() {
    // libzmq asserts nothing here: it throws away what the binding side
    // queues before anything connects. A PUSH with no peer has nowhere to
    // queue, so it sends nothing.
    assert_eq!(inproc_bind_and_close_first(1), 0);
}
