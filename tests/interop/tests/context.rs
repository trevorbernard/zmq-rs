//! Context options, compared with libzmq.

use std::time::Duration;

use zmq::{Context, Error, Message, Pair, Pull, Push};
use zmq_interop::libzmq::{self, Kind, Socket};
use zmq_test_ports::free_endpoint;

const FIVE: Duration = Duration::from_secs(5);

#[test]
fn libzmq_without_io_threads_serves_inproc_alone() {
    let context = libzmq::Context::with_io_threads(0);
    let bound = Socket::in_context(&context, Kind::Pair);
    bound.bind("inproc://alone");
    let connected = Socket::in_context(&context, Kind::Pair);
    connected.try_connect("inproc://alone").unwrap();
    connected.send(&[b"hello"]).unwrap();
    assert_eq!(bound.recv().unwrap(), [b"hello"]);

    let tcp = Socket::in_context(&context, Kind::Pull);
    let refused = tcp.try_bind(&free_endpoint()).unwrap_err().to_string();
    assert!(refused.contains("No thread available"), "{refused}");
    let refused = tcp.try_connect(&free_endpoint()).unwrap_err().to_string();
    assert!(refused.contains("No thread available"), "{refused}");
}

#[test]
fn we_serve_inproc_alone_without_io_threads() {
    let context = Context::with_io_threads(0).expect("as libzmq takes it");
    let mut bound = context.socket::<Pair>();
    bound.bind("inproc://alone").unwrap();
    let mut connected = context.socket::<Pair>();
    connected.connect("inproc://alone").unwrap();
    connected.send("hello").unwrap();
    assert_eq!(
        bound.recv_timeout(FIVE).unwrap(),
        Some(Message::from("hello"))
    );

    let mut pull = context.socket::<Pull>();
    assert!(matches!(pull.bind(free_endpoint()), Err(Error::NoIoThread)));
    let mut push = context.socket::<Push>();
    assert!(matches!(
        push.connect(free_endpoint()),
        Err(Error::NoIoThread)
    ));
}
