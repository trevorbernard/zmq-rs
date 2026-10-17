//! XPUB options against libzmq: welcome messages and manual subscriptions.

use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, ManualXPub, Message, Sub, XPub, XPubEvent};
use zmq_interop::libzmq::{Kind, Socket};

const FIVE: Duration = Duration::from_secs(5);

#[test]
fn our_xpub_welcomes_a_libzmq_sub() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<XPub>();
    xpub.set_welcome(Some(Message::from("WELCOME")));
    let bound = xpub.bind("tcp://127.0.0.1:*").unwrap();
    let sub = Socket::new(Kind::Sub);
    sub.subscribe(b"WELCOME");
    sub.connect(bound);
    // Our XPUB greets the peer when it next takes peers in.
    assert_eq!(xpub.recv(), XPubEvent::Subscribe("WELCOME".into()));
    assert_eq!(sub.recv().unwrap(), [b"WELCOME".to_vec()]);
}

#[test]
fn a_libzmq_xpub_welcomes_our_sub() {
    let xpub = Socket::new(Kind::XPub);
    xpub.set_xpub_welcome(b"WELCOME");
    let addr = xpub.bind_loopback();
    let context = Context::new().unwrap();
    let mut sub = context.socket::<Sub>();
    sub.subscribe("WELCOME");
    sub.connect(&format!("tcp://{addr}")).unwrap();
    // libzmq attaches the pipe, and sends the welcome, while it is called.
    assert_eq!(xpub.recv().unwrap(), [b"\x01WELCOME".to_vec()]);
    let started = Instant::now();
    let welcome = loop {
        assert!(started.elapsed() < FIVE, "never welcomed");
        if let Some(message) = sub.try_recv() {
            break message;
        }
        thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(welcome, Message::from("WELCOME"));
}

#[test]
fn our_manual_xpub_decides_a_libzmq_subs_topics() {
    let context = Context::new().unwrap();
    let mut xpub = context.socket::<ManualXPub>();
    let bound = xpub.bind("tcp://127.0.0.1:*").unwrap();
    let sub = Socket::new(Kind::Sub);
    sub.subscribe(b"");
    sub.connect(bound);
    let mut request = xpub.recv();
    assert_eq!(request.event(), &XPubEvent::Subscribe("".into()));
    assert!(request.subscribe("allowed"));
    drop(request);
    assert_eq!(xpub.send("secret"), 0);
    assert_eq!(xpub.send("allowed"), 1);
    assert_eq!(sub.recv().unwrap(), [b"allowed".to_vec()]);
}
