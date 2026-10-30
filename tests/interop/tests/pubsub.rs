//! PUB and SUB against libzmq, a ZMTP 1.0 subscriber, and a publisher that
//! ignores subscriptions.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Message, Pub, Sub};
use zmq_interop::libzmq::{Kind, Socket};
use zmq_interop::{Peer, Security};
use zmq_test_ports::{free_endpoint, free_port};
use zmtp::{Decoder, DecoderConfig, Frame, Identity, Incoming, SocketType, Version};

const FIVE: Duration = Duration::from_secs(5);

/// Runs `f` on another thread and fails the test if it takes longer than
/// `limit`, so a lost wakeup fails instead of hanging the suite.
fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, result) = mpsc::channel();
    thread::spawn(move || {
        let _ = done.send(f());
    });
    result
        .recv_timeout(limit)
        .unwrap_or_else(|_| panic!("timed out after {limit:?}"))
}

/// Publishes `message` until a subscriber takes it.
fn publish_until_taken(publisher: &mut zmq::Socket<Pub>, message: &'static str) {
    let started = Instant::now();
    while publisher.send(message) == 0 {
        assert!(started.elapsed() < FIVE, "no subscriber arrived");
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn our_sub_receives_its_topics_from_a_libzmq_pub() {
    let publisher = Socket::new(Kind::Pub);
    let addr = publisher.bind_loopback();
    let context = Context::new().unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.subscribe("news");
    subscriber.connect(&format!("tcp://{addr}")).unwrap();

    // libzmq drops what it publishes before our subscription arrives.
    let started = Instant::now();
    loop {
        publisher.send(&[b"sports"]).unwrap();
        publisher.send(&[b"news", b"two frames"]).unwrap();
        if let Some(message) = subscriber.try_recv() {
            let expected = vec![Frame::from("news"), Frame::from("two frames")];
            assert_eq!(message, Message::try_from(expected).unwrap());
            break;
        }
        assert!(started.elapsed() < FIVE, "nothing arrived");
        thread::sleep(Duration::from_millis(5));
    }
    publisher.send(&[b"sports"]).unwrap();
    publisher.send(&[b"news: last"]).unwrap();
    // Earlier rounds may still be queued; none of them is sports.
    within(FIVE, move || {
        loop {
            let message = subscriber.recv();
            assert_ne!(message, Message::from("sports"));
            if message == Message::from("news: last") {
                break;
            }
        }
    });
}

#[test]
fn a_libzmq_sub_receives_its_topics_from_our_pub() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    let bound = publisher.bind(&free_endpoint()).unwrap();
    let subscriber = Socket::new(Kind::Sub);
    subscriber.set_receive_timeout(FIVE);
    subscriber.subscribe(b"news");
    subscriber.connect(bound.clone());

    publish_until_taken(&mut publisher, "news: first");
    assert_eq!(publisher.send("sports"), 0);
    assert_eq!(publisher.send("news: second"), 1);
    assert_eq!(subscriber.recv().unwrap(), [b"news: first"]);
    assert_eq!(subscriber.recv().unwrap(), [b"news: second"]);
}

#[test]
fn a_libzmq_sub_unsubscribing_stops_our_pub() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    let bound = publisher.bind(&free_endpoint()).unwrap();
    let subscriber = Socket::new(Kind::Sub);
    subscriber.subscribe(b"a");
    subscriber.connect(bound);
    publish_until_taken(&mut publisher, "a");
    subscriber.unsubscribe(b"a");
    let started = Instant::now();
    while publisher.send("a") != 0 {
        assert!(started.elapsed() < FIVE, "the cancel never arrived");
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn our_sub_filters_what_a_careless_publisher_sends() {
    let listener = TcpListener::bind(free_port()).unwrap();
    let context = Context::new().unwrap();
    let mut subscriber = context.socket::<Sub>();
    subscriber.subscribe("news");
    subscriber
        .connect(&format!("tcp://{}", listener.local_addr().unwrap()))
        .unwrap();
    let mut publisher = Peer::accept(
        &listener,
        SocketType::Pub,
        Identity::default(),
        Security::Null,
    )
    .unwrap();
    // Sent regardless of subscriptions, as a ZMTP 1.0 publisher would.
    for text in ["sports", "news: rain", "weather"] {
        publisher.send(&Message::from(text)).unwrap();
    }
    assert_eq!(
        within(FIVE, move || subscriber.recv()),
        Message::from("news: rain")
    );
}

#[test]
fn a_zmtp_1_0_subscriber_gets_everything() {
    // ZMTP 1.0 SUBs filter for themselves and never send subscriptions.
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    let addr = publisher
        .bind(&free_endpoint())
        .unwrap()
        .tcp_addr()
        .unwrap();
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(FIVE)).unwrap();
    // A ZMTP 1.0 greeting is the identity as a frame: here, empty.
    stream.write_all(&[0x01, 0x00]).unwrap();
    // Ours starts with a 1.0-compatible long frame header: 0xff, a 64-bit
    // length counting the flags octet, then 0x7f where a 3.x peer looks.
    let mut header = [0u8; 10];
    stream.read_exact(&mut header).unwrap();
    assert_eq!((header[0], header[9]), (0xff, 0x7f));
    let identity_len = u64::from_be_bytes(header[1..9].try_into().unwrap()) - 1;
    let mut identity = vec![0u8; identity_len as usize];
    stream.read_exact(&mut identity).unwrap();

    publish_until_taken(&mut publisher, "anything at all");
    let mut decoder = Decoder::new(Version::V1, DecoderConfig::default());
    let item = loop {
        if let Some(item) = decoder.decode().unwrap() {
            break item;
        }
        let read = decoder.fill(|buf| stream.read(buf)).unwrap();
        assert_ne!(read, 0, "closed");
    };
    assert_eq!(item, Incoming::Message(Message::from("anything at all")));
}
