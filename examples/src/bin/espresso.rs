//! Espresso pattern: a proxy between a publisher and a subscriber whose
//! capture socket shows a listener thread everything that passes through,
//! subscriptions included.

use std::thread;

use zguide::{dump, randof, sleep_ms};
use zmq::{Context, Pair, Proxy, Pub, Sub, XPub, XSub};

// The subscriber thread requests messages starting with A and B, then
// reads and counts incoming messages.
fn subscriber_thread(context: Context) -> Result<(), zmq::Error> {
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("tcp://localhost:6001")?;
    subscriber.subscribe("A");
    subscriber.subscribe("B");

    for _ in 0..5 {
        subscriber.recv();
    }
    Ok(())
}

// The publisher sends random messages starting with A-J.
fn publisher_thread(context: Context) -> Result<(), zmq::Error> {
    let mut publisher = context.socket::<Pub>();
    publisher.bind("tcp://*:6000")?;

    loop {
        let topic = char::from(b'A' + randof(10) as u8);
        publisher.send(format!("{topic}-{:05}", randof(100_000)));
        sleep_ms(100);
    }
}

// The listener receives all messages flowing through the proxy, on its
// pipe. In CZMQ, the pipe is a pair of PAIR sockets connected over inproc.
fn listener_thread(mut pipe: zmq::Socket<Pair>) {
    loop {
        dump(&pipe.recv());
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start child threads
    let context = Context::new()?;
    thread::spawn({
        let context = context.clone();
        move || publisher_thread(context)
    });
    thread::spawn({
        let context = context.clone();
        move || subscriber_thread(context)
    });

    let mut subscriber = context.socket::<XSub>();
    subscriber.connect("tcp://localhost:6000")?;
    let mut publisher = context.socket::<XPub>();
    publisher.bind("tcp://*:6001")?;

    let mut listener = context.socket::<Pair>();
    listener.bind("inproc://listener")?;
    let mut pipe = context.socket::<Pair>();
    pipe.connect("inproc://listener")?;
    thread::spawn(move || listener_thread(pipe));

    Proxy::new(&mut subscriber, &mut publisher)
        .capture(&mut listener)
        .run();
    Ok(())
}
