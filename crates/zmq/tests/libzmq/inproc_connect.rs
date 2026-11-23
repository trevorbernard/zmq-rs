//! libzmq's `tests/test_inproc_connect.cpp`: `inproc` connections made
//! before or after the bind, from one thread or many.

use std::thread;

use zmq::{Context, Dealer, Message, Pair, Pub, Pull, Push, Router, Sub};

use crate::support::{FIVE, settle, within};

#[test]
fn bind_before_connect() {
    let context = Context::new().unwrap();
    let mut bound = context.socket::<Pair>().unwrap();
    bound.bind("inproc://bbc").unwrap();
    let mut connected = context.socket::<Pair>().unwrap();
    connected.connect("inproc://bbc").unwrap();
    connected.send("foobar").unwrap();
    within(FIVE, move || {
        assert_eq!(bound.recv().unwrap(), Message::from("foobar"))
    });
}

#[test]
fn connect_before_bind() {
    let context = Context::new().unwrap();
    let mut connected = context.socket::<Pair>().unwrap();
    connected.connect("inproc://cbb").unwrap();
    connected.send("foobar").unwrap();
    let mut bound = context.socket::<Pair>().unwrap();
    bound.bind("inproc://cbb").unwrap();
    within(FIVE, move || {
        assert_eq!(bound.recv().unwrap(), Message::from("foobar"))
    });
}

#[test]
fn connect_before_bind_pub_sub() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>().unwrap();
    publisher.connect("inproc://cbbps").unwrap();
    // Dropped: nobody has subscribed yet.
    publisher.send("before").unwrap();
    let mut subscriber = context.socket::<Sub>().unwrap();
    subscriber.subscribe("");
    subscriber.bind("inproc://cbbps").unwrap();
    settle();
    publisher.send("after").unwrap();
    within(FIVE, move || {
        assert_eq!(subscriber.recv().unwrap(), Message::from("after"));
    });
}

#[test]
fn connect_before_bind_ctx_term() {
    within(FIVE, || {
        let context = Context::new().unwrap();
        for i in 0..20 {
            let mut router = context.socket::<Router>().unwrap();
            router.connect(format!("inproc://cbbrr{i}")).unwrap();
        }
    });
}

#[test]
fn multiple_connects() {
    let context = Context::new().unwrap();
    let pushes: Vec<_> = (0..10)
        .map(|_| {
            let mut push = context.socket::<Push>().unwrap();
            push.connect("inproc://multiple").unwrap();
            push.send("foobar").unwrap();
            push
        })
        .collect();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://multiple").unwrap();
    within(FIVE, move || {
        for _ in &pushes {
            assert_eq!(pull.recv().unwrap(), Message::from("foobar"));
        }
    });
}

// The pushers are PAIR sockets connecting to a PULL, as libzmq's are:
// over inproc, which has no handshake, any two socket types are paired.
#[test]
fn multiple_threads() {
    let context = Context::new().unwrap();
    let pushers: Vec<_> = (0..30)
        .map(|_| {
            let context = context.clone();
            thread::spawn(move || {
                let mut pair = context.socket::<Pair>().unwrap();
                pair.connect("inproc://sink").unwrap();
                pair.send("foobar").unwrap();
            })
        })
        .collect();
    let mut pull = context.socket::<Pull>().unwrap();
    pull.bind("inproc://sink").unwrap();
    within(FIVE, move || {
        for _ in 0..30 {
            assert_eq!(pull.recv().unwrap(), Message::from("foobar"));
        }
    });
    for pusher in pushers {
        pusher.join().unwrap();
    }
}

#[test]
fn simultaneous_connect_bind_threads() {
    let context = Context::new().unwrap();
    let threads: Vec<_> = (0..50)
        .flat_map(|i| {
            let endpoint = format!("inproc://foo_{i}");
            let connecting = {
                let (context, endpoint) = (context.clone(), endpoint.clone());
                thread::spawn(move || {
                    let mut connected = context.socket::<Pair>().unwrap();
                    connected.connect(&endpoint).unwrap();
                    assert_eq!(connected.recv().unwrap(), Message::from("foobar"));
                })
            };
            let binding = {
                let context = context.clone();
                thread::spawn(move || {
                    let mut bound = context.socket::<Pair>().unwrap();
                    bound.bind(&endpoint).unwrap();
                    bound.send("foobar").unwrap();
                })
            };
            [connecting, binding]
        })
        .collect();
    within(FIVE, move || {
        for thread in threads {
            thread.join().unwrap();
        }
    });
}

#[test]
fn routing_id() {
    let context = Context::new().unwrap();
    let mut dealer = context.socket::<Dealer>().unwrap();
    dealer.connect("inproc://routing_id").unwrap();
    let mut router = context.socket::<Router>().unwrap();
    router.bind("inproc://routing_id").unwrap();
    let mut message = Message::new("A");
    message.push("B");
    dealer.send(message.clone()).unwrap();
    // The routing id comes first, then both parts of the body.
    let (_, body) = within(FIVE, move || router.recv().unwrap());
    assert_eq!(body, message);
}

#[test]
fn connect_only() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>().unwrap();
    push.connect("inproc://a").unwrap();
}

#[test]
fn unbind() {
    let context = Context::new().unwrap();
    let mut first = context.socket::<Pair>().unwrap();
    first.bind("inproc://unbind").unwrap();
    first.unbind("inproc://unbind").unwrap();
    let mut second = context.socket::<Pair>().unwrap();
    second.bind("inproc://unbind").unwrap();
    let mut connected = context.socket::<Pair>().unwrap();
    connected.connect("inproc://unbind").unwrap();
    connected.send("foobar").unwrap();
    within(FIVE, move || {
        assert_eq!(second.recv().unwrap(), Message::from("foobar"))
    });
}

#[test]
fn shutdown_during_pend() {
    let context = Context::new().unwrap();
    let mut connect_socket = context.socket::<Pair>().unwrap();
    connect_socket.connect("inproc://cbb").unwrap();

    context.shutdown(FIVE).unwrap();
}
