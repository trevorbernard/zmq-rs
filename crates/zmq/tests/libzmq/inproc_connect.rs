//! libzmq's `tests/test_inproc_connect.cpp`: `inproc` connections made
//! before or after the bind, from one thread or many.

use std::thread;

use zmq::{Context, Dealer, Message, Pair, Pub, Pull, Push, Router, Sub};

use crate::support::{FIVE, settle, within};

#[test]
fn bind_before_connect() {
    let context = Context::new().unwrap();
    let mut bound = context.socket::<Pair>();
    bound.bind("inproc://bbc").unwrap();
    let mut connected = context.socket::<Pair>();
    connected.connect("inproc://bbc").unwrap();
    connected.send("foobar");
    within(FIVE, move || {
        assert_eq!(bound.recv(), Message::from("foobar"))
    });
}

#[test]
fn connect_before_bind() {
    let context = Context::new().unwrap();
    let mut connected = context.socket::<Pair>();
    connected.connect("inproc://cbb").unwrap();
    connected.send("foobar");
    let mut bound = context.socket::<Pair>();
    bound.bind("inproc://cbb").unwrap();
    within(FIVE, move || {
        assert_eq!(bound.recv(), Message::from("foobar"))
    });
}

#[test]
fn connect_before_bind_pub_sub() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher.connect("inproc://cbbps").unwrap();
    // Dropped: nobody has subscribed yet.
    publisher.send("before");
    let mut subscriber = context.socket::<Sub>();
    subscriber.subscribe("");
    subscriber.bind("inproc://cbbps").unwrap();
    settle();
    publisher.send("after");
    within(FIVE, move || {
        assert_eq!(subscriber.recv(), Message::from("after"));
    });
}

#[test]
fn connect_before_bind_ctx_term() {
    within(FIVE, || {
        let context = Context::new().unwrap();
        for i in 0..20 {
            let mut router = context.socket::<Router>();
            router.connect(&format!("inproc://cbbrr{i}")).unwrap();
        }
    });
}

#[test]
fn multiple_connects() {
    let context = Context::new().unwrap();
    let pushes: Vec<_> = (0..10)
        .map(|_| {
            let mut push = context.socket::<Push>();
            push.connect("inproc://multiple").unwrap();
            push.send("foobar");
            push
        })
        .collect();
    let mut pull = context.socket::<Pull>();
    pull.bind("inproc://multiple").unwrap();
    within(FIVE, move || {
        for _ in &pushes {
            assert_eq!(pull.recv(), Message::from("foobar"));
        }
    });
}

// libzmq's pushers here are PAIR sockets connecting to a PULL, which
// libzmq lets through over inproc. zmq-rs checks socket types on inproc
// as on TCP (docs/rfc-compliance.md, "Valid socket pairs"), so they are
// PUSH sockets.
#[test]
fn multiple_threads() {
    let context = Context::new().unwrap();
    let pushers: Vec<_> = (0..30)
        .map(|_| {
            let context = context.clone();
            thread::spawn(move || {
                let mut push = context.socket::<Push>();
                push.connect("inproc://sink").unwrap();
                push.send("foobar");
            })
        })
        .collect();
    let mut pull = context.socket::<Pull>();
    pull.bind("inproc://sink").unwrap();
    within(FIVE, move || {
        for _ in 0..30 {
            assert_eq!(pull.recv(), Message::from("foobar"));
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
                    let mut connected = context.socket::<Pair>();
                    connected.connect(&endpoint).unwrap();
                    assert_eq!(connected.recv(), Message::from("foobar"));
                })
            };
            let binding = {
                let context = context.clone();
                thread::spawn(move || {
                    let mut bound = context.socket::<Pair>();
                    bound.bind(&endpoint).unwrap();
                    bound.send("foobar");
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
    let mut dealer = context.socket::<Dealer>();
    dealer.connect("inproc://routing_id").unwrap();
    let mut router = context.socket::<Router>();
    router.bind("inproc://routing_id").unwrap();
    let mut message = Message::new("A");
    message.push("B");
    dealer.send(message.clone());
    // The routing id comes first, then both parts of the body.
    let (_, body) = within(FIVE, move || router.recv());
    assert_eq!(body, message);
}

#[test]
fn connect_only() {
    let context = Context::new().unwrap();
    let mut push = context.socket::<Push>();
    push.connect("inproc://a").unwrap();
}

#[test]
fn unbind() {
    let context = Context::new().unwrap();
    let mut first = context.socket::<Pair>();
    first.bind("inproc://unbind").unwrap();
    first.unbind("inproc://unbind").unwrap();
    let mut second = context.socket::<Pair>();
    second.bind("inproc://unbind").unwrap();
    let mut connected = context.socket::<Pair>();
    connected.connect("inproc://unbind").unwrap();
    connected.send("foobar");
    within(FIVE, move || {
        assert_eq!(second.recv(), Message::from("foobar"))
    });
}

// libzmq's test_shutdown_during_pend calls zmq_ctx_shutdown with an
// inproc connect pending. zmq::Context has no shutdown; connect_only and
// connect_before_bind_ctx_term cover dropping a pending connect.
