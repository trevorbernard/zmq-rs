//! libzmq's `tests/test_pub_invert_matching.cpp`: with inverted matching,
//! a message goes to the subscribers none of whose topics it starts with.

use zmq::{Context, Message, Pub, Sub};

use crate::support::settle;

const PREFIX1: &str = "prefix1";
const PREFIX2: &str = "p2";

fn expect(subscriber: &mut zmq::Socket<Sub>, topic: &str) {
    assert_eq!(subscriber.try_recv(), Some(Message::from(topic.to_owned())));
}

#[test]
fn test() {
    let context = Context::new().unwrap();
    let mut publisher = context.socket::<Pub>();
    publisher
        .bind("inproc://libzmq-pub-invert-matching")
        .unwrap();

    let mut sub1 = context.socket::<Sub>();
    sub1.connect("inproc://libzmq-pub-invert-matching").unwrap();
    let mut sub2 = context.socket::<Sub>();
    sub2.connect("inproc://libzmq-pub-invert-matching").unwrap();

    // Subscribe sub1 to one prefix and sub2 to another prefix.
    sub1.subscribe(PREFIX1);
    sub2.subscribe(PREFIX2);

    // Send a message with the first prefix
    publisher.send(PREFIX1);
    settle();
    // sub1 should receive it, but not sub2
    expect(&mut sub1, PREFIX1);
    assert_eq!(sub2.try_recv(), None);

    // Send a message with the second prefix
    publisher.send(PREFIX2);
    settle();
    // sub2 should receive it, but not sub1
    expect(&mut sub2, PREFIX2);
    assert_eq!(sub1.try_recv(), None);

    // Now invert the matching, on both sides, otherwise the SUB socket
    // will filter the messages out
    publisher.set_invert_matching(true);
    sub1.set_invert_matching(true);
    sub2.set_invert_matching(true);

    // Send a message with the first prefix
    publisher.send(PREFIX1);
    settle();
    // sub2 should receive it, but not sub1
    expect(&mut sub2, PREFIX1);
    assert_eq!(sub1.try_recv(), None);

    // Send a message with the second prefix
    publisher.send(PREFIX2);
    settle();
    // sub1 should receive it, but not sub2
    expect(&mut sub1, PREFIX2);
    assert_eq!(sub2.try_recv(), None);
}
