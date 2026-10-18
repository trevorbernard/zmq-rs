//! Pubsub envelope publisher: publishes two-frame messages whose first
//! frame is the topic.

use zguide::sleep_ms;
use zmq::{Context, Message, Pub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut publisher = context.socket::<Pub>();
    publisher.bind("tcp://*:5563")?;

    loop {
        // Write two messages, each with an envelope and content
        let mut a = Message::new("A");
        a.push("We don't want to see this");
        publisher.send(a);
        let mut b = Message::new("B");
        b.push("We would like to see this");
        publisher.send(b);
        sleep_ms(1000);
    }
}
