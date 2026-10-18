//! Pathological publisher: sends out 1,000 topics and then one random
//! update per second.

use std::env;

use zguide::{randof, sleep_ms};
use zmq::{Context, Message, Pub};

fn update(topic: u64, data: &'static str) -> Message {
    let mut message = Message::new(format!("{topic:03}"));
    message.push(data);
    message
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut publisher = context.socket::<Pub>();
    let endpoint = env::args()
        .nth(1)
        .unwrap_or_else(|| "tcp://*:5556".to_owned());
    publisher.bind(&endpoint)?;

    // Ensure subscriber connection has time to complete
    sleep_ms(1000);

    // Send out all 1,000 topic messages
    for topic_nbr in 0..1000 {
        publisher.send(update(topic_nbr, "Save Roger"));
    }
    // Send one random update per second
    loop {
        sleep_ms(1000);
        publisher.send(update(randof(1000), "Off with his head!"));
    }
}
