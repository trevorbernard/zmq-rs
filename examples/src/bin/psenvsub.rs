//! Pubsub envelope subscriber: subscribes to topic "B" and prints each
//! message's envelope and contents.

use zguide::text;
use zmq::{Context, Sub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("tcp://localhost:5563")?;
    subscriber.subscribe("B");

    loop {
        let message = subscriber.recv();
        let (address, contents) = message.split_first();
        let contents = contents
            .first()
            .map(|frame| text(frame))
            .unwrap_or_default();
        println!("[{}] {contents}", text(address));
    }
}
