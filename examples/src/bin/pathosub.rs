//! Pathological subscriber: subscribes to one random topic and prints
//! whatever arrives on it.

use std::env;

use zguide::{randof, text};
use zmq::{Context, Sub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut subscriber = context.socket::<Sub>();
    let endpoint = env::args()
        .nth(1)
        .unwrap_or_else(|| "tcp://localhost:5556".to_owned());
    subscriber.connect(&endpoint)?;

    let subscription = format!("{:03}", randof(1000));
    subscriber.subscribe(&subscription);

    loop {
        let message = subscriber.recv();
        let (topic, data) = message.split_first();
        assert_eq!(text(topic), subscription);
        println!(
            "{}",
            data.first().map(|frame| text(frame)).unwrap_or_default()
        );
    }
}
