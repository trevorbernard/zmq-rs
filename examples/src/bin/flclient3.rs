//! Freelance client, model three: uses the flcliapi agent to encapsulate
//! the Freelance pattern, and measures the average round trip.

use std::time::Instant;

use zguide::flcliapi::Flcliapi;
use zmq::{Context, Message};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create new freelance client object
    let context = Context::new()?;
    let mut client = Flcliapi::new(&context)?;

    // Connect to several endpoints
    client.connect("tcp://localhost:5555");
    client.connect("tcp://localhost:5556");
    client.connect("tcp://localhost:5557");

    // Send a bunch of name resolution 'requests', measure time
    let requests = 10_000;
    let start = Instant::now();
    for _ in 0..requests {
        if client.request(Message::new("random name")).is_none() {
            println!("E: name service not available, aborting");
            break;
        }
    }
    println!(
        "Average round trip cost: {} usec",
        start.elapsed().as_micros() / requests
    );
    Ok(())
}
