//! Weather update client: connects SUB to tcp://localhost:5556, collects
//! weather updates for one zipcode and averages its temperature.

use std::env;

use zguide::body;
use zmq::{Context, Sub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Collecting updates from weather server...");
    let context = Context::new()?;
    let mut subscriber = context.socket::<Sub>();
    subscriber.connect("tcp://localhost:5556")?;

    let filter = env::args().nth(1).unwrap_or_else(|| "10001 ".to_owned());
    subscriber.subscribe(&filter);

    let updates = 100;
    let mut total_temp = 0;
    for _ in 0..updates {
        let update = body(&subscriber.recv());
        let temperature: i64 = update
            .split_whitespace()
            .nth(1)
            .and_then(|t| t.parse().ok())
            .ok_or("malformed update")?;
        total_temp += temperature;
    }
    println!(
        "Average temperature for zipcode '{filter}' was {}F",
        total_temp / updates
    );
    Ok(())
}
