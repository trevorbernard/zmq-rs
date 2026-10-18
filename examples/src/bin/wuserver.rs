//! Weather update server: binds PUB to tcp://*:5556 and publishes random
//! weather updates.

use zguide::randof;
use zmq::{Context, Pub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut publisher = context.socket::<Pub>();
    publisher.bind("tcp://*:5556")?;
    loop {
        let zipcode = randof(100_000);
        let temperature = randof(215) as i64 - 80;
        let relhumidity = randof(50) + 10;
        publisher.send(format!("{zipcode:05} {temperature} {relhumidity}"));
    }
}
