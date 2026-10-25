//! Clone client model six: uses the clone API to keep a map in step with
//! a primary and backup clone server, on tcp://localhost:5556 and
//! tcp://localhost:5566, setting a random value under `/client/` every
//! second.

use zguide::clone::CloneClient;
use zguide::{randof, sleep_ms};

const SUBTREE: &str = "/client/";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create distributed hash instance
    let mut clone = CloneClient::new()?;

    // Specify configuration
    clone.subtree(SUBTREE);
    clone.connect("tcp://localhost", "5556");
    clone.connect("tcp://localhost", "5566");

    // Set random tuples into the distributed hash
    loop {
        let key = format!("{SUBTREE}{}", randof(10_000));
        let value = randof(1_000_000).to_string();
        clone.set(&key, &value, randof(30) as u32);
        sleep_ms(1000);
    }
}
