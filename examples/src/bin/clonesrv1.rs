//! Clone server model one: publishes random key-value updates on
//! tcp://*:5556 and keeps its own copy of the map.
//!
//! The guide prints how many updates went out when interrupted; without a
//! signal handler this runs until killed.

use std::collections::HashMap;

use zguide::kvsimple::KvMsg;
use zguide::{randof, sleep_ms};
use zmq::{Context, Pub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prepare our context and publisher socket
    let context = Context::new()?;
    let mut publisher = context.socket::<Pub>();
    publisher.bind("tcp://*:5556")?;
    sleep_ms(200);

    let mut kvmap = HashMap::new();
    let mut sequence = 0;
    loop {
        // Distribute as key-value message
        sequence += 1;
        let mut kvmsg = KvMsg::new(sequence);
        kvmsg.set_key(randof(10_000).to_string());
        kvmsg.set_body(randof(1_000_000).to_string());
        publisher.send(kvmsg.to_message());
        kvmsg.store(&mut kvmap);
    }
}
