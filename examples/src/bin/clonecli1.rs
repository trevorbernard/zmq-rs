//! Clone client model one: subscribes to the clone server's updates on
//! tcp://localhost:5556 and applies them to its own map.
//!
//! The guide prints how many updates came in when interrupted; without a
//! signal handler this runs until killed.

use std::collections::HashMap;

use zguide::kvsimple::KvMsg;
use zmq::{Context, Sub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prepare our context and updates socket
    let context = Context::new()?;
    let mut updates = context.socket::<Sub>();
    updates.subscribe("");
    updates.connect("tcp://localhost:5556")?;

    let mut kvmap = HashMap::new();
    loop {
        let Some(kvmsg) = KvMsg::from_message(&updates.recv()) else {
            continue;
        };
        kvmsg.store(&mut kvmap);
    }
}
