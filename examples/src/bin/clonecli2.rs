//! Clone client model two: asks the clone server on tcp://localhost:5556
//! for a snapshot of its map, then applies updates from
//! tcp://localhost:5557, discarding those the snapshot already holds.
//!
//! The guide prints how many updates came in when interrupted; without a
//! signal handler this runs until killed.

use std::collections::HashMap;

use zguide::kvsimple::KvMsg;
use zmq::{Context, Dealer, Sub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prepare our context and subscriber
    let context = Context::new()?;
    let mut snapshot = context.socket::<Dealer>();
    snapshot.connect("tcp://localhost:5556")?;
    let mut subscriber = context.socket::<Sub>();
    subscriber.subscribe("");
    subscriber.connect("tcp://localhost:5557")?;

    let mut kvmap = HashMap::new();

    // Get state snapshot
    snapshot.send("ICANHAZ?");
    let mut sequence = loop {
        let Some(kvmsg) = KvMsg::from_message(&snapshot.recv()) else {
            continue;
        };
        if kvmsg.key() == Some("KTHXBAI") {
            println!("Received snapshot={}", kvmsg.sequence());
            break kvmsg.sequence();
        }
        kvmsg.store(&mut kvmap);
    };

    // Now apply pending updates, discard out-of-sequence messages
    loop {
        let Some(kvmsg) = KvMsg::from_message(&subscriber.recv()) else {
            continue;
        };
        if kvmsg.sequence() > sequence {
            sequence = kvmsg.sequence();
            kvmsg.store(&mut kvmap);
        }
    }
}
