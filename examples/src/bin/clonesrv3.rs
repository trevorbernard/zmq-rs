//! Clone server model three: collects updates from clients on
//! tcp://*:5558, numbers them and republishes them on tcp://*:5557, and
//! serves snapshots of its map on tcp://*:5556.
//!
//! The guide prints how many updates it handled when interrupted; without
//! a signal handler this runs until killed.

use std::collections::HashMap;

use zguide::body;
use zguide::kvsimple::KvMsg;
use zmq::{Context, Pub, Pull, Router};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prepare our context and sockets
    let context = Context::new()?;
    let mut snapshot = context.socket::<Router>();
    snapshot.bind("tcp://*:5556")?;
    let mut publisher = context.socket::<Pub>();
    publisher.bind("tcp://*:5557")?;
    let mut collector = context.socket::<Pull>();
    collector.bind("tcp://*:5558")?;

    // The body of the main task collects updates from clients and
    // publishes them back out to clients
    let mut sequence = 0;
    let mut kvmap = HashMap::new();
    loop {
        let [update, request] = zmq::poll([collector.readable(), snapshot.readable()]);

        // Apply state update sent from client
        if update && let Some(mut kvmsg) = KvMsg::from_message(&collector.recv()) {
            sequence += 1;
            kvmsg.set_sequence(sequence);
            publisher.send(kvmsg.to_message());
            kvmsg.store(&mut kvmap);
            println!("I: publishing update {sequence:5}");
        }
        // Execute state snapshot request
        if request {
            let (identity, request) = snapshot.recv();
            if body(&request) != "ICANHAZ?" {
                println!("E: bad request, aborting");
                return Ok(());
            }
            // For each entry in kvmap, send kvmsg to client
            for kvmsg in kvmap.values() {
                snapshot.send(&identity, kvmsg.to_message())?;
            }

            // Now send END message with sequence number
            println!("I: sending shapshot={sequence}");
            let mut kvmsg = KvMsg::new(sequence);
            kvmsg.set_key("KTHXBAI");
            kvmsg.set_body("");
            snapshot.send(&identity, kvmsg.to_message())?;
        }
    }
}
