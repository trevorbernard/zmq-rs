//! Clone server model four: collects updates from clients on
//! tcp://*:5558, numbers them and republishes them on tcp://*:5557, and
//! serves snapshots of the subtree each client asks for on tcp://*:5556.
//!
//! The guide prints how many updates it handled when interrupted; without
//! a signal handler this runs until killed.

use std::collections::HashMap;

use zguide::kvsimple::KvMsg;
use zguide::text;
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

    let mut sequence = 0;
    let mut kvmap: HashMap<String, KvMsg> = HashMap::new();
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
        // Execute state snapshot request, for the subtree in its second
        // frame
        if request {
            let (identity, request) = snapshot.recv();
            let (command, rest) = request.split_first();
            if **command != *b"ICANHAZ?" {
                println!("E: bad request, aborting");
                return Ok(());
            }
            let subtree = rest.first().map(|frame| text(frame)).unwrap_or_default();

            // Send the entries under the subtree to the client
            for (key, kvmsg) in &kvmap {
                if key.starts_with(&subtree) {
                    snapshot.send(&identity, kvmsg.to_message())?;
                }
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
