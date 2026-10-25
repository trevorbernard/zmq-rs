//! Clone server model two: publishes random key-value updates on
//! tcp://*:5557, and a state manager thread keeps the map and serves
//! snapshots on tcp://*:5556.
//!
//! The guide prints how many updates went out when interrupted; without a
//! signal handler this runs until killed.

use std::collections::HashMap;
use std::thread;

use zguide::kvsimple::KvMsg;
use zguide::{body, randof};
use zmq::{Context, Pair, Pub, Router, Socket};

type TaskResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prepare our context and sockets
    let context = Context::new()?;
    let mut publisher = context.socket::<Pub>();
    publisher.bind("tcp://*:5557")?;

    // Start state manager and wait for synchronization signal
    let mut updates = context.socket::<Pair>();
    updates.bind("inproc://state")?;
    let mut pipe = context.socket::<Pair>();
    pipe.connect("inproc://state")?;
    let manager = context.clone();
    thread::spawn(move || state_manager(&manager, pipe));
    updates.recv();

    let mut sequence = 0;
    loop {
        // Distribute as key-value message
        sequence += 1;
        let mut kvmsg = KvMsg::new(sequence);
        kvmsg.set_key(randof(10_000).to_string());
        kvmsg.set_body(randof(1_000_000).to_string());
        publisher.send(kvmsg.to_message());
        updates.send(kvmsg.to_message());
    }
}

// The state manager task maintains the state and handles requests from
// clients for snapshots
fn state_manager(context: &Context, mut pipe: Socket<Pair>) -> TaskResult {
    let mut kvmap = HashMap::new();

    pipe.send("READY");
    let mut snapshot = context.socket::<Router>();
    snapshot.bind("tcp://*:5556")?;

    // Current snapshot version number
    let mut sequence = 0;
    loop {
        let [update, request] = zmq::poll([pipe.readable(), snapshot.readable()]);

        // Apply state update from main thread
        if update && let Some(kvmsg) = KvMsg::from_message(&pipe.recv()) {
            sequence = kvmsg.sequence();
            kvmsg.store(&mut kvmap);
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
            println!("Sending state shapshot={sequence}");
            let mut kvmsg = KvMsg::new(sequence);
            kvmsg.set_key("KTHXBAI");
            kvmsg.set_body("");
            snapshot.send(&identity, kvmsg.to_message())?;
        }
    }
}
