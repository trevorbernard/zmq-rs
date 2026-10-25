//! Clone client model four: as model three, but working only on the keys
//! under `/client/`. It asks the server for that subtree's snapshot,
//! subscribes to its updates, and sends its own updates under it.
//!
//! The guide prints how many updates came in when interrupted; without a
//! signal handler this runs until killed.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use zguide::kvsimple::KvMsg;
use zguide::randof;
use zmq::{Context, Dealer, Message, Push, Sub};

const SUBTREE: &str = "/client/";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Prepare our context and subscriber
    let context = Context::new()?;
    let mut snapshot = context.socket::<Dealer>();
    snapshot.connect("tcp://localhost:5556")?;
    let mut subscriber = context.socket::<Sub>();
    subscriber.subscribe(SUBTREE);
    subscriber.connect("tcp://localhost:5557")?;
    let mut publisher = context.socket::<Push>();
    publisher.connect("tcp://localhost:5558")?;

    let mut kvmap = HashMap::new();

    // We first request a state snapshot of our subtree
    let mut request = Message::new("ICANHAZ?");
    request.push(SUBTREE);
    snapshot.send(request);
    let mut sequence = loop {
        let Some(kvmsg) = KvMsg::from_message(&snapshot.recv()) else {
            continue;
        };
        if kvmsg.key() == Some("KTHXBAI") {
            println!("I: received snapshot={}", kvmsg.sequence());
            break kvmsg.sequence();
        }
        kvmsg.store(&mut kvmap);
    };

    // Now we wait for updates from the server and every so often, we
    // send a random key-value update to the server
    let mut alarm = Instant::now() + Duration::from_secs(1);
    loop {
        let tickless = alarm.saturating_duration_since(Instant::now());
        if let Some(message) = subscriber.recv_timeout(tickless)
            && let Some(kvmsg) = KvMsg::from_message(&message)
        {
            // Discard out-of-sequence kvmsgs, incl. heartbeats
            if kvmsg.sequence() > sequence {
                sequence = kvmsg.sequence();
                kvmsg.store(&mut kvmap);
                println!("I: received update={sequence}");
            }
        }
        // If we timed out, generate a random kvmsg
        if Instant::now() >= alarm {
            let mut kvmsg = KvMsg::new(0);
            kvmsg.set_key(format!("{SUBTREE}{}", randof(10_000)));
            kvmsg.set_body(randof(1_000_000).to_string());
            publisher.send(kvmsg.to_message());
            alarm = Instant::now() + Duration::from_secs(1);
        }
    }
}
