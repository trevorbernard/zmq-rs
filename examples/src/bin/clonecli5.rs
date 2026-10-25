//! Clone client model five: as model four, with kvmsg's properties. Each
//! update it sends lives for a random time-to-live of up to 30 seconds.
//!
//! The guide prints how many updates came in when interrupted; without a
//! signal handler this runs until killed.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use zguide::kvmsg::KvMsg;
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

    // Get state snapshot
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
            kvmsg.set_prop("ttl", randof(30).to_string());
            publisher.send(kvmsg.to_message());
            alarm = Instant::now() + Duration::from_secs(1);
        }
    }
}
