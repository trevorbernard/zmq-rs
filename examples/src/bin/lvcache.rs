//! Last value cache: subscribes to every topic from a publisher on
//! tcp://localhost:5557 and republishes on tcp://*:5558, sending each new
//! subscriber the last value of its topic at once.
//!
//! Run pathopub with `tcp://*:5557`, and pathosub with
//! `tcp://localhost:5558`.

use std::collections::HashMap;

use zguide::text;
use zmq::{Context, Frame, Message, Sub, XPub, XPubEvent};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut frontend = context.socket::<Sub>();
    frontend.connect("tcp://localhost:5557")?;
    let mut backend = context.socket::<XPub>();
    backend.bind("tcp://*:5558")?;

    // Subscribe to every single topic from publisher
    frontend.subscribe("");

    // Store last instance of each topic in a cache
    let mut cache: HashMap<Frame, Message> = HashMap::new();

    // We route topic updates from frontend to backend, and we handle
    // subscriptions by sending whatever we cached, if anything
    loop {
        let [update, subscription] = zmq::poll([frontend.readable(), backend.readable()]);

        // Any new topic data we cache and then forward
        if update {
            let message = frontend.recv();
            cache.insert(message.split_first().0.clone(), message.clone());
            backend.send(message);
        }
        // When we get a new subscription, we pull data from the cache
        if subscription && let XPubEvent::Subscribe(topic) = backend.recv() {
            println!("Sending cached topic {}", text(&topic));
            if let Some(previous) = cache.get(&topic) {
                backend.send(previous.clone());
            }
        }
    }
}
