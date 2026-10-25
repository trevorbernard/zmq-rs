//! Broker peering simulation (part 1): prototypes the state flow. Each
//! broker publishes how many workers it has free and prints what its
//! peers publish.
//!
//! `peering1 me {you}...`: the first argument is this broker's name, the
//! others its peers' names.

use std::env;
use std::time::Duration;

use zguide::{randof, text};
use zmq::{Context, Message, Pub, Sub};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    let Some((me, peers)) = args.split_first() else {
        println!("syntax: peering1 me {{you}}...");
        return Ok(());
    };
    println!("I: preparing broker at {me}...");

    let context = Context::new()?;

    // Bind state backend to endpoint
    let mut statebe = context.socket::<Pub>();
    statebe.bind(&format!("ipc://{me}-state.ipc"))?;

    // Connect statefe to all peers
    let mut statefe = context.socket::<Sub>();
    statefe.subscribe("");
    for peer in peers {
        println!("I: connecting to state backend at '{peer}'");
        statefe.connect(&format!("ipc://{peer}-state.ipc"))?;
    }

    // The main loop sends out status messages to peers, and collects
    // status messages back from peers. The receive timeout defines our own
    // heartbeat
    loop {
        match statefe.recv_timeout(Duration::from_secs(1)) {
            // Handle incoming status messages
            Some(status) => {
                let peer_name = text(status.split_first().0);
                println!("{peer_name} - {} workers free", text(status.last()));
            }
            // Send random values for worker availability
            None => {
                let mut status = Message::new(me.clone());
                status.push(randof(10).to_string());
                statebe.send(status);
            }
        }
    }
}
