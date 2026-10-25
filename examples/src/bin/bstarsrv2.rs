//! Binary Star server, using the bstar reactor: an echo service.
//!
//! Run as `bstarsrv2 -p` for the primary, at tcp://localhost:5001, or
//! `bstarsrv2 -b` for the backup, at tcp://localhost:5002.

use std::env;

use zguide::bstar::{BinaryStar, Role};
use zmq::Context;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let bstar = match env::args().nth(1).as_deref() {
        Some("-p") => {
            println!("I: Primary active, waiting for backup (passive)");
            BinaryStar::new(
                &context,
                Role::Primary,
                "tcp://*:5003",
                "tcp://localhost:5004",
                "tcp://*:5001",
            )?
        }
        Some("-b") => {
            println!("I: Backup passive, waiting for primary (active)");
            BinaryStar::new(
                &context,
                Role::Backup,
                "tcp://*:5004",
                "tcp://localhost:5003",
                "tcp://*:5002",
            )?
        }
        _ => {
            println!("Usage: bstarsrv2 {{ -p | -b }}");
            return Ok(());
        }
    };

    // Echo service
    bstar.start(|voter, client, request| {
        let _ = voter.try_send(&client, request);
    })?;
    Ok(())
}
