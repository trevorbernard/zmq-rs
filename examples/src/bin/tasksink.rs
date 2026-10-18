//! Task sink: binds PULL to tcp://*:5558 and collects results from workers.

use std::io::{self, Write};
use std::time::Instant;

use zmq::{Context, Pull};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut receiver = context.socket::<Pull>();
    receiver.bind("tcp://*:5558")?;

    // Wait for start of batch
    receiver.recv();
    let start = Instant::now();

    for task_nbr in 0..100 {
        receiver.recv();
        print!("{}", if task_nbr % 10 == 0 { ":" } else { "." });
        io::stdout().flush()?;
    }
    println!("Total elapsed time: {} msec", start.elapsed().as_millis());
    Ok(())
}
