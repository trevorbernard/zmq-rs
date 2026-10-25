//! Task sink, with kill signal: as tasksink, and binds PUB to tcp://*:5559
//! to tell the workers to stop once the batch is done.

use std::io::{self, Write};
use std::time::Instant;

use zmq::{Context, Pub, Pull};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut receiver = context.socket::<Pull>();
    receiver.bind("tcp://*:5558")?;
    let mut controller = context.socket::<Pub>();
    controller.bind("tcp://*:5559")?;

    // Wait for start of batch
    receiver.recv();
    let start = Instant::now();

    for task_nbr in 0..100 {
        receiver.recv();
        print!("{}", if task_nbr % 10 == 0 { ":" } else { "." });
        io::stdout().flush()?;
    }
    println!("Total elapsed time: {} msec", start.elapsed().as_millis());

    // Send kill signal to workers
    controller.send("KILL");
    Ok(())
}
