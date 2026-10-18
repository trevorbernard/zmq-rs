//! ROUTER-to-DEALER: like rtreq, but the workers are DEALERs that add and
//! strip the empty envelope delimiter themselves.

use std::thread;
use std::time::{Duration, Instant};

use zguide::{body, randof, random_identity, sleep_ms};
use zmq::{Context, Dealer, Message, Router, SocketOptions};

const NBR_WORKERS: usize = 10;

fn worker_task(context: Context) -> Result<(), zmq::Error> {
    let mut worker = context.socket_with::<Dealer>(SocketOptions {
        identity: random_identity(),
        ..SocketOptions::default()
    });
    worker.connect("tcp://localhost:5671")?;

    let mut total = 0;
    loop {
        // Tell the broker we're ready for work
        let mut ready = Message::new("");
        ready.push("Hi Boss");
        worker.send(ready);

        // Get workload from broker, after the envelope delimiter
        if body(&worker.recv()) == "Fired!" {
            println!("Completed: {total} tasks");
            return Ok(());
        }
        total += 1;

        // Do some random work
        sleep_ms(randof(500) + 1);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut broker = context.socket::<Router>();
    broker.bind("tcp://*:5671")?;

    let workers: Vec<_> = (0..NBR_WORKERS)
        .map(|_| {
            let context = context.clone();
            thread::spawn(move || worker_task(context))
        })
        .collect();

    // Run for five seconds and then tell workers to end
    let end_time = Instant::now() + Duration::from_secs(5);
    let mut workers_fired = 0;
    while workers_fired < NBR_WORKERS {
        // Next message gives us least recently used worker
        let (identity, _) = broker.recv();
        let mut reply = Message::new("");
        if Instant::now() < end_time {
            reply.push("Work harder");
        } else {
            reply.push("Fired!");
            workers_fired += 1;
        }
        broker.send(&identity, reply)?;
    }
    for worker in workers {
        worker.join().expect("worker panicked")?;
    }
    Ok(())
}
