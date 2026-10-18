//! Round-trip demonstrator: measures synchronous and asynchronous
//! round trips from a client through a broker to a worker and back.

use std::thread;
use std::time::Instant;

use zguide::sleep_ms;
use zmq::{Context, Dealer, Pair, Proxy};

fn client_task(context: Context, mut pipe: zmq::Socket<Pair>) -> Result<(), zmq::Error> {
    let mut client = context.socket::<Dealer>();
    client.connect("tcp://localhost:5555")?;
    println!("Setting up test...");
    sleep_ms(100);

    println!("Synchronous round-trip test...");
    let start = Instant::now();
    for _ in 0..10_000 {
        client.send("hello");
        client.recv();
    }
    println!(
        " {:.0} calls/second",
        10_000.0 / start.elapsed().as_secs_f64()
    );

    println!("Asynchronous round-trip test...");
    let start = Instant::now();
    for _ in 0..100_000 {
        client.send("hello");
    }
    for _ in 0..100_000 {
        client.recv();
    }
    println!(
        " {:.0} calls/second",
        100_000.0 / start.elapsed().as_secs_f64()
    );
    pipe.send("done");
    Ok(())
}

// The worker uses its own context, as the guide's does.
fn worker_task() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let context = Context::new()?;
    let mut worker = context.socket::<Dealer>();
    worker.connect("tcp://localhost:5556")?;

    loop {
        let message = worker.recv();
        worker.send(message);
    }
}

// The broker runs a proxy between the client and the worker, in its own
// context.
fn broker_task() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let context = Context::new()?;
    let mut frontend = context.socket::<Dealer>();
    frontend.bind("tcp://*:5555")?;
    let mut backend = context.socket::<Dealer>();
    backend.bind("tcp://*:5556")?;
    Proxy::new(&mut frontend, &mut backend).run();
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut client = context.socket::<Pair>();
    client.bind("inproc://client")?;
    let mut pipe = context.socket::<Pair>();
    pipe.connect("inproc://client")?;

    thread::spawn({
        let context = context.clone();
        move || client_task(context, pipe)
    });
    thread::spawn(worker_task);
    thread::spawn(broker_task);

    // Wait for the client to signal it's done
    client.recv();
    Ok(())
}
