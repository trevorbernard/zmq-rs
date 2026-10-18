//! Multithreaded Hello World server: a proxy hands requests from
//! tcp://*:5555 to five worker threads over inproc.

use std::thread;

use zguide::{body, sleep_ms};
use zmq::{Context, Dealer, Proxy, Rep, Router};

fn worker_routine(context: Context) -> Result<(), zmq::Error> {
    let mut receiver = context.socket::<Rep>();
    receiver.connect("inproc://workers")?;

    loop {
        let request = receiver.recv();
        println!("Received request: [{}]", body(request.body()));
        sleep_ms(1000);
        request.reply("World");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;

    // Socket to talk to clients
    let mut clients = context.socket::<Router>();
    clients.bind("tcp://*:5555")?;

    // Socket to talk to workers
    let mut workers = context.socket::<Dealer>();
    workers.bind("inproc://workers")?;

    for _ in 0..5 {
        let context = context.clone();
        thread::spawn(move || worker_routine(context));
    }
    // Connect work threads to client threads via a queue proxy
    Proxy::new(&mut clients, &mut workers).run();
    Ok(())
}
