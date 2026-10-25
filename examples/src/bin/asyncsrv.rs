//! Asynchronous client-to-server (DEALER to ROUTER): three clients send a
//! request a second and print replies as they arrive, while a server deals
//! requests out to a pool of workers, each sending back zero to four
//! replies.
//!
//! While this example runs in a single process, that is to make it easier
//! to start and stop the example. Each task conceptually acts as a
//! separate process.

use std::thread;
use std::time::Duration;

use zguide::{body, randof, sleep_ms};
use zmq::{Context, Dealer, Identity, Proxy, Router, SocketOptions};

type TaskResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// A client: connects to the server and sends a request once per second,
/// printing replies as they arrive. Each client has a random identity, to
/// make tracing easier.
fn client_task(context: Context) -> TaskResult {
    let name = format!("{:04X}-{:04X}", randof(0x10000), randof(0x10000));
    let mut client = context.socket_with::<Dealer>(SocketOptions {
        identity: Identity::new(name.clone())?,
        ..SocketOptions::default()
    });
    client.connect("tcp://localhost:5570")?;

    let mut request_nbr = 0;
    loop {
        // Tick once per second, pulling in arriving messages
        for _centitick in 0..100 {
            if let Some(reply) = client.recv_timeout(Duration::from_millis(10)) {
                println!("{name} {}", body(&reply));
            }
        }
        request_nbr += 1;
        client.send(format!("request #{request_nbr}"));
    }
}

/// The server: deals requests out to a pool of workers and routes their
/// replies back to clients. One worker handles one request at a time, but
/// one client can talk to several workers at once.
fn server_task(context: Context) -> TaskResult {
    let mut frontend = context.socket::<Router>();
    frontend.bind("tcp://*:5570")?;
    let mut backend = context.socket::<Dealer>();
    backend.bind("inproc://backend")?;

    // Launch pool of worker threads, precise number is not critical
    for _ in 0..5 {
        let context = context.clone();
        thread::spawn(move || server_worker(context));
    }

    // Connect backend to frontend via a proxy
    Proxy::new(&mut frontend, &mut backend).run();
    Ok(())
}

/// A worker: works on one request at a time and sends a random number of
/// replies back, with random delays between them.
fn server_worker(context: Context) -> TaskResult {
    let mut worker = context.socket::<Dealer>();
    worker.connect("inproc://backend")?;

    loop {
        // The DEALER socket gives us the reply envelope and message
        let request = worker.recv();

        // Send 0..4 replies back
        for _ in 0..randof(5) {
            // Sleep for some fraction of a second
            sleep_ms(randof(1000) + 1);
            worker.send(request.clone());
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    for _ in 0..3 {
        let context = context.clone();
        thread::spawn(move || client_task(context));
    }
    thread::spawn(move || server_task(context));

    // Run for 5 seconds then quit
    sleep_ms(5 * 1000);
    Ok(())
}
