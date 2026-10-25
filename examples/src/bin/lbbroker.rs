//! Load-balancing broker: clients and workers are threads here, to keep
//! the example in one process. Workers say when they are ready, and the
//! broker hands each request to the worker that has waited longest.

use std::collections::VecDeque;
use std::thread;

use zguide::{body, random_identity};
use zmq::{Context, Message, Req, Router, RoutingId, SocketOptions};

const NBR_CLIENTS: usize = 10;
const NBR_WORKERS: usize = 3;

type TaskResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn with_identity() -> SocketOptions {
    SocketOptions {
        identity: random_identity(),
        ..SocketOptions::default()
    }
}

/// A client: a REQ that sends one request and prints the reply.
fn client_task(context: Context) -> TaskResult {
    let mut client = context.socket_with::<Req>(with_identity());
    client.connect("ipc://frontend.ipc")?;

    let reply = client.request("HELLO")?;
    println!("Client: {}", body(&reply));
    Ok(())
}

/// A worker: a REQ that says it is ready, then answers each request it is
/// handed with OK. Each request comes as the client's routing id, an empty
/// frame, and the request.
fn worker_task(context: Context) -> TaskResult {
    let mut worker = context.socket_with::<Req>(with_identity());
    worker.connect("ipc://backend.ipc")?;

    // Tell broker we're ready for work
    let mut request = worker.request("READY")?;
    loop {
        let (client, rest) = request.into_parts();
        let rest = Message::try_from(rest)?;
        println!("Worker: {}", body(&rest));
        let mut reply = Message::new(client);
        reply.push("");
        reply.push("OK");
        request = worker.request(reply)?;
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut frontend = context.socket::<Router>();
    frontend.bind("ipc://frontend.ipc")?;
    let mut backend = context.socket::<Router>();
    backend.bind("ipc://backend.ipc")?;

    let clients: Vec<_> = (0..NBR_CLIENTS)
        .map(|_| {
            let context = context.clone();
            thread::spawn(move || client_task(context))
        })
        .collect();
    for _ in 0..NBR_WORKERS {
        let context = context.clone();
        thread::spawn(move || worker_task(context));
    }

    // Workers that are ready, least recently used first
    let mut workers: VecDeque<RoutingId> = VecDeque::new();
    let mut clients_left = NBR_CLIENTS;
    while clients_left > 0 {
        // Always poll for worker activity on the backend; take client
        // requests only while a worker is free to handle them
        let [from_worker, from_client] = zmq::poll([
            backend.readable(),
            frontend.readable().when(!workers.is_empty()),
        ]);

        if from_worker {
            // A worker's message is its envelope delimiter, then READY or
            // a client's routing id, an empty frame and the reply
            let (worker, message) = backend.recv();
            workers.push_back(worker);
            let mut frames = message.into_iter().skip(1);
            let first = frames.next().ok_or("empty worker message")?;
            if *first != *b"READY" {
                let client = RoutingId::new(first)?;
                let reply = Message::try_from(frames.collect::<Vec<_>>())?;
                frontend.send(&client, reply)?;
                clients_left -= 1;
            }
        }
        if from_client {
            // A client's request, as its delimiter and body, goes to the
            // worker that has waited longest, behind the client's id
            let (client, request) = frontend.recv();
            let worker = workers.pop_front().ok_or("no worker is free")?;
            let mut message = Message::new("");
            message.push(client.as_bytes().to_vec());
            message.extend(request);
            backend.send(&worker, message)?;
        }
    }
    // Let every client print its reply; the workers wait forever
    for client in clients {
        client
            .join()
            .map_err(|_| "a client panicked")?
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}
