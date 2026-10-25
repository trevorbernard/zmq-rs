//! Simple Pirate broker: the load-balancing broker with no reliability of
//! its own. It depends on the client for recovery. Runs forever.

use std::collections::VecDeque;

use zmq::{Context, Message, Router, RoutingId};

/// Signals worker is ready
const WORKER_READY: &[u8] = b"\x01";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut frontend = context.socket::<Router>();
    frontend.bind("tcp://*:5555")?; // For clients
    let mut backend = context.socket::<Router>();
    backend.bind("tcp://*:5556")?; // For workers

    // Queue of available workers
    let mut workers: VecDeque<RoutingId> = VecDeque::new();

    loop {
        // Poll frontend only if we have available workers
        let [from_worker, from_client] = zmq::poll([
            backend.readable(),
            frontend.readable().when(!workers.is_empty()),
        ]);

        // Handle worker activity on backend
        if from_worker {
            // Use worker identity for load-balancing; a worker's message
            // is its envelope delimiter, then READY or a client's routing
            // id, an empty frame and the reply
            let (worker, message) = backend.recv();
            workers.push_back(worker);
            let mut frames = message.into_iter().skip(1);
            let Some(first) = frames.next() else {
                continue;
            };
            // Forward message to client if it's not a READY; a client
            // that went away meanwhile gets nothing, as with libzmq
            if *first != *WORKER_READY {
                let client = RoutingId::new(first)?;
                let reply = Message::try_from(frames.collect::<Vec<_>>())?;
                let _ = frontend.try_send(&client, reply);
            }
        }
        if from_client {
            // Get client request, route to first available worker
            let (client, request) = frontend.recv();
            let worker = workers.pop_front().ok_or("no worker is free")?;
            let mut message = Message::new("");
            message.push(client.as_bytes().to_vec());
            message.extend(request);
            let _ = backend.try_send(&worker, message);
        }
    }
}
