//! Paranoid Pirate queue: a load-balancing broker that heartbeats its
//! workers, so it notices those that crash or block.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use zguide::dump;
use zmq::{Context, Message, Router, RoutingId};

const HEARTBEAT_LIVENESS: u32 = 3; // 3-5 is reasonable
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(1000);

// Paranoid Pirate Protocol constants
const PPP_READY: &[u8] = b"\x01"; // Signals worker is ready
const PPP_HEARTBEAT: &[u8] = b"\x02"; // Signals worker heartbeat

/// A worker that is ready for work, until it expires.
struct Worker {
    identity: RoutingId,
    expiry: Instant,
}

impl Worker {
    fn new(identity: RoutingId) -> Self {
        Self {
            identity,
            expiry: Instant::now() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS,
        }
    }
}

/// Workers that are ready, oldest first.
#[derive(Default)]
struct Workers(VecDeque<Worker>);

impl Workers {
    /// Puts `worker` at the end of the ready list, in place of its earlier
    /// entry, if any.
    fn ready(&mut self, worker: Worker) {
        self.0.retain(|ready| ready.identity != worker.identity);
        self.0.push_back(worker);
    }

    /// The next available worker.
    fn next(&mut self) -> Option<RoutingId> {
        self.0.pop_front().map(|worker| worker.identity)
    }

    /// Forgets expired workers. They are held oldest first, so this stops
    /// at the first one alive.
    fn purge(&mut self) {
        let now = Instant::now();
        while self.0.front().is_some_and(|worker| worker.expiry <= now) {
            self.0.pop_front();
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut frontend = context.socket::<Router>();
    frontend.bind("tcp://*:5555")?; // For clients
    let mut backend = context.socket::<Router>();
    backend.bind("tcp://*:5556")?; // For workers

    let mut workers = Workers::default();

    // Send out heartbeats at regular intervals
    let mut heartbeat_at = Instant::now() + HEARTBEAT_INTERVAL;

    loop {
        // Poll frontend only if we have available workers
        let [from_worker, from_client] = zmq::poll_timeout(
            [
                backend.readable(),
                frontend.readable().when(!workers.0.is_empty()),
            ],
            HEARTBEAT_INTERVAL,
        )
        .unwrap_or_default();

        // Handle worker activity on backend
        if from_worker {
            // Any sign of life from worker means it's ready
            let (identity, message) = backend.recv();
            workers.ready(Worker::new(identity));

            // Validate control message, or return reply to client: a
            // client's routing id, an empty frame and the reply
            if message.len().get() == 1 {
                let control = message.last();
                if **control != *PPP_READY && **control != *PPP_HEARTBEAT {
                    println!("E: invalid message from worker");
                    dump(&message);
                }
            } else {
                let (client, reply) = message.into_parts();
                let _ = frontend.try_send(&RoutingId::new(client)?, Message::try_from(reply)?);
            }
        }
        if from_client {
            // Now get next client request, route to next worker
            let (client, request) = frontend.recv();
            let worker = workers.next().ok_or("no worker is free")?;
            let mut message = Message::new(client.as_bytes().to_vec());
            message.extend(request);
            let _ = backend.try_send(&worker, message);
        }

        // We handle heartbeating after any socket activity. First, we send
        // heartbeats to any idle workers if it's time. Then, we purge any
        // dead workers
        if Instant::now() >= heartbeat_at {
            for worker in &workers.0 {
                let _ = backend.try_send(&worker.identity, PPP_HEARTBEAT);
            }
            heartbeat_at = Instant::now() + HEARTBEAT_INTERVAL;
        }
        workers.purge();
    }
}
