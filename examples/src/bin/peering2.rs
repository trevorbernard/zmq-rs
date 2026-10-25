//! Broker peering simulation (part 2): prototypes the request-reply flow.
//! Each broker load-balances its own clients' requests over its own
//! workers, and sends a fifth of them to a random peer broker instead.
//!
//! `peering2 me {you}...`: the first argument is this broker's name, the
//! others its peers' names.

use std::collections::VecDeque;
use std::env;
use std::io::{self, Write};
use std::thread;
use std::time::Duration;

use zguide::{body, randof, route, sleep_ms, with_sender};
use zmq::{Context, Frame, Identity, Message, Req, Router, RoutingId, SocketOptions};

const NBR_CLIENTS: usize = 10;
const NBR_WORKERS: usize = 3;
/// Signals worker is ready
const WORKER_READY: &[u8] = b"\x01";

type TaskResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// A client: a request-reply dialog over a standard synchronous REQ
/// socket.
fn client_task(context: Context, me: String) -> TaskResult {
    let mut client = context.socket::<Req>();
    client.connect(&format!("ipc://{me}-localfe.ipc"))?;

    loop {
        // Send request, get reply
        let reply = client.request("HELLO")?;
        println!("Client: {}", body(&reply));
        sleep_ms(1000);
    }
}

/// A worker: plugs into the load balancer with a REQ socket, and answers
/// each request with OK in place of its body.
fn worker_task(context: Context, me: String) -> TaskResult {
    let mut worker = context.socket::<Req>();
    worker.connect(&format!("ipc://{me}-localbe.ipc"))?;

    // Tell broker we're ready for work
    let mut request = worker.request(WORKER_READY)?;

    // Process messages as they arrive
    loop {
        println!("Worker: {}", body(&request));
        let mut frames: Vec<Frame> = request.into_iter().collect();
        if let Some(last) = frames.last_mut() {
            *last = Frame::from("OK");
        }
        request = worker.request(Message::try_from(frames)?)?;
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    let Some((me, peers)) = args.split_first() else {
        println!("syntax: peering2 me {{you}}...");
        return Ok(());
    };
    println!("I: preparing broker at {me}...");

    let context = Context::new()?;
    let identity = Identity::new(me.clone())?;
    let named = || SocketOptions {
        identity: identity.clone(),
        ..SocketOptions::default()
    };

    // Bind cloud frontend to endpoint
    let mut cloudfe = context.socket_with::<Router>(named());
    cloudfe.bind(&format!("ipc://{me}-cloud.ipc"))?;

    // Connect cloud backend to all peers
    let mut cloudbe = context.socket_with::<Router>(named());
    for peer in peers {
        println!("I: connecting to cloud frontend at '{peer}'");
        cloudbe.connect(&format!("ipc://{peer}-cloud.ipc"))?;
    }

    // Prepare local frontend and backend
    let mut localfe = context.socket::<Router>();
    localfe.bind(&format!("ipc://{me}-localfe.ipc"))?;
    let mut localbe = context.socket::<Router>();
    localbe.bind(&format!("ipc://{me}-localbe.ipc"))?;

    // Get user to tell us when we can start...
    print!("Press Enter when all brokers are started: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut String::new())?;

    // Start local workers and clients
    for _ in 0..NBR_WORKERS {
        let (context, me) = (context.clone(), me.clone());
        thread::spawn(move || worker_task(context, me));
    }
    for _ in 0..NBR_CLIENTS {
        let (context, me) = (context.clone(), me.clone());
        thread::spawn(move || client_task(context, me));
    }

    // Here, we handle the request-reply flow. We're using load-balancing
    // to poll workers at all times, and clients only when there are one or
    // more workers available.

    // Least recently used queue of available workers
    let mut workers: VecDeque<RoutingId> = VecDeque::new();

    loop {
        // First, route any waiting replies from workers. If we have no
        // workers, wait indefinitely
        let backends = [localbe.readable(), cloudbe.readable()];
        let ready = if workers.is_empty() {
            Some(zmq::poll(backends))
        } else {
            zmq::poll_timeout(backends, Duration::from_secs(1))
        };
        let mut reply = None;
        if let Some([from_worker, from_peer]) = ready {
            // Handle reply from local worker
            if from_worker {
                let (worker, message) = localbe.recv();
                workers.push_back(worker);
                // Its envelope delimiter is of no further use. If it's
                // READY, don't route the message any further
                let frames: Vec<Frame> = message.into_iter().skip(1).collect();
                if frames.first().is_some_and(|frame| **frame != *WORKER_READY) {
                    reply = Some(Message::try_from(frames)?);
                }
            }
            // Or handle reply from peer broker; we don't use the peer
            // broker identity for anything
            else if from_peer {
                reply = Some(cloudbe.recv().1);
            }
        }
        // Route reply to cloud if it's addressed to a broker, and to the
        // client otherwise
        if let Some(reply) = reply {
            let to = reply.split_first().0;
            if peers.iter().any(|peer| **to == *peer.as_bytes()) {
                route(&mut cloudfe, reply)?;
            } else {
                route(&mut localfe, reply)?;
            }
        }

        // Now we route as many client requests as we have worker capacity
        // for. We may reroute requests from our local frontend, but not
        // from the cloud frontend. We reroute randomly now, just to test
        // things out. In the next version, we'll do this properly by
        // calculating cloud capacity
        while !workers.is_empty() {
            // We'll do peer brokers first, to prevent starvation
            let frontends = [cloudfe.readable(), localfe.readable()];
            let Some([from_peer, _]) = zmq::poll_timeout(frontends, Duration::ZERO) else {
                break; // No work, go back to backends
            };
            let (request, reroutable) = if from_peer {
                (with_sender(cloudfe.recv()), false)
            } else {
                (with_sender(localfe.recv()), true)
            };

            // If reroutable, send to cloud 20% of the time. Here we'd
            // normally use cloud status information
            if reroutable && !peers.is_empty() && randof(5) == 0 {
                // Route to random broker peer
                let peer = &peers[randof(peers.len() as u64) as usize];
                let _ = cloudbe.try_send(&RoutingId::new(peer.clone())?, request);
            } else {
                let worker = workers.pop_front().expect("a worker is free");
                let mut wrapped = Message::new("");
                wrapped.extend(request);
                let _ = localbe.try_send(&worker, wrapped);
            }
        }
    }
}
