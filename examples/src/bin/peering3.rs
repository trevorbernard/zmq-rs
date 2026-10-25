//! Broker peering simulation (part 3): prototypes the full flow of status
//! and tasks. Each broker routes requests to its own workers while it has
//! any free, and otherwise to a peer broker, and tells its peers how many
//! workers it has free whenever that changes.
//!
//! `peering3 me {you}...`: the first argument is this broker's name, the
//! others its peers' names.

use std::collections::VecDeque;
use std::env;
use std::thread;
use std::time::Duration;

use zguide::{body, randof, route, sleep_ms, text, with_sender};
use zmq::{
    Context, Identity, Message, Pub, Pull, Push, Req, Router, RoutingId, SocketOptions, Sub,
};

const NBR_CLIENTS: usize = 10;
const NBR_WORKERS: usize = 5;
/// Signals worker is ready
const WORKER_READY: &[u8] = b"\x01";

type TaskResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// A client: issues a burst of requests and then sleeps for a few seconds.
/// This simulates sporadic activity; when a number of clients are active
/// at once, the local workers should be overloaded. The client uses a REQ
/// socket for requests and also pushes statistics to the monitor socket.
fn client_task(context: Context, me: String) -> TaskResult {
    let mut client = context.socket::<Req>();
    client.connect(&format!("ipc://{me}-localfe.ipc"))?;
    let mut monitor = context.socket::<Push>();
    monitor.connect(&format!("ipc://{me}-monitor.ipc"))?;

    loop {
        sleep_ms(randof(5) * 1000);
        for _ in 0..randof(15) {
            // Send request with random hex ID
            let task_id = format!("{:04X}", randof(0x10000));

            // Wait max ten seconds for a reply, then complain
            match client.request_timeout(task_id.clone(), Duration::from_secs(10)) {
                Ok(reply) => {
                    // Worker is supposed to answer us with our task id
                    let reply = body(&reply);
                    assert_eq!(reply, task_id);
                    monitor.send(reply);
                }
                Err(_) => {
                    monitor.send(format!("E: CLIENT EXIT - lost task {task_id}"));
                    return Ok(());
                }
            }
        }
    }
}

/// A worker: plugs into the load balancer with a REQ socket, as the stub
/// worker in other examples, and echoes each request back.
fn worker_task(context: Context, me: String) -> TaskResult {
    let mut worker = context.socket::<Req>();
    worker.connect(&format!("ipc://{me}-localbe.ipc"))?;

    // Tell broker we're ready for work
    let mut request = worker.request(WORKER_READY)?;

    // Process messages as they arrive
    loop {
        // Workers are busy for 0/1 seconds
        sleep_ms(randof(2) * 1000);
        request = worker.request(request)?;
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    let Some((me, peers)) = args.split_first() else {
        println!("syntax: peering3 me {{you}}...");
        return Ok(());
    };
    println!("I: preparing broker at {me}...");

    // The local frontend talks to clients, and our local backend talks to
    // workers. The cloud frontend talks to peer brokers as if they were
    // clients, and the cloud backend talks to peer brokers as if they were
    // workers. The state backend publishes regular state messages, and the
    // state frontend subscribes to all state backends to collect these
    // messages. Finally, we use a PULL monitor socket to collect printable
    // messages from tasks
    let context = Context::new()?;
    let mut localfe = context.socket::<Router>();
    localfe.bind(&format!("ipc://{me}-localfe.ipc"))?;
    let mut localbe = context.socket::<Router>();
    localbe.bind(&format!("ipc://{me}-localbe.ipc"))?;

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

    // Bind state backend to endpoint
    let mut statebe = context.socket::<Pub>();
    statebe.bind(&format!("ipc://{me}-state.ipc"))?;

    // Connect state frontend to all peers
    let mut statefe = context.socket::<Sub>();
    statefe.subscribe("");
    for peer in peers {
        println!("I: connecting to state backend at '{peer}'");
        statefe.connect(&format!("ipc://{peer}-state.ipc"))?;
    }

    // Prepare monitor socket
    let mut monitor = context.socket::<Pull>();
    monitor.bind(&format!("ipc://{me}-monitor.ipc"))?;

    // After binding and connecting all our sockets, we start our child
    // tasks - workers and clients
    for _ in 0..NBR_WORKERS {
        let (context, me) = (context.clone(), me.clone());
        thread::spawn(move || worker_task(context, me));
    }
    for _ in 0..NBR_CLIENTS {
        let (context, me) = (context.clone(), me.clone());
        thread::spawn(move || client_task(context, me));
    }

    // Queue of available workers
    let mut workers: VecDeque<RoutingId> = VecDeque::new();
    let mut cloud_capacity: usize = 0;

    // The main loop has two parts. First, we poll workers and our two
    // service sockets (statefe and monitor), in any case. If we have no
    // ready workers, then there's no point in looking at incoming
    // requests. These can remain on their internal queues
    loop {
        let primary = [
            localbe.readable(),
            cloudbe.readable(),
            statefe.readable(),
            monitor.readable(),
        ];
        // If we have no workers ready, wait indefinitely
        let ready = if workers.is_empty() {
            zmq::poll(primary)
        } else {
            zmq::poll_timeout(primary, Duration::from_secs(1)).unwrap_or_default()
        };
        let [from_worker, from_peer, state, status] = ready;

        // Track if capacity changes during this iteration
        let previous = workers.len();
        let mut reply = None;

        if from_worker {
            let (worker, message) = localbe.recv();
            workers.push_back(worker);
            // Its envelope delimiter is of no further use. If it's READY,
            // don't route the message any further
            let frames: Vec<_> = message.into_iter().skip(1).collect();
            if frames.first().is_some_and(|frame| **frame != *WORKER_READY) {
                reply = Some(Message::try_from(frames)?);
            }
        }
        // Or handle reply from peer broker; we don't use the peer broker
        // identity for anything
        else if from_peer {
            reply = Some(cloudbe.recv().1);
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

        // If we have input messages on our statefe or monitor sockets, we
        // can process these immediately
        if state {
            let capacity = statefe.recv();
            cloud_capacity = text(capacity.last()).parse().unwrap_or(0);
        }
        if status {
            println!("{}", body(&monitor.recv()));
        }

        // Now route as many clients requests as we can handle. If we have
        // local capacity, we poll both localfe and cloudfe. If we have
        // cloud capacity only, we poll just localfe. We route any request
        // locally if we can, else we route to the cloud
        while workers.len() + cloud_capacity > 0 {
            let frontends = [
                localfe.readable(),
                cloudfe.readable().when(!workers.is_empty()),
            ];
            let request = match zmq::poll_timeout(frontends, Duration::ZERO) {
                Some([true, _]) => with_sender(localfe.recv()),
                Some([_, true]) => with_sender(cloudfe.recv()),
                // No work, go back to primary
                _ => break,
            };

            if let Some(worker) = workers.pop_front() {
                let mut wrapped = Message::new("");
                wrapped.extend(request);
                let _ = localbe.try_send(&worker, wrapped);
            } else {
                // Route to random broker peer
                let peer = &peers[randof(peers.len() as u64) as usize];
                let _ = cloudbe.try_send(&RoutingId::new(peer.clone())?, request);
            }
        }

        // We broadcast capacity messages to other peers; to reduce
        // chatter, we do this only if our capacity changed
        if workers.len() != previous {
            // We stick our own identity onto the envelope, then broadcast
            // new capacity
            let mut capacity = Message::new(me.clone());
            capacity.push(workers.len().to_string());
            statebe.send(capacity);
        }
    }
}
