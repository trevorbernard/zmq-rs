//! Majordomo Protocol broker: a minimal implementation of
//! <http://rfc.zeromq.org/spec:7> and <http://rfc.zeromq.org/spec:8>.
//!
//! The guide polls its one socket with a heartbeat timeout; this waits on
//! it with `recv_timeout`.

use std::collections::{HashMap, VecDeque};
use std::env;
use std::time::{Duration, Instant};

use zguide::dump;
use zguide::mdp::{Command, MDPC_CLIENT, MDPW_WORKER, log, message};
use zmq::{Context, Frame, Message, Router, RoutingId, Socket};

// We'd normally pull these from config data
const HEARTBEAT_LIVENESS: u32 = 3; // 3-5 is reasonable
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(2500);
const HEARTBEAT_EXPIRY: Duration = HEARTBEAT_INTERVAL.saturating_mul(HEARTBEAT_LIVENESS);

/// A single service and the requests and workers waiting on it.
#[derive(Debug, Default)]
struct Service {
    /// Client requests, each `[client, "", body...]`
    requests: VecDeque<Vec<Frame>>,
    /// Idle workers
    waiting: VecDeque<RoutingId>,
    /// How many workers we have
    workers: usize,
}

/// A single worker, idle or active.
#[derive(Debug)]
struct Worker {
    /// Identity of worker as hex, for logging
    id_string: String,
    /// Owning service, if known
    service: Option<String>,
    /// When worker expires, if no heartbeat
    expiry: Instant,
}

#[derive(Debug)]
struct Broker {
    socket: Socket<Router>,
    verbose: bool,
    services: HashMap<String, Service>,
    workers: HashMap<RoutingId, Worker>,
    /// Idle workers, oldest first
    waiting: VecDeque<RoutingId>,
    heartbeat_at: Instant,
}

impl Broker {
    fn new(context: &Context, verbose: bool) -> Self {
        Self {
            socket: context.socket::<Router>(),
            verbose,
            services: HashMap::new(),
            workers: HashMap::new(),
            waiting: VecDeque::new(),
            heartbeat_at: Instant::now() + HEARTBEAT_INTERVAL,
        }
    }

    /// Binds the broker to `endpoint`. MDP uses a single socket for both
    /// clients and workers.
    fn bind(&mut self, endpoint: &str) -> Result<(), zmq::Error> {
        self.socket.bind(endpoint)?;
        log(&format!("I: MDP broker/0.2.0 is active at {endpoint}"));
        Ok(())
    }

    /// Processes one READY, REPLY, HEARTBEAT, or DISCONNECT message sent
    /// by a worker.
    fn worker_msg(&mut self, sender: RoutingId, mut frames: VecDeque<Frame>) {
        let command = frames.pop_front();
        let worker_ready = self.workers.contains_key(&sender);
        self.worker_require(&sender);

        match command.as_deref().and_then(Command::parse) {
            Some(Command::Ready) => {
                let service = frames
                    .pop_front()
                    .map(|f| String::from_utf8_lossy(&f).into_owned());
                match service {
                    // Not first command in session, or reserved service name
                    _ if worker_ready => self.worker_delete(&sender, true),
                    Some(name) if name.starts_with("mmi.") => self.worker_delete(&sender, true),
                    None => self.worker_delete(&sender, true),
                    Some(name) => {
                        // Attach worker to service and mark as idle
                        self.service_require(&name).workers += 1;
                        if let Some(worker) = self.workers.get_mut(&sender) {
                            worker.service = Some(name);
                        }
                        self.worker_waiting(&sender);
                    }
                }
            }
            Some(Command::Reply) => {
                let service = self.workers.get(&sender).and_then(|w| w.service.clone());
                match service {
                    Some(service) if worker_ready => {
                        // Remove and save client return envelope and insert
                        // the protocol header and service name, then rewrap
                        // envelope.
                        let client = frames.pop_front().and_then(|f| RoutingId::new(f).ok());
                        frames.pop_front();
                        if let Some(client) = client {
                            let mut reply =
                                vec![Frame::new(), Frame::from(MDPC_CLIENT), Frame::from(service)];
                            reply.extend(frames);
                            let _ = self.socket.try_send(&client, message(reply));
                        }
                        self.worker_waiting(&sender);
                    }
                    _ => self.worker_delete(&sender, true),
                }
            }
            Some(Command::Heartbeat) => {
                if worker_ready {
                    if let Some(worker) = self.workers.get_mut(&sender) {
                        worker.expiry = Instant::now() + HEARTBEAT_EXPIRY;
                    }
                } else {
                    self.worker_delete(&sender, true);
                }
            }
            Some(Command::Disconnect) => self.worker_delete(&sender, false),
            _ => {
                log("E: invalid input message");
                if let Ok(msg) = Message::try_from(Vec::from(frames)) {
                    dump(&msg);
                }
            }
        }
    }

    /// Processes a request from a client. We implement MMI requests
    /// directly here (at present, only the mmi.service request).
    fn client_msg(&mut self, sender: RoutingId, mut frames: VecDeque<Frame>) {
        // Service name + body
        let Some(service_frame) = frames.pop_front().filter(|_| !frames.is_empty()) else {
            log("E: invalid client message");
            return;
        };
        let name = String::from_utf8_lossy(&service_frame).into_owned();

        // If we got a MMI service request, process that internally
        if name.starts_with("mmi.") {
            let return_code = if name == "mmi.service" {
                let wanted = frames
                    .back()
                    .map(|f| String::from_utf8_lossy(f).into_owned());
                match wanted.and_then(|wanted| self.services.get(&wanted)) {
                    Some(service) if service.workers > 0 => "200",
                    _ => "404",
                }
            } else {
                "501"
            };
            frames.pop_back();
            frames.push_back(Frame::from(return_code));
            let mut reply = vec![Frame::new(), Frame::from(MDPC_CLIENT), service_frame];
            reply.extend(frames);
            let _ = self.socket.try_send(&sender, message(reply));
        } else {
            // Else dispatch the message to the requested service, with the
            // client's return envelope
            let mut request = vec![Frame::from(sender.as_bytes().to_vec()), Frame::new()];
            request.extend(frames);
            self.service_require(&name).requests.push_back(request);
            self.service_dispatch(&name);
        }
    }

    /// Deletes idle workers that haven't pinged us in a while. Workers are
    /// held oldest first, so we stop at the first live one.
    fn purge(&mut self) {
        while let Some(id) = self.waiting.front().cloned() {
            let Some(worker) = self.workers.get(&id) else {
                self.waiting.pop_front();
                continue;
            };
            if Instant::now() < worker.expiry {
                break; // Worker is alive, we're done here
            }
            if self.verbose {
                log(&format!("I: deleting expired worker: {}", worker.id_string));
            }
            self.worker_delete(&id, false);
        }
    }

    /// Locates a service by name, creating it if there is none.
    fn service_require(&mut self, name: &str) -> &mut Service {
        if !self.services.contains_key(name) && self.verbose {
            log(&format!("I: added service: {name}"));
        }
        self.services.entry(name.to_owned()).or_default()
    }

    /// Sends the service's requests to its waiting workers.
    fn service_dispatch(&mut self, name: &str) {
        self.purge();
        loop {
            let Some(service) = self.services.get_mut(name) else {
                return;
            };
            if service.waiting.is_empty() || service.requests.is_empty() {
                return;
            }
            let worker = service.waiting.pop_front().expect("not empty");
            let request = service.requests.pop_front().expect("not empty");
            self.waiting.retain(|id| *id != worker);
            self.worker_send(&worker, Command::Request, None, request);
        }
    }

    /// Locates a worker by identity, creating it if there is none.
    fn worker_require(&mut self, identity: &RoutingId) {
        if self.workers.contains_key(identity) {
            return;
        }
        let id_string: String = identity
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect();
        if self.verbose {
            log(&format!("I: registering new worker: {id_string}"));
        }
        self.workers.insert(
            identity.clone(),
            Worker {
                id_string,
                service: None,
                expiry: Instant::now() + HEARTBEAT_EXPIRY,
            },
        );
    }

    /// Deletes a worker, first telling it to disconnect if `disconnect`.
    fn worker_delete(&mut self, identity: &RoutingId, disconnect: bool) {
        if disconnect {
            self.worker_send(identity, Command::Disconnect, None, Vec::new());
        }
        if let Some(worker) = self.workers.remove(identity)
            && let Some(service) = worker.service.and_then(|name| self.services.get_mut(&name))
        {
            service.waiting.retain(|id| id != identity);
            service.workers -= 1;
        }
        self.waiting.retain(|id| id != identity);
    }

    /// Sends `command` with `option` and `body` to a worker.
    fn worker_send(
        &mut self,
        identity: &RoutingId,
        command: Command,
        option: Option<&str>,
        body: Vec<Frame>,
    ) {
        // Stack protocol envelope to start of message
        let mut frames = vec![Frame::new(), Frame::from(MDPW_WORKER), command.frame()];
        frames.extend(option.map(|option| Frame::from(option.to_owned())));
        frames.extend(body);
        let msg = message(frames);
        if self.verbose {
            log(&format!("I: sending {} to worker", command.name()));
            dump(&msg);
        }
        let _ = self.socket.try_send(identity, msg);
    }

    /// This worker is now waiting for work.
    fn worker_waiting(&mut self, identity: &RoutingId) {
        let Some(worker) = self.workers.get_mut(identity) else {
            return;
        };
        worker.expiry = Instant::now() + HEARTBEAT_EXPIRY;
        let Some(name) = worker.service.clone() else {
            return;
        };
        // Queue to broker and service waiting lists
        self.waiting.push_back(identity.clone());
        self.service_require(&name)
            .waiting
            .push_back(identity.clone());
        self.service_dispatch(&name);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let verbose = env::args().nth(1).as_deref() == Some("-v");
    let context = Context::new()?;
    let mut broker = Broker::new(&context, verbose);
    broker.bind("tcp://*:5555")?;

    // Get and process messages forever
    loop {
        if let Some((sender, msg)) = broker.socket.recv_timeout(HEARTBEAT_INTERVAL) {
            if verbose {
                log("I: received message:");
                dump(&msg);
            }
            let mut frames: VecDeque<Frame> = msg.into_iter().collect();
            let _empty = frames.pop_front();
            let header = frames.pop_front().unwrap_or_default();
            if *header == *MDPC_CLIENT.as_bytes() {
                broker.client_msg(sender, frames);
            } else if *header == *MDPW_WORKER.as_bytes() {
                broker.worker_msg(sender, frames);
            } else {
                log("E: invalid message:");
                if let Ok(msg) = Message::try_from(Vec::from(frames)) {
                    dump(&msg);
                }
            }
        }
        // Disconnect and delete any expired workers
        // Send heartbeats to idle workers if needed
        if Instant::now() > broker.heartbeat_at {
            broker.purge();
            for worker in broker.waiting.clone() {
                broker.worker_send(&worker, Command::Heartbeat, None, Vec::new());
            }
            broker.heartbeat_at = Instant::now() + HEARTBEAT_INTERVAL;
        }
    }
}
