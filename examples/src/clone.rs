//! Clone client API, as the guide's `clone.c`: a distributed key-value map
//! kept in step with one of up to two clone servers, failing over to the
//! other when the one in use goes quiet.
//!
//! The application's calls go down a pipe to an agent thread, which talks
//! to the servers.

use std::collections::HashMap;
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Dealer, Message, Pair, Pub, Socket, Sub};

use crate::kvmsg::KvMsg;
use crate::mdp::log;
use crate::text;

/// Number of servers to which we will talk to.
const SERVER_MAX: usize = 2;

/// Server considered dead if silent for this long.
const SERVER_TTL: Duration = Duration::from_secs(5);

// =====================================================================
// Synchronous part, works in our application thread

#[derive(Debug)]
pub struct CloneClient {
    // Pipe through to clone agent
    pipe: Socket<Pair>,
}

impl CloneClient {
    /// Starts the agent, with a context of its own for the pipe and the
    /// servers.
    pub fn new() -> Result<Self, zmq::Error> {
        let context = Context::new().map_err(zmq::Error::Io)?;
        let mut pipe = context.socket::<Pair>();
        pipe.bind("inproc://clone-agent")?;
        let mut agent_pipe = context.socket::<Pair>();
        agent_pipe.connect("inproc://clone-agent")?;
        thread::spawn(move || Agent::new(context, agent_pipe).run());
        Ok(Self { pipe })
    }

    /// Specifies the subtree for snapshots and updates, which must come
    /// before connecting to a server, as the subtree is the first thing
    /// sent to it.
    pub fn subtree(&mut self, subtree: &str) {
        self.pipe.send(command(&["SUBTREE", subtree]));
    }

    /// Connects to a new server endpoint, `address` being
    /// `tcp://host` and `service` its port. We can connect to at most two
    /// servers.
    pub fn connect(&mut self, address: &str, service: &str) {
        self.pipe.send(command(&["CONNECT", address, service]));
    }

    /// Sets a new value in the shared map, for `ttl` seconds if not zero.
    pub fn set(&mut self, key: &str, value: &str, ttl: u32) {
        self.pipe
            .send(command(&["SET", key, value, &ttl.to_string()]));
    }

    /// Looks up `key` in the distributed map; `""` if it has no value.
    pub fn get(&mut self, key: &str) -> String {
        self.pipe.send(command(&["GET", key]));
        text(self.pipe.recv().split_first().0)
    }
}

fn command(frames: &[&str]) -> Message {
    let mut message = Message::new(frames[0].to_owned());
    message.extend(frames[1..].iter().map(|frame| (*frame).to_owned().into()));
    message
}

// =====================================================================
// Asynchronous part, works in the background agent thread

// The backend agent manages a set of servers, which we implement using
// our simple class model
struct Server {
    // Server address and port
    address: String,
    port: u16,
    // Snapshot socket
    snapshot: Socket<Dealer>,
    // Incoming updates
    subscriber: Socket<Sub>,
    // When server expires
    expiry: Instant,
    // How many snapshot requests made?
    requests: u32,
}

impl Server {
    fn new(context: &Context, address: &str, port: u16, subtree: &str) -> Result<Self, zmq::Error> {
        log(&format!("I: adding server {address}:{port}..."));
        let mut snapshot = context.socket::<Dealer>();
        snapshot.connect(&format!("{address}:{port}"))?;
        let mut subscriber = context.socket::<Sub>();
        subscriber.connect(&format!("{address}:{}", port + 1))?;
        subscriber.subscribe(subtree);
        subscriber.subscribe("HUGZ");
        Ok(Self {
            address: address.to_owned(),
            port,
            snapshot,
            subscriber,
            expiry: Instant::now() + SERVER_TTL,
            requests: 0,
        })
    }
}

// States we can be in
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    // Before asking server for state
    Initial,
    // Getting state from server
    Syncing,
    // Getting new updates from server
    Active,
}

struct Agent {
    context: Context,
    // Pipe back to application
    pipe: Socket<Pair>,
    // Actual key/value table
    kvmap: HashMap<String, KvMsg>,
    // Subtree specification, if any
    subtree: String,
    servers: Vec<Server>,
    state: State,
    // If active, server 0 or 1
    cur_server: usize,
    // Last kvmsg processed
    sequence: i64,
    // Outgoing updates
    publisher: Socket<Pub>,
}

impl Agent {
    fn new(context: Context, pipe: Socket<Pair>) -> Self {
        let publisher = context.socket::<Pub>();
        Self {
            context,
            pipe,
            kvmap: HashMap::new(),
            subtree: String::new(),
            servers: Vec::new(),
            state: State::Initial,
            cur_server: 0,
            sequence: 0,
            publisher,
        }
    }

    // The asynchronous agent manages a server pool and handles the
    // request-reply dialog when the application asks for it
    fn run(mut self) {
        loop {
            if self.state == State::Initial
                && let Some(server) = self.servers.get_mut(self.cur_server)
            {
                // In this state we ask the server for a snapshot, if we
                // have a server to talk to...
                log(&format!(
                    "I: waiting for server at {}:{}...",
                    server.address, server.port
                ));
                if server.requests < 2 {
                    let mut request = Message::new("ICANHAZ?");
                    request.push(self.subtree.clone());
                    server.snapshot.send(request);
                    server.requests += 1;
                }
                server.expiry = Instant::now() + SERVER_TTL;
                self.state = State::Syncing;
            }

            // We're ready to process incoming messages; if nothing at all
            // comes from our server within the timeout, that means the
            // server is dead. While syncing we read from snapshot, and
            // while active from subscriber, where the server gives HUGZ
            let event = match self.servers.get_mut(self.cur_server) {
                None => {
                    zmq::poll([self.pipe.readable()]);
                    Event::Control
                }
                Some(server) => {
                    let syncing = self.state == State::Syncing;
                    match zmq::poll_timeout(
                        [
                            self.pipe.readable(),
                            server.snapshot.readable().when(syncing),
                            server.subscriber.readable().when(!syncing),
                        ],
                        server.expiry.saturating_duration_since(Instant::now()),
                    ) {
                        None => Event::Silence,
                        Some([true, _, _]) => Event::Control,
                        Some([_, true, _]) => Event::Server(server.snapshot.recv()),
                        Some(_) => Event::Server(server.subscriber.recv()),
                    }
                }
            };

            match event {
                Event::Control => self.control_message(),
                Event::Server(message) => self.server_message(&message),
                Event::Silence => {
                    // Server has died, failover to next
                    let server = &self.servers[self.cur_server];
                    log(&format!(
                        "I: server at {}:{} didn't give HUGZ",
                        server.address, server.port
                    ));
                    self.cur_server = (self.cur_server + 1) % self.servers.len();
                    self.state = State::Initial;
                }
            }
        }
    }

    // Here we handle the different control messages from the frontend;
    // SUBTREE, CONNECT, SET, and GET
    fn control_message(&mut self) {
        let message = self.pipe.recv();
        let frames: Vec<String> = message.iter().map(|frame| text(frame)).collect();
        match frames.iter().map(String::as_str).collect::<Vec<_>>()[..] {
            ["SUBTREE", subtree] => self.subtree = subtree.to_owned(),
            ["CONNECT", address, service] => {
                if self.servers.len() < SERVER_MAX {
                    let Ok(port) = service.parse::<u16>() else {
                        log(&format!("E: bad service {service}"));
                        return;
                    };
                    match Server::new(&self.context, address, port, &self.subtree) {
                        Ok(server) => self.servers.push(server),
                        Err(err) => log(&format!("E: cannot connect to {address}: {err}")),
                    }
                    // We broadcast updates to all known servers
                    if let Err(err) = self.publisher.connect(&format!("{address}:{}", port + 2)) {
                        log(&format!("E: cannot connect to {address}: {err}"));
                    }
                } else {
                    log(&format!("E: too many servers (max. {SERVER_MAX})"));
                }
            }
            // When we set a property, we push the new key-value pair onto
            // all our connected servers
            ["SET", key, value, ttl] => {
                let mut kvmsg = KvMsg::new(0);
                kvmsg.set_key(key);
                kvmsg.set_uuid();
                kvmsg.set_body(value.to_owned());
                kvmsg.set_prop("ttl", ttl);
                self.publisher.send(kvmsg.to_message());
                kvmsg.store(&mut self.kvmap);
            }
            ["GET", key] => {
                let value = self
                    .kvmap
                    .get(key)
                    .and_then(KvMsg::body)
                    .unwrap_or_default();
                self.pipe.send(value.to_vec());
            }
            _ => log("E: bad control message"),
        }
    }

    // A snapshot entry or an update from the server in use
    fn server_message(&mut self, message: &Message) {
        let server = &mut self.servers[self.cur_server];
        // Anything from server resets its expiry time
        server.expiry = Instant::now() + SERVER_TTL;
        let Some(kvmsg) = KvMsg::from_message(message) else {
            return;
        };
        if self.state == State::Syncing {
            // Store in snapshot until we're finished
            server.requests = 0;
            if kvmsg.key() == Some("KTHXBAI") {
                self.sequence = kvmsg.sequence();
                self.state = State::Active;
                log(&format!(
                    "I: received from {}:{} snapshot={}",
                    server.address, server.port, self.sequence
                ));
            } else {
                kvmsg.store(&mut self.kvmap);
            }
        } else if kvmsg.sequence() > self.sequence {
            // Discard out-of-sequence updates, incl. HUGZ
            self.sequence = kvmsg.sequence();
            log(&format!(
                "I: received from {}:{} update={}",
                server.address, server.port, self.sequence
            ));
            kvmsg.store(&mut self.kvmap);
        }
    }
}

// What woke the agent
enum Event {
    // A command from the application
    Control,
    // A message from the server in use
    Server(Message),
    // Nothing from the server in use before it expired
    Silence,
}
