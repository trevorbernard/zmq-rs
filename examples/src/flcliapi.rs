//! Freelance client API, as the guide's `flcliapi.c`: the Freelance
//! pattern's agent, which implements the Freelance Protocol at
//! <http://rfc.zeromq.org/spec:10>.
//!
//! The API works in two halves, a common pattern for APIs that need to run
//! in the background. One half is a frontend object the application
//! creates and works with; the other half is a backend agent that runs in
//! a background thread. The frontend talks to the backend over an inproc
//! pipe.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use zmq::{Context, Message, Pair, Router, RoutingId, Socket};

use crate::text;

/// If no server replies within this time, abandon request
const GLOBAL_TIMEOUT: Duration = Duration::from_millis(3000);
/// PING interval for servers we think are alive
const PING_INTERVAL: Duration = Duration::from_millis(2000);
/// Server considered dead if silent for this long
const SERVER_TTL: Duration = Duration::from_millis(6000);

/// A Freelance client: sends each request to a server it believes alive,
/// failing over to the next until one answers.
#[derive(Debug)]
pub struct Flcliapi {
    pipe: Socket<Pair>,
}

impl Flcliapi {
    /// A client whose agent runs on a new thread.
    pub fn new(context: &Context) -> Result<Self, zmq::Error> {
        // Each client's pipe needs a name of its own
        static PIPES: AtomicUsize = AtomicUsize::new(0);
        let endpoint = format!(
            "inproc://flcliapi-{}",
            PIPES.fetch_add(1, Ordering::Relaxed)
        );
        let mut pipe = context.socket::<Pair>();
        pipe.bind(&endpoint)?;
        let mut agent_pipe = context.socket::<Pair>();
        agent_pipe.connect(&endpoint)?;
        let router = context.socket::<Router>();
        thread::spawn(move || Agent::new(agent_pipe, router).run());
        Ok(Self { pipe })
    }

    /// Connects to the server at `endpoint`, whose ROUTER identity must be
    /// that endpoint. Waits 100 msec for the connection to come up, which
    /// isn't pretty, but saves us from sending all requests to a single
    /// server, at startup time.
    pub fn connect(&mut self, endpoint: &str) {
        let mut message = Message::new("CONNECT");
        message.push(endpoint.to_owned());
        self.pipe.send(message);
        thread::sleep(Duration::from_millis(100)); // Allow connection to come up
    }

    /// Sends `request` and returns the reply, or `None` if no server
    /// answered within the global timeout.
    pub fn request(&mut self, request: Message) -> Option<Message> {
        let mut message = Message::new("REQUEST");
        message.extend(request);
        self.pipe.send(message);
        let (status, reply) = self.pipe.recv().into_parts();
        if *status == *b"FAILED" {
            return None;
        }
        Message::try_from(reply).ok()
    }
}

/// One server we talk to.
struct Server {
    endpoint: String,
    alive: bool,
    ping_at: Instant,
    expires: Instant,
}

impl Server {
    fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            alive: false,
            ping_at: Instant::now() + PING_INTERVAL,
            expires: Instant::now() + SERVER_TTL,
        }
    }

    fn refresh(&mut self) {
        self.ping_at = Instant::now() + PING_INTERVAL;
        self.expires = Instant::now() + SERVER_TTL;
    }
}

/// The background agent: processes messages from the application's pipe
/// and from the servers.
struct Agent {
    pipe: Socket<Pair>,
    router: Socket<Router>,
    servers: HashMap<String, Server>,
    /// Servers we know are alive, by endpoint
    actives: VecDeque<String>,
    sequence: u32,
    /// Current request if any, and when it expires
    request: Option<(Message, Instant)>,
}

impl Agent {
    fn new(pipe: Socket<Pair>, router: Socket<Router>) -> Self {
        Self {
            pipe,
            router,
            servers: HashMap::new(),
            actives: VecDeque::new(),
            sequence: 0,
            request: None,
        }
    }

    fn run(mut self) -> Result<(), zmq::Error> {
        loop {
            // Calculate tickless timer, up to 1 hour
            let mut tickless = Instant::now() + Duration::from_secs(3600);
            if let Some((_, expires)) = &self.request {
                tickless = tickless.min(*expires);
            }
            for server in self.servers.values() {
                tickless = tickless.min(server.ping_at);
            }
            let timeout = tickless.saturating_duration_since(Instant::now());
            let [control, reply] =
                zmq::poll_timeout([self.pipe.readable(), self.router.readable()], timeout)
                    .unwrap_or_default();

            if control {
                self.control_message()?;
            }
            if reply {
                self.router_message();
            }

            // If we're processing a request, dispatch to next server
            if let Some((_, expires)) = self.request
                && Instant::now() >= expires
            {
                // Request expired, kill it
                self.pipe.send("FAILED");
                self.request = None;
            } else if let Some((request, _)) = &self.request {
                // Find server to talk to, remove any expired ones
                while let Some(endpoint) = self.actives.front() {
                    let server = self.servers.get_mut(endpoint).expect("known server");
                    if Instant::now() >= server.expires {
                        server.alive = false;
                        self.actives.pop_front();
                    } else {
                        let to = RoutingId::new(endpoint.clone()).expect("an endpoint");
                        let _ = self.router.try_send(&to, request.clone());
                        break;
                    }
                }
            }
            // Send heartbeats to idle servers if needed
            for server in self.servers.values_mut() {
                if Instant::now() >= server.ping_at {
                    let to = RoutingId::new(server.endpoint.clone()).expect("an endpoint");
                    let _ = self.router.try_send(&to, "PING");
                    server.ping_at = Instant::now() + PING_INTERVAL;
                }
            }
        }
    }

    /// Processes one message from the frontend: CONNECT or REQUEST.
    fn control_message(&mut self) -> Result<(), zmq::Error> {
        let (command, rest) = self.pipe.recv().into_parts();
        match command.as_slice() {
            b"CONNECT" => {
                let Some(endpoint) = rest.first().map(|frame| text(frame)) else {
                    return Ok(());
                };
                println!("I: connecting to {endpoint}...");
                self.router.connect(&endpoint)?;
                let mut server = Server::new(endpoint.clone());
                server.refresh();
                self.servers.insert(endpoint.clone(), server);
                self.actives.push_back(endpoint);
            }
            b"REQUEST" => {
                // Prefix request with sequence number
                self.sequence += 1;
                let mut request = Message::new(self.sequence.to_string());
                request.extend(rest);
                // Request expires after global timeout
                self.request = Some((request, Instant::now() + GLOBAL_TIMEOUT));
            }
            _ => {}
        }
        Ok(())
    }

    /// Processes one message from a connected server.
    fn router_message(&mut self) {
        let (identity, reply) = self.router.recv();

        // Frame 0 is server that replied
        let endpoint = text(identity.as_bytes());
        let Some(server) = self.servers.get_mut(&endpoint) else {
            return;
        };
        if !server.alive {
            self.actives.push_back(endpoint);
            server.alive = true;
        }
        server.refresh();

        // Frame 1 may be sequence number for reply
        let (sequence, rest) = reply.into_parts();
        if self.request.is_some() && *sequence == *self.sequence.to_string().as_bytes() {
            let mut reply = Message::new("OK");
            reply.extend(rest);
            self.pipe.send(reply);
            self.request = None;
        }
    }
}
