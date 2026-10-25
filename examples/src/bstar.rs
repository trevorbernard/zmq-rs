//! Binary Star reactor, as the guide's `bstar.c`: a primary and a backup
//! server that watch each other's state and fail over between them.
//!
//! The C registers handlers with a zloop reactor. Here the application
//! owns the loop: it polls [`BinaryStar::readable`]'s two sockets beside
//! its own, waits at most [`BinaryStar::timeout`], and hands what was
//! ready to [`BinaryStar::handle`], which returns what the C would call
//! handlers for. [`BinaryStar::start`] runs that loop for a server with no
//! sockets of its own, as `bstar_start` does.
//!
//! ```no_run
//! # use zguide::bstar::{BinaryStar, Event, Role};
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let context = zmq::Context::new()?;
//! # let mut other = context.socket::<zmq::Pull>();
//! let mut bstar = BinaryStar::new(
//!     &context,
//!     Role::Primary,
//!     "tcp://*:5003",
//!     "tcp://localhost:5004",
//!     "tcp://*:5001",
//! )?;
//! loop {
//!     let timeout = bstar.timeout();
//!     let [vote, peer] = bstar.readable();
//!     let ready = zmq::poll_timeout([vote, peer, other.readable()], timeout);
//!     let [vote, peer, other_ready] = ready.unwrap_or_default();
//!     for event in bstar.handle([vote, peer])? {
//!         if let Event::Request(client, request) = event {
//!             let _ = bstar.voter().try_send(&client, request);
//!         }
//!     }
//!     if other_ready {
//!         other.recv();
//!     }
//! }
//! # }
//! ```

use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};

use zmq::{Context, Message, PollItem, Pub, Router, RoutingId, Socket, Sub};

use crate::text;

/// We send state information every this often. If peer doesn't respond in
/// two heartbeats, it is 'dead'.
pub const HEARTBEAT: Duration = Duration::from_millis(1000);

/// Which of the pair this server is, as `bstar_new`'s `primary` argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Primary,
    Backup,
}

/// What the C's handlers are called for.
#[derive(Debug)]
pub enum Event {
    /// A client request this server may answer, as `bstar_voter`'s
    /// handler gets it. Answer it on [`BinaryStar::voter`].
    Request(RoutingId, Message),
    /// This server became active (`bstar_new_active`'s handler).
    Active,
    /// This server became passive (`bstar_new_passive`'s handler).
    Passive,
}

/// The pair is in a state it cannot recover from, such as both servers
/// active; the C's reactor ends when a handler returns -1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fatal(&'static str);

impl fmt::Display for Fatal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl Error for Fatal {}

/// States we can be in at any point in time
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Primary = 1, // Primary, waiting for peer to connect
    Backup = 2,  // Backup, waiting for peer to connect
    Active = 3,  // Active - accepting connections
    Passive = 4, // Passive - not accepting connections
}

/// Events, which start with the states our peer can be in
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FsmEvent {
    PeerPrimary,   // HA peer is pending primary
    PeerBackup,    // HA peer is pending backup
    PeerActive,    // HA peer is active
    PeerPassive,   // HA peer is passive
    ClientRequest, // Client makes request
}

/// What applying an event did, beyond changing state.
enum Outcome {
    /// The event was accepted; the state may have changed.
    Accepted(Option<Event>),
    /// A client request that must not be answered now.
    Rejected,
}

/// A Binary Star server: publishes its state to its peer, follows the
/// peer's, and decides from both whether to answer clients.
#[derive(Debug)]
pub struct BinaryStar {
    statepub: Socket<Pub>,
    statesub: Socket<Sub>,
    voter: Socket<Router>,
    state: State,
    peer_expiry: Instant,
    send_state_at: Instant,
}

impl BinaryStar {
    /// A Binary Star server that binds `local` to publish its state,
    /// connects `remote` to hear its peer's, as `bstar_new`, and binds the
    /// ROUTER that clients send requests to at `voter`, as `bstar_voter`.
    /// Requests on it are the CLIENT_REQUEST events for the state machine,
    /// and come out of [`handle`](Self::handle) as [`Event::Request`] when
    /// this server may answer them.
    pub fn new(
        context: &Context,
        role: Role,
        local: &str,
        remote: &str,
        voter: &str,
    ) -> Result<Self, zmq::Error> {
        // Create publisher for state going to peer
        let mut statepub = context.socket::<Pub>();
        statepub.bind(local)?;

        // Create subscriber for state coming from peer
        let mut statesub = context.socket::<Sub>();
        statesub.subscribe("");
        statesub.connect(remote)?;

        let mut voter_socket = context.socket::<Router>();
        voter_socket.bind(voter)?;

        let now = Instant::now();
        Ok(Self {
            statepub,
            statesub,
            voter: voter_socket,
            state: match role {
                Role::Primary => State::Primary,
                Role::Backup => State::Backup,
            },
            peer_expiry: now + 2 * HEARTBEAT,
            send_state_at: now + HEARTBEAT,
        })
    }

    /// The voter socket, to answer requests on.
    pub fn voter(&mut self) -> &mut Socket<Router> {
        &mut self.voter
    }

    /// Whether this server is active, answering clients.
    pub fn is_active(&self) -> bool {
        self.state == State::Active
    }

    /// The voter, then the peer's state, to poll beside the application's
    /// own sockets.
    pub fn readable(&mut self) -> [PollItem<'_>; 2] {
        [self.voter.readable(), self.statesub.readable()]
    }

    /// How long to wait before the next state message is due.
    pub fn timeout(&self) -> Duration {
        self.send_state_at.saturating_duration_since(Instant::now())
    }

    /// Handles what a poll found ready on [`readable`](Self::readable)'s
    /// items, and publishes our state if it is time. Returns what
    /// happened, in order.
    pub fn handle(&mut self, [vote, peer]: [bool; 2]) -> Result<Vec<Event>, Fatal> {
        let mut events = Vec::new();
        if peer {
            // Receive state from peer, execute finite state machine
            let state = text(self.statesub.recv().last());
            let event = match state.as_str() {
                "1" => Some(FsmEvent::PeerPrimary),
                "2" => Some(FsmEvent::PeerBackup),
                "3" => Some(FsmEvent::PeerActive),
                "4" => Some(FsmEvent::PeerPassive),
                _ => None,
            };
            if let Some(event) = event {
                self.peer_expiry = Instant::now() + 2 * HEARTBEAT;
                match self.execute(event)? {
                    Outcome::Accepted(change) => events.extend(change),
                    Outcome::Rejected => {}
                }
            }
        }
        if vote {
            // Application wants to speak to us, see if it's possible
            let (client, request) = self.voter.recv();
            match self.execute(FsmEvent::ClientRequest)? {
                Outcome::Accepted(change) => {
                    events.extend(change);
                    events.push(Event::Request(client, request));
                }
                // Destroy waiting message, no-one to read it
                Outcome::Rejected => {}
            }
        }
        // Publish our state to peer
        if Instant::now() >= self.send_state_at {
            self.statepub.send((self.state as u8).to_string());
            self.send_state_at = Instant::now() + HEARTBEAT;
        }
        Ok(events)
    }

    /// Runs the reactor for a server whose only input is its voter,
    /// calling `on_request` for each request it may answer, as
    /// `bstar_start`. Ends only on a fatal state.
    pub fn start(
        mut self,
        mut on_request: impl FnMut(&mut Socket<Router>, RoutingId, Message),
    ) -> Result<(), Fatal> {
        loop {
            let timeout = self.timeout();
            let ready = zmq::poll_timeout(self.readable(), timeout).unwrap_or_default();
            for event in self.handle(ready)? {
                if let Event::Request(client, request) = event {
                    on_request(&mut self.voter, client, request);
                }
            }
        }
    }

    /// Binary Star finite state machine (applies event to state)
    fn execute(&mut self, event: FsmEvent) -> Result<Outcome, Fatal> {
        let peer_dead = Instant::now() >= self.peer_expiry;
        let change = match (self.state, event) {
            // Primary server is waiting for peer to connect. Accepts
            // CLIENT_REQUEST events in this state
            (State::Primary, FsmEvent::PeerBackup) => {
                println!("I: connected to backup (passive), ready as active");
                Some(State::Active)
            }
            (State::Primary, FsmEvent::PeerActive) => {
                println!("I: connected to backup (active), ready as passive");
                Some(State::Passive)
            }
            // Allow client requests to turn us into the active if we've
            // waited sufficiently long to believe the backup is not
            // currently acting as active (i.e., after a failover)
            (State::Primary, FsmEvent::ClientRequest) => {
                if !peer_dead {
                    // Don't respond to clients yet - it's possible we're
                    // performing a failback and the backup is currently
                    // active
                    return Ok(Outcome::Rejected);
                }
                println!("I: request from client, ready as active");
                Some(State::Active)
            }
            // Backup server is waiting for peer to connect. Rejects
            // CLIENT_REQUEST events in this state
            (State::Backup, FsmEvent::PeerActive) => {
                println!("I: connected to primary (active), ready as passive");
                Some(State::Passive)
            }
            (State::Backup, FsmEvent::ClientRequest) => return Ok(Outcome::Rejected),
            // Server is active. Accepts CLIENT_REQUEST events in this
            // state. The only way out of ACTIVE is death
            (State::Active, FsmEvent::PeerActive) => {
                // Two actives would mean split-brain
                println!("E: fatal error - dual actives, aborting");
                return Err(Fatal("dual actives"));
            }
            // Server is passive. CLIENT_REQUEST events can trigger
            // failover if peer looks dead
            (State::Passive, FsmEvent::PeerPrimary) => {
                // Peer is restarting - become active, peer will go passive
                println!("I: primary (passive) is restarting, ready as active");
                Some(State::Active)
            }
            (State::Passive, FsmEvent::PeerBackup) => {
                // Peer is restarting - become active, peer will go passive
                println!("I: backup (passive) is restarting, ready as active");
                Some(State::Active)
            }
            (State::Passive, FsmEvent::PeerPassive) => {
                // Two passives would mean cluster would be non-responsive
                println!("E: fatal error - dual passives, aborting");
                return Err(Fatal("dual passives"));
            }
            (State::Passive, FsmEvent::ClientRequest) => {
                // Peer becomes active if timeout has passed. It's the
                // client request that triggers the failover
                if !peer_dead {
                    // If peer is alive, reject connections
                    return Ok(Outcome::Rejected);
                }
                // If peer is dead, switch to the active state
                println!("I: failover successful, ready as active");
                Some(State::Active)
            }
            _ => None,
        };
        let Some(state) = change else {
            return Ok(Outcome::Accepted(None));
        };
        self.state = state;
        Ok(Outcome::Accepted(Some(match state {
            State::Passive => Event::Passive,
            _ => Event::Active,
        })))
    }
}
