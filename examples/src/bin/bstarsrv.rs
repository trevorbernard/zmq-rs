//! Binary Star server proof-of-concept implementation. This server does no
//! real work; it just demonstrates the Binary Star failover model.
//!
//! Run as `bstarsrv -p` for the primary, at tcp://localhost:5001, or
//! `bstarsrv -b` for the backup, at tcp://localhost:5002.

use std::env;
use std::time::{Duration, Instant};

use zguide::body;
use zmq::{Context, Pub, Router, Sub};

/// We send state information this often. If peer doesn't respond in two
/// heartbeats, it is 'dead'.
const HEARTBEAT: Duration = Duration::from_millis(1000);

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
enum Event {
    PeerPrimary,   // HA peer is pending primary
    PeerBackup,    // HA peer is pending backup
    PeerActive,    // HA peer is active
    PeerPassive,   // HA peer is passive
    ClientRequest, // Client makes request
}

impl Event {
    /// The event for a state our peer published.
    fn from_peer(state: &str) -> Option<Self> {
        match state {
            "1" => Some(Self::PeerPrimary),
            "2" => Some(Self::PeerBackup),
            "3" => Some(Self::PeerActive),
            "4" => Some(Self::PeerPassive),
            _ => None,
        }
    }
}

/// Our finite state machine
struct Fsm {
    state: State,
    peer_expiry: Instant, // When peer is considered 'dead'
}

impl Fsm {
    /// The heart of the Binary Star design is its finite-state machine
    /// (FSM). The FSM runs one event at a time. We apply an event to the
    /// current state, which checks if the event is accepted, and if so,
    /// sets a new state. Returns false if the event was an exception.
    fn apply(&mut self, event: Event) -> bool {
        match (self.state, event) {
            // These are the PRIMARY and BACKUP states; we're waiting to
            // become ACTIVE or PASSIVE depending on events we get from our
            // peer. Primary accepts client connections.
            (State::Primary, Event::PeerBackup) => {
                println!("I: connected to backup (passive), ready active");
                self.state = State::Active;
            }
            (State::Primary, Event::PeerActive) => {
                println!("I: connected to backup (active), ready passive");
                self.state = State::Passive;
            }
            (State::Backup, Event::PeerActive) => {
                println!("I: connected to primary (active), ready passive");
                self.state = State::Passive;
            }
            // Reject client connections when acting as backup
            (State::Backup, Event::ClientRequest) => return false,
            // Two actives would mean split-brain
            (State::Active, Event::PeerActive) => {
                println!("E: fatal error - dual actives, aborting");
                return false;
            }
            // Peer is restarting - become active, peer will go passive
            (State::Passive, Event::PeerPrimary) => {
                println!("I: primary (passive) is restarting, ready active");
                self.state = State::Active;
            }
            (State::Passive, Event::PeerBackup) => {
                println!("I: backup (passive) is restarting, ready active");
                self.state = State::Active;
            }
            // Two passives would mean cluster would be non-responsive
            (State::Passive, Event::PeerPassive) => {
                println!("E: fatal error - dual passives, aborting");
                return false;
            }
            // Peer becomes active if timeout has passed. It's the client
            // request that triggers the failover
            (State::Passive, Event::ClientRequest) => {
                if Instant::now() < self.peer_expiry {
                    // If peer is alive, reject connections
                    return false;
                }
                // If peer is dead, switch to the active state
                println!("I: failover successful, ready active");
                self.state = State::Active;
            }
            _ => {}
        }
        true
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let mut statepub = context.socket::<Pub>();
    let mut statesub = context.socket::<Sub>();
    statesub.subscribe("");
    let mut frontend = context.socket::<Router>();

    let state = match env::args().nth(1).as_deref() {
        Some("-p") => {
            println!("I: Primary active, waiting for backup (passive)");
            frontend.bind("tcp://*:5001")?;
            statepub.bind("tcp://*:5003")?;
            statesub.connect("tcp://localhost:5004")?;
            State::Primary
        }
        Some("-b") => {
            println!("I: Backup passive, waiting for primary (active)");
            frontend.bind("tcp://*:5002")?;
            statepub.bind("tcp://*:5004")?;
            statesub.connect("tcp://localhost:5003")?;
            State::Backup
        }
        _ => {
            println!("Usage: bstarsrv {{ -p | -b }}");
            return Ok(());
        }
    };
    let mut fsm = Fsm {
        state,
        peer_expiry: Instant::now(),
    };

    // We now process events on our two input sockets, and process these
    // events one at a time via our finite-state machine. Our "work" for a
    // client request is simply to echo it back

    // Set timer for next outgoing state message
    let mut send_state_at = Instant::now() + HEARTBEAT;
    loop {
        let time_left = send_state_at.saturating_duration_since(Instant::now());
        let [request, peer_state] =
            zmq::poll_timeout([frontend.readable(), statesub.readable()], time_left)
                .unwrap_or_default();

        if request {
            // Have a client request; answer it by echoing it back
            let (client, message) = frontend.recv();
            if fsm.apply(Event::ClientRequest) {
                let _ = frontend.try_send(&client, message);
            }
        }
        if peer_state {
            // Have state from our peer, execute as event
            if let Some(event) = Event::from_peer(&body(&statesub.recv())) {
                if !fsm.apply(event) {
                    break; // Error, so exit
                }
                fsm.peer_expiry = Instant::now() + 2 * HEARTBEAT;
            }
        }
        // If we timed out, send state to peer
        if Instant::now() >= send_state_at {
            statepub.send((fsm.state as u8).to_string());
            send_state_at = Instant::now() + HEARTBEAT;
        }
    }
    Ok(())
}
