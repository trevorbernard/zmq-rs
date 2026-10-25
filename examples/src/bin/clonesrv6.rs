//! Clone server model six: a primary and backup pair of clone servers,
//! using the Binary Star pattern to agree which one is active. Run one
//! with `-p` and one with `-b`.
//!
//! Ports 5003/5004 interconnect the servers. Ports 5556/5566 receive
//! snapshot requests, the Binary Star voting events; ports 5557/5567 are
//! the publishers and ports 5558/5568 the collectors.
//!
//! The guide runs this on bstar's zloop reactor; here one loop polls
//! bstar's sockets beside the server's own and wakes for its timers.

use std::collections::HashMap;
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use zguide::bstar::{BinaryStar, Event, Role};
use zguide::kvmsg::KvMsg;
use zguide::mdp::log;
use zguide::text;
use zmq::{Context, Dealer, Message, Pub, Router, RoutingId, Socket, Sub};

/// How often ephemeral values are checked for expiry, and HUGZ sent.
const TICK: Duration = Duration::from_secs(1);

// Our server is defined by these properties
struct CloneServer {
    context: Context,
    // Key-value store; none while passive until a snapshot arrives
    kvmap: Option<HashMap<String, KvMsg>>,
    // How many updates we're at
    sequence: i64,
    // Main port of our peer
    peer: u16,
    // Publish updates and hugz
    publisher: Socket<Pub>,
    // Collect updates from clients
    collector: Socket<Sub>,
    // Get updates from peer
    subscriber: Socket<Sub>,
    // Pending updates from clients
    pending: Vec<KvMsg>,
    // true if we're active
    active: bool,
    // true if we're passive
    passive: bool,
}

// The main task parses the command line to decide whether to start as a
// primary or backup server, and runs both servers' loop
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let (role, port, peer) = match env::args().nth(1).as_deref() {
        Some("-p") => {
            log("I: primary active, waiting for backup (passive)");
            (Role::Primary, 5556, 5566)
        }
        Some("-b") => {
            log("I: backup passive, waiting for primary (active)");
            (Role::Backup, 5566, 5556)
        }
        _ => {
            println!("Usage: clonesrv6 {{ -p | -b }}");
            return Ok(());
        }
    };
    let (local, remote) = match role {
        Role::Primary => ("tcp://*:5003", "tcp://localhost:5004"),
        Role::Backup => ("tcp://*:5004", "tcp://localhost:5003"),
    };
    let mut bstar = BinaryStar::new(&context, role, local, remote, &format!("tcp://*:{port}"))?;

    // Set up our clone server sockets
    let mut publisher = context.socket::<Pub>();
    publisher.bind(&format!("tcp://*:{}", port + 1))?;
    let mut collector = context.socket::<Sub>();
    collector.subscribe("");
    collector.bind(&format!("tcp://*:{}", port + 2))?;

    // Set up our own clone client interface to peer
    let mut subscriber = context.socket::<Sub>();
    subscriber.subscribe("");
    subscriber.connect(&format!("tcp://localhost:{}", peer + 1))?;

    let mut server = CloneServer {
        context,
        // Primary server will become first active
        kvmap: (role == Role::Primary).then(HashMap::new),
        sequence: 0,
        peer,
        publisher,
        collector,
        subscriber,
        pending: Vec::new(),
        active: false,
        passive: false,
    };

    let mut tick_at = Instant::now() + TICK;
    loop {
        let timeout = bstar
            .timeout()
            .min(tick_at.saturating_duration_since(Instant::now()));
        let [vote, peer_state] = bstar.readable();
        let [vote, peer_state, update, from_peer] = zmq::poll_timeout(
            [
                vote,
                peer_state,
                server.collector.readable(),
                server.subscriber.readable().when(server.passive),
            ],
            timeout,
        )
        .unwrap_or_default();

        for event in bstar.handle([vote, peer_state])? {
            match event {
                Event::Request(client, request) => {
                    server.send_snapshot(bstar.voter(), &client, &request)?;
                }
                Event::Active => server.new_active(),
                Event::Passive => server.new_passive(),
            }
        }
        if update {
            server.collect();
        }
        if from_peer {
            server.follow_peer()?;
        }
        if Instant::now() >= tick_at {
            server.flush_ttl();
            server.send_hugz();
            tick_at += TICK;
        }
    }
}

impl CloneServer {
    fn kvmap(&mut self) -> &mut HashMap<String, KvMsg> {
        self.kvmap.get_or_insert_with(HashMap::new)
    }

    // We handle ICANHAZ? requests exactly as in the clonesrv5 example
    fn send_snapshot(
        &mut self,
        snapshot: &mut Socket<Router>,
        identity: &RoutingId,
        request: &Message,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Request is in second frame of message
        let (command, rest) = request.split_first();
        if **command != *b"ICANHAZ?" {
            println!("E: bad request, aborting");
            return Ok(());
        }
        let subtree = rest.first().map(|frame| text(frame)).unwrap_or_default();

        // Send state snapshot to client
        for (key, kvmsg) in self.kvmap.iter().flatten() {
            if key.starts_with(&subtree) {
                snapshot.send(identity, kvmsg.to_message())?;
            }
        }

        // Now send END message with sequence number
        log(&format!("I: sending shapshot={}", self.sequence));
        let mut kvmsg = KvMsg::new(self.sequence);
        kvmsg.set_key("KTHXBAI");
        kvmsg.set_body("");
        snapshot.send(identity, kvmsg.to_message())?;
        Ok(())
    }

    // If the message was already on the pending list, removes it and
    // returns true
    fn was_pending(&mut self, kvmsg: &KvMsg) -> bool {
        let Some(at) = self
            .pending
            .iter()
            .position(|held| held.uuid() == kvmsg.uuid())
        else {
            return false;
        };
        self.pending.remove(at);
        true
    }

    // The collector is more complex than in the clonesrv5 example because
    // the way it processes updates depends on whether we're active or
    // passive. The active applies them immediately to its kvmap, whereas
    // the passive queues them as pending
    fn collect(&mut self) {
        let Some(mut kvmsg) = KvMsg::from_message(&self.collector.recv()) else {
            return;
        };
        if self.active {
            self.sequence += 1;
            kvmsg.set_sequence(self.sequence);
            self.publisher.send(kvmsg.to_message());
            let ttl: i64 = kvmsg.prop("ttl").parse().unwrap_or(0);
            if ttl != 0 {
                kvmsg.set_prop("ttl", (now_ms() + ttl * 1000).to_string());
            }
            kvmsg.store(self.kvmap());
            log(&format!("I: publishing update={}", self.sequence));
        } else if !self.was_pending(&kvmsg) {
            // If we already got message from active, drop it, else hold on
            // pending list
            self.pending.push(kvmsg);
        }
    }

    // We purge ephemeral values using exactly the same code as in the
    // previous clonesrv5 example
    fn flush_ttl(&mut self) {
        let Some(kvmap) = &mut self.kvmap else {
            return;
        };
        let now = now_ms();
        let expired: Vec<String> = kvmap
            .iter()
            .filter(|(_, kvmsg)| {
                let ttl: i64 = kvmsg.prop("ttl").parse().unwrap_or(0);
                ttl != 0 && now >= ttl
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            let Some(mut kvmsg) = kvmap.remove(&key) else {
                continue;
            };
            self.sequence += 1;
            kvmsg.set_sequence(self.sequence);
            kvmsg.set_body("");
            self.publisher.send(kvmsg.to_message());
            log(&format!("I: publishing delete={}", self.sequence));
        }
    }

    // We send a HUGZ message once a second to all subscribers so that they
    // can detect if our server dies. They'll then switch over to the
    // backup server, which will become active
    fn send_hugz(&mut self) {
        let mut kvmsg = KvMsg::new(self.sequence);
        kvmsg.set_key("HUGZ");
        kvmsg.set_body("");
        self.publisher.send(kvmsg.to_message());
    }

    // When we switch from passive to active, we apply our pending list so
    // that our kvmap is up-to-date, and stop following the peer's updates
    fn new_active(&mut self) {
        self.active = true;
        self.passive = false;
        for mut kvmsg in std::mem::take(&mut self.pending) {
            self.sequence += 1;
            kvmsg.set_sequence(self.sequence);
            self.publisher.send(kvmsg.to_message());
            kvmsg.store(self.kvmap());
            log(&format!("I: publishing pending={}", self.sequence));
        }
    }

    // When we switch to passive, we wipe our kvmap and follow the active
    // server's updates, grabbing a new snapshot first
    fn new_passive(&mut self) {
        self.kvmap = None;
        self.active = false;
        self.passive = true;
    }

    // When we get an update, we take a snapshot first if we have no kvmap,
    // and then add our update to our kvmap. We're always passive in this
    // case
    fn follow_peer(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.kvmap.is_none() {
            let mut kvmap = HashMap::new();
            let mut snapshot = self.context.socket::<Dealer>();
            let endpoint = format!("tcp://localhost:{}", self.peer);
            snapshot.connect(&endpoint)?;
            log(&format!("I: asking for snapshot from: {endpoint}"));
            let mut request = Message::new("ICANHAZ?");
            // Blank subtree to get all
            request.push("");
            snapshot.send(request);
            loop {
                let Some(kvmsg) = KvMsg::from_message(&snapshot.recv()) else {
                    continue;
                };
                if kvmsg.key() == Some("KTHXBAI") {
                    self.sequence = kvmsg.sequence();
                    break;
                }
                kvmsg.store(&mut kvmap);
            }
            log(&format!("I: received snapshot={}", self.sequence));
            self.kvmap = Some(kvmap);
        }

        // Find and remove update off pending list
        let Some(kvmsg) = KvMsg::from_message(&self.subscriber.recv()) else {
            return Ok(());
        };
        if kvmsg.key() == Some("HUGZ") {
            return Ok(());
        }
        if !self.was_pending(&kvmsg) {
            // If active update came before client update, flip it around,
            // store active update (with sequence) on pending list and use
            // to clear client update when it comes later
            self.pending.push(kvmsg.clone());
        }
        // If update is more recent than our kvmap, apply it
        if kvmsg.sequence() > self.sequence {
            self.sequence = kvmsg.sequence();
            kvmsg.store(self.kvmap());
            log(&format!("I: received update={}", self.sequence));
        }
        Ok(())
    }
}

/// Milliseconds since the epoch, as CZMQ's `zclock_time`.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}
