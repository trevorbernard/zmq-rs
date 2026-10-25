//! Clone server model five: as model four, with kvmsg's properties. An
//! update with a `ttl` property, in seconds, expires: the server deletes
//! it then and publishes the delete as an update with an empty body.
//!
//! The guide runs this on CZMQ's zloop reactor; here one loop polls both
//! sockets and wakes for the flush timer.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use zguide::kvmsg::KvMsg;
use zguide::mdp::log;
use zguide::text;
use zmq::{Context, Pub, Pull, Router, Socket};

/// How often ephemeral values are checked for expiry.
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

// Our server is defined by these properties
struct CloneServer {
    kvmap: HashMap<String, KvMsg>,
    // How many updates we're at
    sequence: i64,
    // Handle snapshot requests
    snapshot: Socket<Router>,
    // Publish updates to clients
    publisher: Socket<Pub>,
    // Collect updates from clients
    collector: Socket<Pull>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context::new()?;
    let port = 5556;

    // Set up our clone server sockets
    let mut snapshot = context.socket::<Router>();
    snapshot.bind(&format!("tcp://*:{port}"))?;
    let mut publisher = context.socket::<Pub>();
    publisher.bind(&format!("tcp://*:{}", port + 1))?;
    let mut collector = context.socket::<Pull>();
    collector.bind(&format!("tcp://*:{}", port + 2))?;

    let mut server = CloneServer {
        kvmap: HashMap::new(),
        sequence: 0,
        snapshot,
        publisher,
        collector,
    };
    server.run()
}

impl CloneServer {
    // Run until killed, handling requests and updates as they arrive and
    // flushing expired values every second
    fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut flush_at = Instant::now() + FLUSH_INTERVAL;
        loop {
            let timeout = flush_at.saturating_duration_since(Instant::now());
            if let Some([request, update]) = zmq::poll_timeout(
                [self.snapshot.readable(), self.collector.readable()],
                timeout,
            ) {
                if request {
                    self.send_snapshot()?;
                }
                if update {
                    self.collect();
                }
            }
            if Instant::now() >= flush_at {
                self.flush_ttl();
                flush_at += FLUSH_INTERVAL;
            }
        }
    }

    // We handle ICANHAZ? requests by sending snapshot data to the client
    // that requested it, ending with a KTHXBAI message
    fn send_snapshot(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let (identity, request) = self.snapshot.recv();
        // Request is in second frame of message
        let (command, rest) = request.split_first();
        if **command != *b"ICANHAZ?" {
            println!("E: bad request, aborting");
            return Ok(());
        }
        let subtree = rest.first().map(|frame| text(frame)).unwrap_or_default();

        // Send state snapshot to client
        for (key, kvmsg) in &self.kvmap {
            if key.starts_with(&subtree) {
                self.snapshot.send(&identity, kvmsg.to_message())?;
            }
        }

        // Now send END message with sequence number
        log(&format!("I: sending shapshot={}", self.sequence));
        let mut kvmsg = KvMsg::new(self.sequence);
        kvmsg.set_key("KTHXBAI");
        kvmsg.set_body("");
        self.snapshot.send(&identity, kvmsg.to_message())?;
        Ok(())
    }

    // We store each update with a new sequence number, and if necessary, a
    // time-to-live. We publish updates immediately on our publisher socket
    fn collect(&mut self) {
        let Some(mut kvmsg) = KvMsg::from_message(&self.collector.recv()) else {
            return;
        };
        self.sequence += 1;
        kvmsg.set_sequence(self.sequence);
        self.publisher.send(kvmsg.to_message());
        let ttl: i64 = kvmsg.prop("ttl").parse().unwrap_or(0);
        if ttl != 0 {
            kvmsg.set_prop("ttl", (now_ms() + ttl * 1000).to_string());
        }
        kvmsg.store(&mut self.kvmap);
        log(&format!("I: publishing update={}", self.sequence));
    }

    // At regular intervals, we flush ephemeral values that have expired,
    // deleting each and publishing the fact to listening clients. This
    // could be slow on very large data sets
    fn flush_ttl(&mut self) {
        let now = now_ms();
        let expired: Vec<String> = self
            .kvmap
            .iter()
            .filter(|(_, kvmsg)| {
                let ttl: i64 = kvmsg.prop("ttl").parse().unwrap_or(0);
                ttl != 0 && now >= ttl
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            let Some(mut kvmsg) = self.kvmap.remove(&key) else {
                continue;
            };
            self.sequence += 1;
            kvmsg.set_sequence(self.sequence);
            kvmsg.set_body("");
            self.publisher.send(kvmsg.to_message());
            log(&format!("I: publishing delete={}", self.sequence));
        }
    }
}

/// Milliseconds since the epoch, as CZMQ's `zclock_time`.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}
