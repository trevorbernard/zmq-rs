//! Publisher fan-out: the cost of one `Publisher::send` as peers and topic
//! lengths grow.
//!
//! Peers are inproc SUB connections, so no I/O thread is involved. Their
//! queues are drained between batches, outside the timed sends, so no
//! message is dropped for a full queue.
//!
//! `cargo bench -p zmq-core --bench fanout [filter]`

use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use zmq_core::{Connection, ConnectionConfig, Context, PeerList, Publisher};
use zmtp::{Frame, Message, SocketType, Subscription};

/// Sends between drains, well under the inproc queues' 2000 messages.
const BATCH: u64 = 256;

struct Setup {
    publisher: Publisher,
    peers: PeerList,
    subscribers: Vec<Connection>,
    message: Message,
    // Keeps the inproc name bound.
    _context: Context,
}

/// `topics[i]` is peer `i`'s subscription; `message`'s first frame is
/// `data`.
fn setup(topics: &[Vec<u8>], data: Vec<u8>) -> Setup {
    let context = Context::new().expect("context");
    let mut listener = context
        .bind_inproc("fanout", ConnectionConfig::new(SocketType::Pub))
        .expect("bind");
    let subscribers: Vec<Connection> = topics
        .iter()
        .map(|topic| {
            let mut sub = context.connect_inproc("fanout", ConnectionConfig::new(SocketType::Sub));
            let subscription = Subscription::Subscribe(Frame::copy_from_slice(topic));
            sub.tx.try_send(subscription.to_message()).expect("room");
            sub
        })
        .collect();
    let mut peers = PeerList::default();
    while let Ok(connection) = listener.try_accept() {
        peers.push(connection);
    }
    assert_eq!(peers.len(), topics.len());
    let mut publisher = Publisher::default();
    publisher.sync(&mut peers);
    Setup {
        publisher,
        peers,
        subscribers,
        message: Message::new(Frame::from(data)),
        _context: context,
    }
}

fn bench_sends(setup: &mut Setup, iters: u64, expected: usize) -> Duration {
    let mut elapsed = Duration::ZERO;
    let mut left = iters;
    while left > 0 {
        let batch = left.min(BATCH);
        let start = Instant::now();
        for _ in 0..batch {
            let sent = setup
                .publisher
                .send(&mut setup.peers, black_box(setup.message.clone()));
            debug_assert_eq!(sent, expected);
        }
        elapsed += start.elapsed();
        left -= batch;
        for sub in &mut setup.subscribers {
            while sub.rx.try_recv().is_ok() {}
        }
    }
    elapsed
}

/// Each peer subscribes to its own short topic; the message matches one.
fn one_match_among_many(c: &mut Criterion) {
    let mut group = c.benchmark_group("one_match_among_many");
    for peers in [1, 10, 100, 1000] {
        let topics: Vec<Vec<u8>> = (0..peers)
            .map(|i| format!("topic-{i:04}").into_bytes())
            .collect();
        let mut setup = setup(&topics, b"topic-0000 payload".to_vec());
        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::from_parameter(peers), |b| {
            b.iter_custom(|iters| bench_sends(&mut setup, iters, 1));
        });
    }
    group.finish();
}

/// 100 peers on topics of `len` octets that differ only in their last
/// two; the message matches one.
fn long_topics(c: &mut Criterion) {
    let mut group = c.benchmark_group("long_topics");
    for len in [16, 256, 4096] {
        let topics: Vec<Vec<u8>> = (0..100u16)
            .map(|i| {
                let mut topic = vec![b'x'; len - 2];
                topic.extend(i.to_be_bytes());
                topic
            })
            .collect();
        let mut setup = setup(&topics, topics[0].clone());
        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::from_parameter(len), |b| {
            b.iter_custom(|iters| bench_sends(&mut setup, iters, 1));
        });
    }
    group.finish();
}

/// Every peer subscribes to everything, the common PUB case.
fn all_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("all_match");
    for peers in [1, 10, 100] {
        let topics = vec![Vec::new(); peers];
        let mut setup = setup(&topics, b"payload".to_vec());
        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::from_parameter(peers), |b| {
            b.iter_custom(|iters| bench_sends(&mut setup, iters, peers));
        });
    }
    group.finish();
}

criterion_group!(benches, one_match_among_many, long_topics, all_match);
criterion_main!(benches);
