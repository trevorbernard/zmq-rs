//! Fair queueing: the cost of one `FairQueue::poll_recv` as peers grow,
//! when only some of them send.
//!
//! Peers are inproc connections, so no I/O thread is involved. Senders
//! queue each batch outside the timed receives.
//!
//! `cargo bench -p zmq-core --bench fairqueue [filter]`

use std::hint::black_box;
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use zmq_core::{Connection, ConnectionConfig, Context, FairQueue, PeerList};
use zmtp::{Frame, Message, SocketType};

/// Messages received per batch, spread over the sending peers; well under
/// the inproc queues' 2000 messages each.
const BATCH: u64 = 1000;

struct Setup {
    queue: FairQueue,
    peers: PeerList,
    senders: Vec<Connection>,
    // Kept open: a dropped connection's peer is removed, not idle.
    _idle: Vec<Connection>,
    message: Message,
    // Keeps the inproc name bound.
    _context: Context,
}

fn setup(peers: usize, sending: usize) -> Setup {
    let context = Context::new().expect("context");
    let mut listener = context
        .bind_inproc("fairqueue", ConnectionConfig::new(SocketType::Pull))
        .expect("bind");
    let all: Vec<Connection> = (0..peers)
        .map(|_| context.connect_inproc("fairqueue", ConnectionConfig::new(SocketType::Push)))
        .collect();
    let mut list = PeerList::default();
    while let Ok(connection) = listener.try_accept() {
        list.push(connection);
    }
    assert_eq!(list.len(), peers);
    // Senders spread over the list, as busy peers among idle ones would be.
    let stride = peers / sending;
    let (senders, idle) = all
        .into_iter()
        .enumerate()
        .partition(|&(i, _)| i % stride == 0 && i / stride < sending);
    let unzip =
        |connections: Vec<(usize, Connection)>| connections.into_iter().map(|(_, c)| c).collect();
    Setup {
        queue: FairQueue::default(),
        peers: list,
        senders: unzip(senders),
        _idle: unzip(idle),
        message: Message::new(Frame::from(vec![0; 64])),
        _context: context,
    }
}

fn bench_receives(setup: &mut Setup, iters: u64) -> Duration {
    let mut cx = TaskContext::from_waker(Waker::noop());
    let mut elapsed = Duration::ZERO;
    let mut left = iters;
    while left > 0 {
        let batch = left.min(BATCH);
        let senders = setup.senders.len() as u64;
        for (i, sender) in setup.senders.iter_mut().enumerate() {
            let share = batch / senders + u64::from((i as u64) < batch % senders);
            for _ in 0..share {
                sender.tx.try_send(setup.message.clone()).expect("room");
            }
        }
        let start = Instant::now();
        for _ in 0..batch {
            let received = setup.queue.poll_recv(&mut setup.peers, &mut cx);
            debug_assert!(received.is_ready());
            black_box(&received);
        }
        elapsed += start.elapsed();
        assert_eq!(
            setup.queue.poll_recv(&mut setup.peers, &mut cx),
            Poll::Pending
        );
        left -= batch;
    }
    elapsed
}

fn one_sender_among_many(c: &mut Criterion) {
    let mut group = c.benchmark_group("one_sender_among_many");
    group.throughput(Throughput::Elements(1));
    for peers in [1, 10, 100, 1000] {
        let mut setup = setup(peers, 1);
        group.bench_with_input(BenchmarkId::from_parameter(peers), &peers, |b, _| {
            b.iter_custom(|iters| bench_receives(&mut setup, iters));
        });
    }
    group.finish();
}

fn all_send(c: &mut Criterion) {
    let mut group = c.benchmark_group("all_send");
    group.throughput(Throughput::Elements(1));
    for peers in [1, 10, 100] {
        let mut setup = setup(peers, peers);
        group.bench_with_input(BenchmarkId::from_parameter(peers), &peers, |b, _| {
            b.iter_custom(|iters| bench_receives(&mut setup, iters));
        });
    }
    group.finish();
}

criterion_group!(benches, one_sender_among_many, all_send);
criterion_main!(benches);
