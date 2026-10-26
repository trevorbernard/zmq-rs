//! Properties of a pipe driven from one thread through generated sequences
//! of sends, receives, polls and drops, checked against a `VecDeque`: every
//! result is the model's, and a side left waiting is woken once what it
//! waits for happens.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use proptest::prelude::*;
use zmq_pipe::{Hwm, Reader, ReaderGone, TryRecvError, TrySendError, Writer, pipe, pipe_conflated};

#[derive(Default)]
struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Copy)]
enum Kind {
    Queue(Option<usize>),
    Conflated,
}

fn kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        4 => prop::option::weighted(0.8, 1usize..8).prop_map(Kind::Queue),
        1 => Just(Kind::Conflated),
    ]
}

#[derive(Debug, Clone)]
enum Op {
    Send,
    Recv,
    PollRecv,
    PollReady,
    ReaderPollClosed,
    WriterPollClosed,
    DropWriter,
    DropReader,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => Just(Op::Send),
        5 => Just(Op::Recv),
        2 => Just(Op::PollRecv),
        2 => Just(Op::PollReady),
        1 => Just(Op::ReaderPollClosed),
        1 => Just(Op::WriterPollClosed),
        1 => Just(Op::DropWriter),
        1 => Just(Op::DropReader),
    ]
}

/// What a side left waiting is owed: its latest waker, and when it must
/// have been woken.
struct Waiting {
    count: Arc<Count>,
    until: Until,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Until {
    /// An item arrives or the writer goes.
    Item,
    /// The writer goes.
    WriterGone,
    /// The reader has read this many items in all, a multiple of the
    /// low-water mark, or the reader goes.
    Read(u64),
    /// The reader goes.
    ReaderGone,
}

impl Waiting {
    fn new(until: Until) -> (Self, Waker) {
        let count = Arc::new(Count::default());
        let waker = Waker::from(count.clone());
        (Self { count, until }, waker)
    }

    fn woken(&self) -> bool {
        self.count.0.load(Ordering::SeqCst) > 0
    }
}

struct Model {
    kind: Kind,
    queue: VecDeque<u32>,
    writer: bool,
    reader: bool,
    read: u64,
    sent: u32,
}

impl Model {
    fn full(&self) -> bool {
        matches!(self.kind, Kind::Queue(Some(hwm)) if self.queue.len() >= hwm)
    }

    /// libzmq's `pipe_t::compute_lwm`.
    fn lwm(&self) -> Option<u64> {
        match self.kind {
            Kind::Queue(Some(hwm)) => Some((hwm as u64).div_ceil(2)),
            _ => None,
        }
    }

    fn recv(&mut self) -> Result<u32, TryRecvError> {
        match self.queue.pop_front() {
            Some(item) => {
                self.read += 1;
                Ok(item)
            }
            None if !self.writer => Err(TryRecvError::Closed),
            None => Err(TryRecvError::Empty),
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    #[test]
    fn a_pipe_behaves_as_a_bounded_queue(
        kind in kind(),
        ops in prop::collection::vec(op(), 0..80),
    ) {
        let (writer, reader) = match kind {
            Kind::Queue(hwm) => pipe::<u32>(match hwm {
                None => Hwm::Unlimited,
                Some(n) => Hwm::Messages(NonZeroUsize::new(n).expect("positive")),
            }),
            Kind::Conflated => pipe_conflated(),
        };
        let (mut writer, mut reader): (Option<Writer<u32>>, Option<Reader<u32>>) =
            (Some(writer), Some(reader));
        let mut model = Model { kind, queue: VecDeque::new(), writer: true, reader: true, read: 0, sent: 0 };
        let mut reader_waiting: Option<Waiting> = None;
        let mut writer_waiting: Option<Waiting> = None;

        for op in ops {
            match op {
                Op::Send => {
                    let Some(writer) = &mut writer else { continue };
                    let item = model.sent;
                    model.sent += 1;
                    let expected = if !model.reader {
                        Err(TrySendError::Closed(item))
                    } else if model.full() {
                        Err(TrySendError::Full(item))
                    } else {
                        if let Kind::Conflated = kind {
                            model.queue.clear();
                        }
                        model.queue.push_back(item);
                        Ok(())
                    };
                    prop_assert_eq!(writer.try_send(item), expected);
                }
                Op::Recv => {
                    let Some(reader) = &mut reader else { continue };
                    prop_assert_eq!(reader.try_recv(), model.recv());
                }
                Op::PollRecv => {
                    let Some(reader) = &mut reader else { continue };
                    let (waiting, waker) = Waiting::new(Until::Item);
                    let polled = reader.poll_recv(&mut Context::from_waker(&waker));
                    let expected = match model.recv() {
                        Ok(item) => Poll::Ready(Some(item)),
                        Err(TryRecvError::Closed) => Poll::Ready(None),
                        Err(TryRecvError::Empty) => Poll::Pending,
                    };
                    if expected.is_pending() {
                        reader_waiting = Some(waiting);
                    }
                    prop_assert_eq!(polled, expected);
                }
                Op::ReaderPollClosed => {
                    let Some(reader) = &mut reader else { continue };
                    let (waiting, waker) = Waiting::new(Until::WriterGone);
                    let polled = reader.poll_closed(&mut Context::from_waker(&waker));
                    if model.writer {
                        reader_waiting = Some(waiting);
                    }
                    prop_assert_eq!(polled.is_ready(), !model.writer);
                }
                Op::PollReady => {
                    let Some(writer) = &mut writer else { continue };
                    let next_lwm = model.lwm().map(|lwm| (model.read / lwm + 1) * lwm);
                    let until = next_lwm.map_or(Until::ReaderGone, Until::Read);
                    let (waiting, waker) = Waiting::new(until);
                    let polled = writer.poll_ready(&mut Context::from_waker(&waker));
                    let expected = if !model.reader {
                        Poll::Ready(Err(ReaderGone))
                    } else if model.full() {
                        writer_waiting = Some(waiting);
                        Poll::Pending
                    } else {
                        Poll::Ready(Ok(()))
                    };
                    prop_assert_eq!(polled, expected);
                }
                Op::WriterPollClosed => {
                    let Some(writer) = &mut writer else { continue };
                    let (waiting, waker) = Waiting::new(Until::ReaderGone);
                    let polled = writer.poll_closed(&mut Context::from_waker(&waker));
                    if model.reader {
                        writer_waiting = Some(waiting);
                    }
                    prop_assert_eq!(polled.is_ready(), !model.reader);
                }
                Op::DropWriter => {
                    if writer.take().is_some() {
                        model.writer = false;
                    }
                }
                Op::DropReader => {
                    if reader.take().is_some() {
                        model.reader = false;
                    }
                }
            }

            if let Some(reader) = &reader {
                prop_assert_eq!(reader.is_closed(), !model.writer);
            }
            if let Some(writer) = &writer {
                prop_assert_eq!(writer.is_closed(), !model.reader);
            }
            if let Some(waiting) = &reader_waiting {
                let owed = !model.writer || (waiting.until == Until::Item && !model.queue.is_empty());
                prop_assert!(!owed || waiting.woken(), "a waiting reader was not woken ({:?})", waiting.until);
                if waiting.woken() {
                    reader_waiting = None;
                }
            }
            if let Some(waiting) = &writer_waiting {
                let owed = !model.reader || matches!(waiting.until, Until::Read(n) if model.read >= n);
                prop_assert!(!owed || waiting.woken(), "a waiting writer was not woken ({:?})", waiting.until);
                if waiting.woken() {
                    if let Until::Read(_) = waiting.until && model.reader {
                        prop_assert!(!model.full(), "woken without room");
                    }
                    writer_waiting = None;
                }
            }
        }
    }
}
