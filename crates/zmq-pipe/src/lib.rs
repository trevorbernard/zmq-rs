//! Lock-free single-producer, single-consumer message pipes.
//!
//! The Rust counterpart of libzmq's `ypipe`: a queue between exactly one
//! [`Writer`] and one [`Reader`], with a high-water mark and wakers so either
//! side can wait by blocking the thread or by `.await`, on any executor.
//!
//! Like libzmq, a writer that has hit the high-water mark is woken once the
//! reader has caught up to the low-water mark, half the high-water mark,
//! rather than after every message.

use std::fmt;
use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use atomic_waker::AtomicWaker;

/// How many messages a pipe holds before the writer must wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Hwm {
    /// No limit, like a libzmq HWM of zero.
    #[default]
    Unlimited,
    /// At most this many queued messages.
    Messages(NonZeroUsize),
}

/// Creates a pipe.
pub fn pipe<T>(hwm: Hwm) -> (Writer<T>, Reader<T>) {
    let (sender, receiver) = mpsc::channel();
    let shared = Arc::new(Shared::default());
    let hwm = match hwm {
        Hwm::Unlimited => None,
        Hwm::Messages(n) => Some(n.get() as u64),
    };
    let writer = Writer {
        sender: Some(sender),
        shared: shared.clone(),
        written: 0,
        hwm,
    };
    let reader = Reader {
        receiver,
        shared,
        read: 0,
        // libzmq's pipe_t::compute_lwm.
        lwm: hwm.map(|hwm| hwm.div_ceil(2)),
    };
    (writer, reader)
}

/// State both ends see.
#[derive(Debug, Default)]
struct Shared {
    /// Items the reader has taken, for the writer's high-water mark check.
    read: AtomicU64,
    /// Set by the reader before it goes away.
    reader_closed: AtomicBool,
    reader_waker: AtomicWaker,
    writer_waker: AtomicWaker,
}

/// The sending end of a pipe.
#[derive(Debug)]
pub struct Writer<T> {
    /// `None` only while dropping, so the reader can see the disconnect.
    sender: Option<mpsc::Sender<T>>,
    shared: Arc<Shared>,
    written: u64,
    hwm: Option<u64>,
}

/// The receiving end of a pipe.
#[derive(Debug)]
pub struct Reader<T> {
    receiver: mpsc::Receiver<T>,
    shared: Arc<Shared>,
    read: u64,
    lwm: Option<u64>,
}

/// Why [`Writer::try_send`] did not queue the item, which it hands back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The pipe is at its high-water mark.
    Full(T),
    /// The reader is gone.
    Closed(T),
}

/// The reader is gone; the item is handed back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Closed<T>(pub T);

/// Why [`Reader::try_recv`] returned nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// Nothing is queued yet.
    Empty,
    /// The writer is gone and everything it sent has been received.
    Closed,
}

impl<T> Writer<T> {
    /// Queues `item` if the pipe is below its high-water mark.
    pub fn try_send(&mut self, item: T) -> Result<(), TrySendError<T>> {
        if self.is_closed() {
            return Err(TrySendError::Closed(item));
        }
        if self.is_full() {
            return Err(TrySendError::Full(item));
        }
        let Some(sender) = &self.sender else {
            return Err(TrySendError::Closed(item));
        };
        sender
            .send(item)
            .map_err(|mpsc::SendError(item)| TrySendError::Closed(item))?;
        self.written += 1;
        self.shared.reader_waker.wake();
        Ok(())
    }

    /// Ready once an item can be queued, or with `Err(())` once the reader
    /// is gone.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        if let Poll::Ready(ready) = self.ready() {
            return Poll::Ready(ready);
        }
        // Register, then check again so a read in between is not missed.
        self.shared.writer_waker.register(cx.waker());
        self.ready()
    }

    /// Queues `item`, waiting while the pipe is full.
    pub async fn send(&mut self, mut item: T) -> Result<(), Closed<T>> {
        loop {
            if poll_fn(|cx| self.poll_ready(cx)).await.is_err() {
                return Err(Closed(item));
            }
            match self.try_send(item) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(rejected)) => item = rejected,
                Err(TrySendError::Closed(rejected)) => return Err(Closed(rejected)),
            }
        }
    }

    /// Queues `item`, blocking the thread while the pipe is full.
    pub fn send_blocking(&mut self, mut item: T) -> Result<(), Closed<T>> {
        loop {
            if block_on_poll(|cx| self.poll_ready(cx)).is_err() {
                return Err(Closed(item));
            }
            match self.try_send(item) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(rejected)) => item = rejected,
                Err(TrySendError::Closed(rejected)) => return Err(Closed(rejected)),
            }
        }
    }

    /// Whether the reader is gone.
    pub fn is_closed(&self) -> bool {
        self.shared.reader_closed.load(Ordering::Acquire)
    }

    fn is_full(&self) -> bool {
        self.hwm
            .is_some_and(|hwm| self.written - self.shared.read.load(Ordering::Acquire) >= hwm)
    }

    fn ready(&self) -> Poll<Result<(), ()>> {
        if self.is_closed() {
            Poll::Ready(Err(()))
        } else if self.is_full() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<T> Drop for Writer<T> {
    fn drop(&mut self) {
        // Disconnect first, so a reader woken here sees the pipe closed.
        drop(self.sender.take());
        self.shared.reader_waker.wake();
    }
}

impl<T> Reader<T> {
    /// Takes the next item if one is queued.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        match self.receiver.try_recv() {
            Ok(item) => {
                self.read += 1;
                self.shared.read.store(self.read, Ordering::Release);
                if self.lwm.is_some_and(|lwm| self.read.is_multiple_of(lwm)) {
                    self.shared.writer_waker.wake();
                }
                Ok(item)
            }
            Err(mpsc::TryRecvError::Empty) => Err(TryRecvError::Empty),
            Err(mpsc::TryRecvError::Disconnected) => Err(TryRecvError::Closed),
        }
    }

    /// The next item, or `None` once the writer is gone and the pipe drained.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if let Poll::Ready(item) = self.ready() {
            return Poll::Ready(item);
        }
        // Register, then check again so a send in between is not missed.
        self.shared.reader_waker.register(cx.waker());
        self.ready()
    }

    /// The next item, waiting while the pipe is empty.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// The next item, blocking the thread while the pipe is empty.
    pub fn recv_blocking(&mut self) -> Option<T> {
        block_on_poll(|cx| self.poll_recv(cx))
    }

    fn ready(&mut self) -> Poll<Option<T>> {
        match self.try_recv() {
            Ok(item) => Poll::Ready(Some(item)),
            Err(TryRecvError::Closed) => Poll::Ready(None),
            Err(TryRecvError::Empty) => Poll::Pending,
        }
    }
}

impl<T> Drop for Reader<T> {
    fn drop(&mut self) {
        self.shared.reader_closed.store(true, Ordering::Release);
        self.shared.writer_waker.wake();
    }
}

/// Polls until ready, parking the thread in between.
fn block_on_poll<R>(mut poll: impl FnMut(&mut Context<'_>) -> Poll<R>) -> R {
    struct Unpark(Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(output) = poll(&mut cx) {
            return output;
        }
        // Spurious wakeups are harmless: the loop polls again.
        thread::park();
    }
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Empty => "pipe is empty",
            Self::Closed => "pipe is closed",
        })
    }
}

impl std::error::Error for TryRecvError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    fn hwm(n: usize) -> Hwm {
        Hwm::Messages(NonZeroUsize::new(n).unwrap())
    }

    /// A waker that counts how often it was woken.
    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counting_waker() -> (Arc<Count>, Waker) {
        let count = Arc::new(Count::default());
        (count.clone(), Waker::from(count))
    }

    fn wakes(count: &Count) -> usize {
        count.0.load(Ordering::SeqCst)
    }

    #[test]
    fn handles_are_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Writer<Vec<u8>>>();
        assert_send::<Reader<Vec<u8>>>();
    }

    #[test]
    fn items_arrive_in_order() {
        let (mut writer, mut reader) = pipe(Hwm::Unlimited);
        for i in 0..100 {
            writer.try_send(i).unwrap();
        }
        let received: Vec<i32> = std::iter::from_fn(|| reader.try_recv().ok()).collect();
        assert_eq!(received, (0..100).collect::<Vec<_>>());
        assert_eq!(reader.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn unlimited_pipe_is_never_full() {
        let (mut writer, _reader) = pipe(Hwm::Unlimited);
        for i in 0..100_000 {
            writer.try_send(i).unwrap();
        }
    }

    #[test]
    fn writer_stops_at_the_high_water_mark() {
        let (mut writer, mut reader) = pipe(hwm(4));
        for i in 0..4 {
            writer.try_send(i).unwrap();
        }
        assert_eq!(writer.try_send(4), Err(TrySendError::Full(4)));
        assert_eq!(reader.try_recv(), Ok(0));
        writer.try_send(4).unwrap();
        assert_eq!(
            writer.try_send(5),
            Err(TrySendError::Full(5)),
            "never more than 4 queued"
        );
    }

    #[test]
    fn reader_drains_then_sees_closed() {
        let (mut writer, mut reader) = pipe(Hwm::Unlimited);
        writer.try_send(1).unwrap();
        drop(writer);
        assert_eq!(reader.try_recv(), Ok(1));
        assert_eq!(reader.try_recv(), Err(TryRecvError::Closed));
        assert_eq!(reader.recv_blocking(), None);
    }

    #[test]
    fn send_to_a_dropped_reader_returns_the_item() {
        let (mut writer, reader) = pipe(hwm(1));
        drop(reader);
        assert!(writer.is_closed());
        assert_eq!(writer.try_send(7), Err(TrySendError::Closed(7)));
        assert_eq!(writer.send_blocking(8), Err(Closed(8)));
    }

    #[test]
    fn pending_reader_is_woken_by_send() {
        let (mut writer, mut reader) = pipe(Hwm::Unlimited);
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(reader.poll_recv(&mut cx), Poll::Pending);
        writer.try_send(1).unwrap();
        assert_eq!(wakes(&count), 1);
        assert_eq!(reader.poll_recv(&mut cx), Poll::Ready(Some(1)));
    }

    #[test]
    fn pending_reader_is_woken_when_writer_drops() {
        let (writer, mut reader) = pipe::<i32>(Hwm::Unlimited);
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(reader.poll_recv(&mut cx), Poll::Pending);
        drop(writer);
        assert_eq!(wakes(&count), 1);
        assert_eq!(reader.poll_recv(&mut cx), Poll::Ready(None));
    }

    #[test]
    fn full_writer_is_woken_at_the_low_water_mark() {
        let (mut writer, mut reader) = pipe(hwm(4)); // low-water mark 2
        for i in 0..4 {
            writer.try_send(i).unwrap();
        }
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(writer.poll_ready(&mut cx), Poll::Pending);
        reader.try_recv().unwrap();
        assert_eq!(
            wakes(&count),
            0,
            "one read is not enough to wake the writer"
        );
        reader.try_recv().unwrap();
        assert_eq!(wakes(&count), 1, "woken at the low-water mark");
        assert_eq!(writer.poll_ready(&mut cx), Poll::Ready(Ok(())));
    }

    #[test]
    fn pending_writer_is_woken_when_reader_drops() {
        let (mut writer, reader) = pipe(hwm(1));
        writer.try_send(0).unwrap();
        let (count, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(writer.poll_ready(&mut cx), Poll::Pending);
        drop(reader);
        assert_eq!(wakes(&count), 1);
        assert_eq!(writer.poll_ready(&mut cx), Poll::Ready(Err(())));
    }

    /// A minimal `block_on`: polls with a waker that unparks this thread.
    fn block_on<F: Future>(future: F) -> F::Output {
        struct Unpark(thread::Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(Unpark(thread::current())));
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
            thread::park();
        }
    }

    #[test]
    fn async_send_and_recv_need_no_runtime() {
        let (mut writer, mut reader) = pipe(hwm(1));
        let consumer = thread::spawn(move || {
            block_on(async {
                let mut received = Vec::new();
                while let Some(item) = reader.recv().await {
                    received.push(item);
                }
                received
            })
        });
        block_on(async {
            for i in 0..1_000 {
                writer.send(i).await.unwrap();
            }
        });
        drop(writer);
        assert_eq!(consumer.join().unwrap(), (0..1_000).collect::<Vec<_>>());
    }

    #[test]
    fn blocking_threads_never_lose_a_wakeup() {
        for hwm_size in [1, 2, 3, 16] {
            let (mut writer, mut reader) = pipe(hwm(hwm_size));
            let consumer = thread::spawn(move || {
                let mut expected = 0u64;
                while let Some(item) = reader.recv_blocking() {
                    assert_eq!(item, expected);
                    expected += 1;
                }
                expected
            });
            let total = 200_000u64;
            for i in 0..total {
                writer.send_blocking(i).unwrap();
            }
            drop(writer);
            assert_eq!(consumer.join().unwrap(), total, "hwm {hwm_size}");
        }
    }

    #[test]
    fn blocking_recv_waits_for_a_late_writer() {
        let (mut writer, mut reader) = pipe(Hwm::Unlimited);
        let consumer = thread::spawn(move || reader.recv_blocking());
        thread::sleep(Duration::from_millis(20));
        writer.try_send("late").unwrap();
        assert_eq!(consumer.join().unwrap(), Some("late"));
    }

    #[test]
    fn error_messages() {
        assert_eq!(TryRecvError::Empty.to_string(), "pipe is empty");
        assert_eq!(TryRecvError::Closed.to_string(), "pipe is closed");
    }
}
