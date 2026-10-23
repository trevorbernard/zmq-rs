//! Where a socket's messages to one peer go: a message pipe, or a byte log
//! the socket encodes ZMTP 3.x frames into, which the engine writes to the
//! stream as they are.

use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use zmq_log::{LogReader, LogWriter};
use zmq_pipe::{Closed, Reader, TrySendError, Writer};
use zmtp::{FrameWalker, MAX_FRAME_HEADER, Message, frame_header};

/// The sending side of a connection, as the socket holds it.
pub enum Outbound {
    /// Whole messages, for every connection but those below.
    Pipe(Writer<Message>),
    /// Frames already encoded, for PUSH over tcp and ipc.
    Log(LogWriter),
}

/// The receiving side of [`Outbound`], held by the session.
pub(crate) enum SessionOutbound {
    Pipe(Reader<Message>),
    Log {
        reader: LogReader,
        /// Where the octets written to the stream so far end, across
        /// connections, so a new one can start at a message boundary.
        walker: FrameWalker,
    },
}

/// A message too large for the log, part way through being written.
#[derive(Debug)]
pub struct Streaming {
    message: Message,
    /// The frame being written.
    frame: usize,
    /// Octets of that frame, header then body, already written.
    offset: usize,
}

impl Streaming {
    /// Starts writing `message`.
    pub fn new(message: Message) -> Self {
        Self {
            message,
            frame: 0,
            offset: 0,
        }
    }
}

impl Outbound {
    /// Queues `message` if it fits now, whole.
    pub fn try_send(&mut self, message: Message) -> Result<(), TrySendError<Message>> {
        match self {
            Self::Pipe(writer) => writer.try_send(message),
            Self::Log(writer) => {
                if writer.is_closed() {
                    return Err(TrySendError::Closed(message));
                }
                let len = encoded_len(&message);
                if len > writer.capacity() || !writer.has_room(len) {
                    return Err(TrySendError::Full(message));
                }
                write_message(writer, &message);
                Ok(())
            }
        }
    }

    /// Ready once a message can be queued, or with `Err(())` once the peer's
    /// side is gone. For a log, "a message" is one of a single octet; use
    /// [`poll_ready_for`](Self::poll_ready_for) when the message is known.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        match self {
            Self::Pipe(writer) => writer.poll_ready(cx),
            Self::Log(writer) => writer.poll_writable(cx, 1).map_err(drop),
        }
    }

    /// Ready once `message` can be queued whole (or, if it is larger than
    /// the log, once the log is empty enough to start streaming it).
    pub fn poll_ready_for(
        &mut self,
        cx: &mut Context<'_>,
        message: &Message,
    ) -> Poll<Result<(), ()>> {
        match self {
            Self::Pipe(writer) => writer.poll_ready(cx),
            Self::Log(writer) => writer.poll_writable(cx, encoded_len(message)).map_err(drop),
        }
    }

    /// Whether `message` can never be queued whole, so must be streamed.
    pub fn needs_streaming(&self, message: &Message) -> bool {
        match self {
            Self::Pipe(_) => false,
            Self::Log(writer) => encoded_len(message) > writer.capacity(),
        }
    }

    /// Writes as much of a streaming message as fits, committing each piece
    /// so the engine can send it, until it is all written. `Err(())` once
    /// the peer's side is gone; the rest is discarded.
    ///
    /// # Panics
    ///
    /// On a pipe, which never streams.
    pub fn poll_stream(
        &mut self,
        streaming: &mut Streaming,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ()>> {
        let Self::Log(writer) = self else {
            panic!("only a log streams");
        };
        let frames = streaming.message.len().get();
        while streaming.frame < frames {
            let frame = streaming.message.get(streaming.frame).expect("frame");
            let more = streaming.frame + 1 < frames;
            let mut header = [0; MAX_FRAME_HEADER];
            let header_len = frame_header(frame.len() as u64, more, &mut header);
            let total = header_len + frame.len();
            while streaming.offset < total {
                // The header goes in whole, so wait for room for all of it.
                let want = if streaming.offset < header_len {
                    header_len - streaming.offset
                } else {
                    1
                };
                match writer.poll_writable(cx, want) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(_)) => return Poll::Ready(Err(())),
                    Poll::Pending => return Poll::Pending,
                }
                let mut free = writer.free();
                if streaming.offset < header_len {
                    writer.put(&header[streaming.offset..header_len]);
                    free -= header_len - streaming.offset;
                    streaming.offset = header_len;
                }
                let body_offset = streaming.offset - header_len;
                let take = free.min(frame.len() - body_offset);
                writer.put(&frame[body_offset..body_offset + take]);
                streaming.offset += take;
                writer.commit();
            }
            streaming.frame += 1;
            streaming.offset = 0;
        }
        writer.end_message();
        writer.commit();
        Poll::Ready(Ok(()))
    }

    /// Whether the peer's side is gone.
    pub fn is_closed(&self) -> bool {
        match self {
            Self::Pipe(writer) => writer.is_closed(),
            Self::Log(writer) => writer.is_closed(),
        }
    }

    /// Ready once the peer's side is gone.
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match self {
            Self::Pipe(writer) => writer.poll_closed(cx),
            Self::Log(writer) => writer.poll_closed(cx),
        }
    }

    /// Queues `message`, blocking the thread while there is no room, and
    /// streaming it if it is larger than the log.
    pub fn send_blocking(&mut self, message: Message) -> Result<(), Closed<Message>> {
        match self {
            Self::Pipe(writer) => writer.send_blocking(message),
            Self::Log(_) => {
                if self.needs_streaming(&message) {
                    let mut streaming = Streaming::new(message.clone());
                    return block_on(|cx| self.poll_stream(&mut streaming, cx))
                        .map_err(|()| Closed(message));
                }
                let mut slot = Some(message);
                let result = block_on(|cx| {
                    let message = slot.take().expect("message");
                    match self.poll_ready_for(cx, &message) {
                        Poll::Ready(Ok(())) => match self.try_send(message) {
                            Ok(()) => Poll::Ready(Ok(())),
                            Err(TrySendError::Full(back)) => {
                                slot = Some(back);
                                Poll::Pending
                            }
                            Err(TrySendError::Closed(back)) => Poll::Ready(Err(back)),
                        },
                        Poll::Ready(Err(())) => Poll::Ready(Err(message)),
                        Poll::Pending => {
                            slot = Some(message);
                            Poll::Pending
                        }
                    }
                });
                result.map_err(Closed)
            }
        }
    }
}

impl fmt::Debug for Outbound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pipe(writer) => f.debug_tuple("Pipe").field(writer).finish(),
            Self::Log(writer) => f.debug_tuple("Log").field(writer).finish(),
        }
    }
}

impl SessionOutbound {
    /// Whether the socket's side is gone. Messages it queued may remain.
    pub(crate) fn is_closed(&self) -> bool {
        match self {
            Self::Pipe(reader) => reader.is_closed(),
            Self::Log { reader, .. } => reader.is_closed(),
        }
    }

    /// Ready once the socket's side is gone.
    pub(crate) fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match self {
            Self::Pipe(reader) => reader.poll_closed(cx),
            Self::Log { reader, .. } => reader.poll_closed(cx),
        }
    }
}

/// The octets `message` takes as ZMTP 2.0/3.x frames.
fn encoded_len(message: &Message) -> usize {
    message
        .iter()
        .map(|frame| if frame.len() <= 255 { 2 } else { MAX_FRAME_HEADER } + frame.len())
        .sum()
}

/// Writes `message`, which fits, as ZMTP 3.x frames and commits it.
fn write_message(writer: &mut LogWriter, message: &Message) {
    let last = message.len().get() - 1;
    let mut header = [0; MAX_FRAME_HEADER];
    for (i, frame) in message.iter().enumerate() {
        let len = frame_header(frame.len() as u64, i < last, &mut header);
        writer.put(&header[..len]);
        writer.put(frame);
    }
    writer.end_message();
    writer.commit();
}

/// Polls until ready, parking the thread in between.
fn block_on<R>(mut poll: impl FnMut(&mut Context<'_>) -> Poll<R>) -> R {
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
        thread::park();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;
    use zmtp::{Decoder, DecoderConfig, Incoming, Version};

    fn log_outbound(capacity: usize, hwm: usize) -> (Outbound, LogReader) {
        let (writer, reader) =
            zmq_log::log(NonZeroUsize::new(capacity).unwrap(), NonZeroUsize::new(hwm));
        (Outbound::Log(writer), reader)
    }

    fn decode(reader: &mut LogReader) -> Vec<Message> {
        let mut decoder = Decoder::new(Version::V3_1, DecoderConfig::default());
        let [a, b] = reader.committed();
        decoder.extend_from_slice(a);
        decoder.extend_from_slice(b);
        std::iter::from_fn(|| match decoder.decode().unwrap() {
            Some(Incoming::Message(message)) => Some(message),
            Some(Incoming::Command(_)) => panic!("no commands in a log"),
            None => None,
        })
        .collect()
    }

    fn two_part(body: usize) -> Message {
        let mut message = Message::new(vec![1u8; body]);
        message.push(&b"tail"[..]);
        message
    }

    #[test]
    fn messages_are_written_as_zmtp_frames() {
        let (mut tx, mut reader) = log_outbound(1024, 10);
        tx.try_send(two_part(3)).unwrap();
        tx.try_send(two_part(300)).unwrap();
        assert_eq!(decode(&mut reader), [two_part(3), two_part(300)]);
    }

    #[test]
    fn try_send_is_whole_or_nothing() {
        let (mut tx, mut reader) = log_outbound(64, 10);
        tx.try_send(two_part(40)).unwrap();
        let rejected = tx.try_send(two_part(40));
        assert!(matches!(rejected, Err(TrySendError::Full(_))));
        assert_eq!(decode(&mut reader).len(), 1, "nothing of the second");
    }

    #[test]
    fn try_send_respects_the_message_hwm() {
        let (mut tx, _reader) = log_outbound(1024, 2);
        tx.try_send(two_part(1)).unwrap();
        tx.try_send(two_part(1)).unwrap();
        assert!(matches!(
            tx.try_send(two_part(1)),
            Err(TrySendError::Full(_))
        ));
    }

    #[test]
    fn a_message_larger_than_the_log_is_never_sent_whole() {
        let (mut tx, _reader) = log_outbound(64, 10);
        let big = two_part(100);
        assert!(tx.needs_streaming(&big));
        assert!(matches!(tx.try_send(big), Err(TrySendError::Full(_))));
    }

    #[test]
    fn a_message_larger_than_the_log_streams_through_it() {
        let (mut tx, mut reader) = log_outbound(64, 10);
        let big = two_part(1000);
        let expected = big.clone();
        let consumer = thread::spawn(move || {
            let mut octets = Vec::new();
            let waker = Waker::from(Arc::new(Unpark(thread::current())));
            let mut cx = Context::from_waker(&waker);
            loop {
                match reader.poll_committed(&mut cx) {
                    Poll::Ready(true) => {
                        let [a, b] = reader.committed();
                        octets.extend_from_slice(a);
                        octets.extend_from_slice(b);
                        let len = a.len() + b.len();
                        reader.release(len, 0);
                    }
                    Poll::Ready(false) => return octets,
                    Poll::Pending => thread::park(),
                }
            }
        });
        tx.send_blocking(big).unwrap();
        drop(tx);
        let octets = consumer.join().unwrap();
        let mut decoder = Decoder::new(Version::V3_1, DecoderConfig::default());
        decoder.extend_from_slice(&octets);
        assert_eq!(decoder.decode().unwrap(), Some(Incoming::Message(expected)));
    }

    struct Unpark(Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    #[test]
    fn a_closed_reader_hands_the_message_back() {
        let (mut tx, reader) = log_outbound(64, 10);
        drop(reader);
        assert!(tx.is_closed());
        assert!(matches!(
            tx.try_send(two_part(1)),
            Err(TrySendError::Closed(_))
        ));
        assert!(tx.send_blocking(two_part(1)).is_err());
    }
}
