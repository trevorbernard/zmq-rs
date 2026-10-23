//! Where a socket's messages from one peer arrive: a message pipe, or a byte
//! log of ZMTP 3.x frames the engine read straight from the stream, decoded
//! here on the socket's own thread.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use zmq_log::{LogReader, LogWriter};
use zmq_pipe::{Reader, TryRecvError, Writer};
use zmtp::{Decoder, DecoderConfig, Incoming, Message, Version};

/// The receiving side of a connection, as the socket holds it.
pub enum Inbound {
    /// Whole messages, for every connection but those below.
    Pipe(Reader<Message>),
    /// Frames as the peer sent them, for PULL over tcp and ipc.
    Log(Box<LogInbound>),
}

/// The sending side of [`Inbound`], held by the session.
pub(crate) enum SessionInbound {
    Pipe(Writer<Message>),
    Log(LogWriter),
}

/// A log of frames and the decoding of it.
pub struct LogInbound {
    reader: LogReader,
    decoder: Decoder,
    /// Messages handed out whose release has not been published yet; they
    /// are released with the next octets, or before waiting.
    delivered: u64,
    /// The abandonment mark already acted on.
    handled_mark: Option<u64>,
}

/// A message lent by [`Inbound::poll_recv_with`]: its frames, in place in
/// the log when it lies whole there, otherwise in a decoded copy.
#[derive(Clone, Copy)]
pub struct MessageRef<'a>(Repr<'a>);

#[derive(Clone, Copy)]
enum Repr<'a> {
    /// ZMTP 3.x message frames, without commands.
    Encoded {
        raw: &'a [u8],
        len: NonZeroUsize,
    },
    Owned(&'a Message),
}

/// The frames of a [`MessageRef`], in order.
pub struct Frames<'a>(FramesRepr<'a>);

enum FramesRepr<'a> {
    Encoded(&'a [u8]),
    Owned(zmtp::Iter<'a>),
}

const MORE: u8 = 0x01;
const LONG: u8 = 0x02;
const COMMAND: u8 = 0x04;

impl LogInbound {
    pub(crate) fn new(reader: LogReader) -> Self {
        Self {
            reader,
            decoder: socket_decoder(),
            delivered: 0,
            handled_mark: None,
        }
    }

    /// The next message. With no `cx`, nothing is registered to wake: a
    /// waker registered earlier stays in place.
    fn poll_recv(&mut self, mut cx: Option<&mut Context<'_>>) -> Poll<Option<Message>> {
        loop {
            match self.decoder.decode() {
                Ok(Some(Incoming::Message(message))) => {
                    self.delivered += 1;
                    return Poll::Ready(Some(message));
                }
                Ok(Some(Incoming::Command(_))) => continue,
                Ok(None) => {}
                Err(err) => {
                    // The engine walked these octets and found them sound.
                    debug_assert!(false, "a walked log failed to decode: {err}");
                    return Poll::Ready(None);
                }
            }
            match self.feed(cx.as_deref_mut()) {
                Poll::Ready(true) => {}
                Poll::Ready(false) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Moves committed octets into the decoder, up to any abandonment mark,
    /// and resets the decoder at the mark. `Ready(false)` once the writer
    /// is gone and everything is read.
    fn feed(&mut self, cx: Option<&mut Context<'_>>) -> Poll<bool> {
        let mut cx = cx;
        loop {
            let position = self.reader.position();
            let mark = self
                .reader
                .abandoned_at()
                .filter(|&mark| Some(mark) != self.handled_mark);
            if mark == Some(position) {
                // The rest of that message will never come.
                self.decoder = socket_decoder();
                self.handled_mark = mark;
                continue;
            }
            let [first, second] = self.reader.committed();
            let available = first.len() + second.len();
            let len = match mark {
                Some(mark) if mark > position => available.min((mark - position) as usize),
                _ => available,
            };
            if len == 0 {
                self.reader.release(0, std::mem::take(&mut self.delivered));
                let Some(cx) = cx.as_deref_mut() else {
                    return if self.reader.is_closed() && self.reader.pending() == 0 {
                        Poll::Ready(false)
                    } else {
                        Poll::Pending
                    };
                };
                return match self.reader.poll_committed(cx) {
                    Poll::Ready(true) => continue,
                    Poll::Ready(false) => Poll::Ready(false),
                    Poll::Pending => Poll::Pending,
                };
            }
            let in_first = len.min(first.len());
            self.decoder.extend_from_slice(&first[..in_first]);
            self.decoder.extend_from_slice(&second[..len - in_first]);
            self.reader
                .release(len, std::mem::take(&mut self.delivered));
            return Poll::Ready(true);
        }
    }

    /// Lends the next message in place if it lies whole in the log, before
    /// any abandonment mark, with nothing held by the decoder. Command
    /// frames ahead of it are skipped. `None` when it cannot.
    fn try_lend<R>(&mut self, f: &mut Option<impl FnOnce(MessageRef<'_>) -> R>) -> Option<R> {
        if !self.decoder.is_idle() {
            return None;
        }
        let position = self.reader.position();
        let limit = self
            .reader
            .abandoned_at()
            .filter(|&mark| Some(mark) != self.handled_mark && mark > position)
            .map(|mark| (mark - position) as usize);
        let [first, _] = self.reader.committed();
        let first = &first[..limit.map_or(first.len(), |limit| first.len().min(limit))];
        let mut pos = 0;
        let mut start = 0;
        let mut frames = 0usize;
        loop {
            let (header, size, flags) = parse_header(&first[pos..])?;
            let end = pos.checked_add(header)?.checked_add(size)?;
            if end > first.len() {
                return None;
            }
            if flags & COMMAND != 0 && frames == 0 {
                pos = end;
                start = end;
                continue;
            }
            frames += 1;
            pos = end;
            if flags & MORE == 0 {
                break;
            }
        }
        let message = MessageRef(Repr::Encoded {
            raw: &first[start..pos],
            len: NonZeroUsize::new(frames).expect("one frame at least"),
        });
        let lent = (f.take().expect("lent once"))(message);
        self.reader
            .release(pos, std::mem::take(&mut self.delivered) + 1);
        Some(lent)
    }
}

/// Parses a ZMTP 3.x frame header: its length, the body's, and the flags.
fn parse_header(buf: &[u8]) -> Option<(usize, usize, u8)> {
    let &flags = buf.first()?;
    if flags & LONG == 0 {
        Some((2, usize::from(*buf.get(1)?), flags))
    } else {
        let size = u64::from_be_bytes(buf.get(1..9)?.try_into().ok()?);
        Some((9, usize::try_from(size).ok()?, flags))
    }
}

fn socket_decoder() -> Decoder {
    Decoder::new(Version::V3_1, DecoderConfig::default())
}

impl Inbound {
    /// The next message, or `None` once the peer's side is gone and
    /// everything it sent has been read.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Message>> {
        match self {
            Self::Pipe(reader) => reader.poll_recv(cx),
            Self::Log(log) => log.poll_recv(Some(cx)),
        }
    }

    /// Lends the next message to `f`, which is taken from its slot only
    /// when a message is ready; `None` once the peer's side is gone and
    /// everything it sent has been read. In a log, a message lying whole and
    /// unwrapped is lent in place, without copying.
    pub fn poll_recv_with<R>(
        &mut self,
        cx: &mut Context<'_>,
        f: &mut Option<impl FnOnce(MessageRef<'_>) -> R>,
    ) -> Poll<Option<R>> {
        if let Self::Log(log) = self
            && let Some(lent) = log.try_lend(f)
        {
            return Poll::Ready(Some(lent));
        }
        self.poll_recv(cx).map(|message| {
            message.map(|message| (f.take().expect("lent once"))(MessageRef(Repr::Owned(&message))))
        })
    }

    /// The next message, if one is waiting.
    pub fn try_recv(&mut self) -> Result<Message, TryRecvError> {
        let polled = match self {
            Self::Pipe(reader) => return reader.try_recv(),
            Self::Log(log) => log.poll_recv(None),
        };
        match polled {
            Poll::Ready(Some(message)) => Ok(message),
            Poll::Ready(None) => Err(TryRecvError::Closed),
            Poll::Pending => Err(TryRecvError::Empty),
        }
    }

    /// The next message, blocking the thread until one arrives; `None` once
    /// the peer's side is gone and everything it sent has been read.
    pub fn recv_blocking(&mut self) -> Option<Message> {
        match self {
            Self::Pipe(reader) => reader.recv_blocking(),
            Self::Log(_) => block_on(|cx| self.poll_recv(cx)),
        }
    }

    /// Whether the peer's side is gone. Messages it sent may remain.
    pub fn is_closed(&self) -> bool {
        match self {
            Self::Pipe(reader) => reader.is_closed(),
            Self::Log(log) => log.reader.is_closed(),
        }
    }
}

impl fmt::Debug for Inbound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pipe(reader) => f.debug_tuple("Pipe").field(reader).finish(),
            Self::Log(log) => f.debug_tuple("Log").field(&log.reader).finish(),
        }
    }
}

impl fmt::Debug for LogInbound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogInbound")
            .field("reader", &self.reader)
            .finish_non_exhaustive()
    }
}

impl<'a> MessageRef<'a> {
    /// How many frames it has. Never zero.
    pub fn len(&self) -> NonZeroUsize {
        match self.0 {
            Repr::Encoded { len, .. } => len,
            Repr::Owned(message) => message.len(),
        }
    }

    /// The frames, in order.
    pub fn frames(&self) -> Frames<'a> {
        Frames(match self.0 {
            Repr::Encoded { raw, .. } => FramesRepr::Encoded(raw),
            Repr::Owned(message) => FramesRepr::Owned(message.iter()),
        })
    }

    /// The frame at `index`.
    pub fn get(&self, index: usize) -> Option<&'a [u8]> {
        self.frames().nth(index)
    }

    /// An owned copy.
    pub fn to_message(&self) -> Message {
        match self.0 {
            Repr::Owned(message) => message.clone(),
            Repr::Encoded { .. } => {
                let mut frames = self.frames();
                let mut message = Message::new(zmtp::Frame::copy_from_slice(
                    frames.next().expect("one frame at least"),
                ));
                for frame in frames {
                    message.push(zmtp::Frame::copy_from_slice(frame));
                }
                message
            }
        }
    }
}

impl fmt::Debug for MessageRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.frames()).finish()
    }
}

impl PartialEq<Message> for MessageRef<'_> {
    fn eq(&self, other: &Message) -> bool {
        self.len() == other.len()
            && self
                .frames()
                .zip(other.iter())
                .all(|(ours, theirs)| ours == theirs.as_slice())
    }
}

impl<'a> Iterator for Frames<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        match &mut self.0 {
            FramesRepr::Owned(iter) => iter.next().map(|frame| frame.as_slice()),
            FramesRepr::Encoded(raw) => {
                let (header, size, _) = parse_header(raw)?;
                let frame = &raw[header..header + size];
                *raw = &raw[header + size..];
                Some(frame)
            }
        }
    }
}

impl fmt::Debug for Frames<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Frames").finish_non_exhaustive()
    }
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
    use zmtp::Encoder;

    fn log_inbound(capacity: usize, hwm: usize) -> (LogWriter, Inbound) {
        let (writer, reader) =
            zmq_log::log(NonZeroUsize::new(capacity).unwrap(), NonZeroUsize::new(hwm));
        (writer, Inbound::Log(Box::new(LogInbound::new(reader))))
    }

    fn put(writer: &mut LogWriter, message: &Message) {
        let mut out = Vec::new();
        Encoder::new(Version::V3_1).encode(message, &mut out);
        writer.put(&out);
        writer.end_message();
        writer.commit();
    }

    fn put_command(writer: &mut LogWriter, body: &[u8]) {
        let mut out = Vec::new();
        Encoder::new(Version::V3_1)
            .encode_command(body, &mut out)
            .unwrap();
        writer.put(&out);
        writer.commit();
    }

    fn two_part(body: usize) -> Message {
        let mut message = Message::new(vec![3u8; body]);
        message.push(&b"tail"[..]);
        message
    }

    #[test]
    fn messages_are_copied_out_in_order_and_commands_skipped() {
        let (mut writer, mut inbound) = log_inbound(1024, 10);
        put(&mut writer, &two_part(3));
        put_command(&mut writer, b"\x04PING\x00\x00");
        put(&mut writer, &two_part(300));
        assert_eq!(inbound.try_recv(), Ok(two_part(3)));
        assert_eq!(inbound.try_recv(), Ok(two_part(300)));
        assert_eq!(inbound.try_recv(), Err(TryRecvError::Empty));
        drop(writer);
        assert_eq!(inbound.try_recv(), Err(TryRecvError::Closed));
    }

    #[test]
    fn a_whole_message_is_lent_in_place() {
        let (mut writer, mut inbound) = log_inbound(1024, 10);
        put_command(&mut writer, b"\x04PING\x00\x00");
        put(&mut writer, &two_part(5));
        let mut f = Some(|message: MessageRef<'_>| {
            assert!(matches!(message.0, Repr::Encoded { .. }), "lent in place");
            assert_eq!(message.len().get(), 2);
            assert_eq!(message.get(1), Some(&b"tail"[..]));
            message.to_message()
        });
        let lent = inbound.poll_recv_with(&mut Context::from_waker(Waker::noop()), &mut f);
        assert_eq!(lent, Poll::Ready(Some(two_part(5))));
    }

    #[test]
    fn a_wrapped_message_is_lent_from_a_copy() {
        let (mut writer, mut inbound) = log_inbound(64, 10);
        put(&mut writer, &Message::new(vec![1u8; 40]));
        assert_eq!(inbound.try_recv(), Ok(Message::new(vec![1u8; 40])));
        put(&mut writer, &two_part(20));
        let mut f = Some(|message: MessageRef<'_>| message.to_message());
        let lent = inbound.poll_recv_with(&mut Context::from_waker(Waker::noop()), &mut f);
        assert_eq!(lent, Poll::Ready(Some(two_part(20))));
    }

    #[test]
    fn the_rest_of_an_abandoned_message_is_dropped() {
        let (mut writer, mut inbound) = log_inbound(1024, 10);
        let mut out = Vec::new();
        Encoder::new(Version::V3_1).encode(&two_part(100), &mut out);
        writer.put(&out[..50]);
        writer.commit();
        assert_eq!(inbound.try_recv(), Err(TryRecvError::Empty));
        writer.put(&out[50..60]);
        writer.abandon_message().unwrap();
        put(&mut writer, &Message::from("next"));
        assert_eq!(inbound.try_recv(), Ok(Message::from("next")));
    }

    #[test]
    fn messages_count_against_the_hwm_once_read() {
        let (mut writer, mut inbound) = log_inbound(1024, 2);
        put(&mut writer, &Message::from("a"));
        put(&mut writer, &Message::from("b"));
        assert!(!writer.has_room(1));
        assert_eq!(inbound.try_recv(), Ok(Message::from("a")));
        assert_eq!(inbound.try_recv(), Ok(Message::from("b")));
        assert_eq!(inbound.try_recv(), Err(TryRecvError::Empty));
        assert!(writer.has_room(1), "released before waiting");
    }
}
