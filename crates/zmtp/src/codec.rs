//! Frame encoding and decoding for every negotiated ZMTP version.
//!
//! - ZMTP 3.0 and 3.1 (RFC 23, 37): `flags | size | body`, where size is one
//!   octet, or eight big-endian octets when the LONG flag is set.
//! - ZMTP 2.0 (RFC 15): the same layout without the COMMAND flag.
//! - ZMTP 1.0 (RFC 13): `length | flags | body`, where length counts the
//!   flags octet and is one octet, or `0xFF` and eight big-endian octets.
//!
//! Decoding is lenient where libzmq is: reserved flag bits are ignored, as is
//! MORE on a command.

use std::fmt;

use bytes::{Buf, Bytes, BytesMut};

use crate::{Frame, Message, Version};

/// Encodes messages and commands for one connection.
#[derive(Debug, Clone, Copy)]
pub struct Encoder {
    version: Version,
}

/// Decodes frames for one connection, reading into a buffer it owns so that
/// large frames are never copied.
#[derive(Debug)]
pub struct Decoder {
    version: Version,
    max_message_size: Option<u64>,
    /// Received octets in `buf[..filled]`, then space that is already zeroed
    /// (or holds stale input) up to `buf.len()`, so it is never zeroed twice.
    buf: BytesMut,
    filled: usize,
    state: State,
    /// A large frame's body, read into a buffer of its own that becomes
    /// the frame (see [`Decoder::fill_into`]); `None` for other frames.
    body: Option<Vec<u8>>,
    partial: Option<Message>,
    /// The size `partial` counts towards the limit.
    partial_size: u64,
    failed: Option<DecodeError>,
}

/// Limits applied while decoding.
#[derive(Debug, Clone, Copy, Default)]
pub struct DecoderConfig {
    /// The largest message accepted: the sum of its frame bodies, where an
    /// empty frame counts as one octet so that a message of endless empty
    /// frames is bounded too. A command counts alone. A message fails at
    /// the header of the frame that takes it over, before any of that body
    /// is buffered. `None` means unlimited.
    pub max_message_size: Option<u64>,
}

/// A decoded unit of traffic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Incoming {
    /// A complete message.
    Message(Message),
    /// The body of a command frame (ZMTP 3.x only).
    Command(Frame),
}

/// Why decoding failed. The connection must be closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// The message, counting its frames so far, is larger than
    /// [`DecoderConfig::max_message_size`], or a frame is larger than this
    /// platform can address.
    MessageTooLarge {
        /// Octets in the message so far, counting the frame that crossed
        /// the limit.
        size: u64,
    },
    /// A ZMTP 1.0 frame length of zero, which cannot hold the flags octet.
    ZeroLength,
    /// A ZMTP 2.0/3.x long size with the top bit set (RFC 37 caps it at
    /// 2^63 - 1).
    SizeOverflow,
}

/// Commands exist only in ZMTP 3.x.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandsUnsupported;

#[derive(Debug, Clone, Copy)]
enum State {
    Header,
    Body {
        size: usize,
        more: bool,
        command: bool,
    },
}

const MORE: u8 = 0x01;
const LONG: u8 = 0x02;
const COMMAND: u8 = 0x04;
/// ZMTP 1.0 marker for an eight-octet length.
const V1_LONG: u8 = 0xFF;

/// Space reserved for reading while between frames.
const READ_SIZE: usize = 8 * 1024;
/// Most space reserved ahead of a large frame's body. Reserving the declared
/// size up front would let a peer make us allocate any amount by lying.
const MAX_RESERVE: usize = 1024 * 1024;
/// Most new space zeroed for one `fill`, so that a small read never pays for
/// zeroing the whole reservation.
const MAX_READ: usize = 64 * 1024;
/// Bodies at least this large are read into a buffer of their own.
const BODY_APART: usize = 16 * 1024;

/// Where [`Decoder::fill_into`] wants input read.
#[derive(Debug)]
pub enum ReadInto<'a> {
    /// Into this initialized space, as [`std::io::Read::read`] reads.
    Slice(&'a mut [u8]),
    /// Appended to this vector, at most this many octets: a large frame's
    /// body, read where it will stay. A reader that can read into a
    /// vector's spare capacity without initializing it should.
    Append(&'a mut Vec<u8>, usize),
}

impl Encoder {
    /// An encoder for `version`.
    pub const fn new(version: Version) -> Self {
        Self { version }
    }

    /// The protocol version it encodes for.
    pub const fn version(&self) -> Version {
        self.version
    }

    /// Appends `message` to `out`, setting MORE on every frame but the last.
    pub fn encode(&self, message: &Message, out: &mut Vec<u8>) {
        let last = message.len().get() - 1;
        for (i, frame) in message.iter().enumerate() {
            let flags = if i < last { MORE } else { 0 };
            self.encode_frame(flags, frame, out);
        }
    }

    /// Encodes `message` as [`encode`](Self::encode) does, except that a
    /// frame of at least `apart` octets is not copied: its header goes to
    /// `out` and its payload to `shared`, with the length `out` has at that
    /// point, for the caller to send from its own buffer there.
    #[inline]
    pub fn encode_parts(
        &self,
        message: Message,
        apart: usize,
        out: &mut Vec<u8>,
        mut shared: impl FnMut(usize, Bytes),
    ) {
        // Looked at by reference first: taking the frames apart costs more
        // than copying small ones, the common case.
        let (first, rest) = message.split_first();
        if first.len() < apart && rest.iter().all(|frame| frame.len() < apart) {
            self.encode(&message, out);
            return;
        }
        let last = message.len().get() - 1;
        for (i, frame) in message.into_iter().enumerate() {
            let flags = if i < last { MORE } else { 0 };
            if frame.len() < apart {
                self.encode_frame(flags, &frame, out);
            } else {
                self.encode_header(flags, frame.len() as u64, out);
                shared(out.len(), frame.into_bytes());
            }
        }
    }

    /// Appends a command frame with `body` (name length, name, data) to `out`.
    pub fn encode_command(
        &self,
        body: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), CommandsUnsupported> {
        match self.version {
            Version::V3_0 | Version::V3_1 => {
                self.encode_frame(COMMAND, body, out);
                Ok(())
            }
            Version::V1 | Version::V2 => Err(CommandsUnsupported),
        }
    }

    /// Writes the header of a data frame of `size` octets, with MORE if
    /// `more`, for a caller that writes or sends the body itself.
    pub fn encode_frame_header(&self, more: bool, size: usize, out: &mut Vec<u8>) {
        self.encode_header(if more { MORE } else { 0 }, size as u64, out);
    }

    fn encode_frame(&self, flags: u8, body: &[u8], out: &mut Vec<u8>) {
        self.encode_header(flags, body.len() as u64, out);
        out.extend_from_slice(body);
    }

    fn encode_header(&self, flags: u8, size: u64, out: &mut Vec<u8>) {
        match self.version {
            Version::V1 => {
                let length = size + 1;
                match u8::try_from(length) {
                    Ok(short) if short != V1_LONG => out.push(short),
                    _ => {
                        out.push(V1_LONG);
                        out.extend_from_slice(&length.to_be_bytes());
                    }
                }
                out.push(flags);
            }
            Version::V2 | Version::V3_0 | Version::V3_1 => match u8::try_from(size) {
                Ok(short) => out.extend_from_slice(&[flags, short]),
                Err(_) => {
                    out.push(flags | LONG);
                    out.extend_from_slice(&size.to_be_bytes());
                }
            },
        }
    }
}

impl Decoder {
    /// A decoder for `version`.
    pub fn new(version: Version, config: DecoderConfig) -> Self {
        Self {
            version,
            max_message_size: config.max_message_size,
            buf: BytesMut::new(),
            filled: 0,
            state: State::Header,
            body: None,
            partial: None,
            partial_size: 0,
            failed: None,
        }
    }

    /// Lets `read` fill the decoder's buffer, and keeps the octets it reports.
    ///
    /// `read` receives initialized space (zeroed, or holding stale input) and
    /// returns how many octets it wrote, as [`std::io::Read::read`] does:
    /// `decoder.fill(|buf| stream.read(buf))`.
    ///
    /// Each region of the buffer is zeroed once, when first offered, rather
    /// than before every read. A large frame's body is zeroed here before
    /// being read: [`fill_into`](Self::fill_into) lets a reader avoid that.
    pub fn fill<E>(
        &mut self,
        read: impl FnOnce(&mut [u8]) -> Result<usize, E>,
    ) -> Result<usize, E> {
        self.fill_into(|into| match into {
            ReadInto::Slice(buf) => read(buf),
            ReadInto::Append(vec, limit) => {
                let start = vec.len();
                vec.resize(start + limit.min(MAX_READ), 0);
                let result = read(&mut vec[start..]);
                let n = result.as_ref().map_or(0, |&n| n.min(vec.len() - start));
                vec.truncate(start + n);
                result.map(|_| n)
            }
        })
    }

    /// As [`fill`](Self::fill), except that `read` is offered a large
    /// frame's body as a vector to append to, which becomes the frame
    /// without a copy, so a reader can fill it without zeroing it first.
    pub fn fill_into<E>(
        &mut self,
        read: impl FnOnce(ReadInto<'_>) -> Result<usize, E>,
    ) -> Result<usize, E> {
        if let (Some(body), State::Body { size, .. }) = (&mut self.body, self.state) {
            let limit = size - body.len();
            let before = body.len();
            let result = read(ReadInto::Append(body, limit));
            debug_assert!(body.len() - before <= limit, "read past the frame");
            return result;
        }
        let wanted = match self.state {
            State::Header => READ_SIZE,
            State::Body { size, .. } => size
                .saturating_sub(self.filled)
                .clamp(READ_SIZE, MAX_RESERVE),
        };
        let target = wanted.min(MAX_READ);
        let spare = self.buf.len() - self.filled;
        if spare < target {
            // Zero only space no read has been offered yet. `reserve` keeps
            // the initialized prefix, including zeroed spare space.
            self.buf.reserve(wanted - spare);
            self.buf.resize(self.filled + target, 0);
        }
        let offered = self.buf.len() - self.filled;
        let result = read(ReadInto::Slice(&mut self.buf[self.filled..]));
        let n = result.as_ref().map_or(0, |&n| n.min(offered));
        self.filled += n;
        result.map(|_| n)
    }

    /// Copies `data` into the buffer, e.g. the replay from a ZMTP 1.0 greeting.
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let end = self.filled + data.len();
        if end > self.buf.len() {
            self.buf.resize(end, 0);
        }
        self.buf[self.filled..end].copy_from_slice(data);
        self.filled = end;
    }

    /// Decodes the next message or command, or `None` if more input is needed.
    pub fn decode(&mut self) -> Result<Option<Incoming>, DecodeError> {
        if let Some(err) = &self.failed {
            return Err(err.clone());
        }
        let result = self.decode_next();
        if let Err(err) = &result {
            self.failed = Some(err.clone());
        }
        result
    }

    fn decode_next(&mut self) -> Result<Option<Incoming>, DecodeError> {
        loop {
            match self.state {
                State::Header => {
                    let Some((header_len, body)) = self.parse_header()? else {
                        return Ok(None);
                    };
                    self.buf.advance(header_len);
                    self.filled -= header_len;
                    self.state = body;
                    if let State::Body { size, .. } = body
                        && size >= BODY_APART
                        && self.filled < size
                    {
                        // Capped as `fill` caps its reservation, against a
                        // peer that declares a size it never sends.
                        let mut apart = Vec::with_capacity(size.min(MAX_RESERVE));
                        apart.extend_from_slice(&self.buf[..self.filled]);
                        self.buf.advance(self.filled);
                        self.filled = 0;
                        self.body = Some(apart);
                    }
                }
                State::Body {
                    size,
                    more,
                    command,
                } => {
                    let frame = match self.body.take() {
                        Some(body) if body.len() < size => {
                            self.body = Some(body);
                            return Ok(None);
                        }
                        Some(body) => Frame::from_shared(Bytes::from(body)),
                        None if self.filled < size => return Ok(None),
                        None => {
                            self.filled -= size;
                            Frame::from_shared(self.buf.split_to(size).freeze())
                        }
                    };
                    self.state = State::Header;
                    if command {
                        return Ok(Some(Incoming::Command(frame)));
                    }
                    let message = match self.partial.take() {
                        Some(mut message) => {
                            message.push(frame);
                            message
                        }
                        None => Message::new(frame),
                    };
                    if !more {
                        self.partial_size = 0;
                        return Ok(Some(Incoming::Message(message)));
                    }
                    self.partial_size += counted_size(size as u64);
                    self.partial = Some(message);
                }
            }
        }
    }

    /// Parses the frame header at the front of the buffer into its length and
    /// the body state that follows it.
    fn parse_header(&self) -> Result<Option<(usize, State)>, DecodeError> {
        let buf = &self.buf[..self.filled];
        let (header_len, size, flags) = match self.version {
            Version::V1 => {
                let Some(&first) = buf.first() else {
                    return Ok(None);
                };
                let (length_len, length) = if first == V1_LONG {
                    let Some(length) = read_u64(&buf[1..]) else {
                        return Ok(None);
                    };
                    (9, length)
                } else {
                    (1, u64::from(first))
                };
                if length == 0 {
                    return Err(DecodeError::ZeroLength);
                }
                let Some(&flags) = buf.get(length_len) else {
                    return Ok(None);
                };
                (length_len + 1, length - 1, flags & MORE)
            }
            Version::V2 | Version::V3_0 | Version::V3_1 => {
                let Some(&flags) = buf.first() else {
                    return Ok(None);
                };
                let (size_len, size) = if flags & LONG == 0 {
                    let Some(&size) = buf.get(1) else {
                        return Ok(None);
                    };
                    (1, u64::from(size))
                } else {
                    let Some(size) = read_u64(&buf[1..]) else {
                        return Ok(None);
                    };
                    if size > i64::MAX as u64 {
                        return Err(DecodeError::SizeOverflow);
                    }
                    (8, size)
                };
                let flags = match self.version {
                    Version::V2 => flags & MORE,
                    _ => flags & (MORE | COMMAND),
                };
                (1 + size_len, size, flags)
            }
        };
        let message_size = if flags & COMMAND == 0 {
            self.partial_size.saturating_add(counted_size(size))
        } else {
            counted_size(size)
        };
        let too_large = self.max_message_size.is_some_and(|max| message_size > max);
        let size = match usize::try_from(size) {
            Ok(size) if !too_large => size,
            _ => return Err(DecodeError::MessageTooLarge { size: message_size }),
        };
        let body = State::Body {
            size,
            more: flags & MORE != 0,
            command: flags & COMMAND != 0,
        };
        Ok(Some((header_len, body)))
    }
}

fn read_u64(buf: &[u8]) -> Option<u64> {
    buf.first_chunk().copied().map(u64::from_be_bytes)
}

/// A frame's size towards [`DecoderConfig::max_message_size`].
fn counted_size(size: u64) -> u64 {
    size.max(1)
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MessageTooLarge { size } => {
                write!(f, "message of {size} octets exceeds the maximum size")
            }
            Self::ZeroLength => f.write_str("ZMTP 1.0 frame length of zero"),
            Self::SizeOverflow => f.write_str("frame size exceeds 2^63 - 1 octets"),
        }
    }
}

impl std::error::Error for DecodeError {}

impl fmt::Display for CommandsUnsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("commands require ZMTP 3.0 or later")
    }
}

impl std::error::Error for CommandsUnsupported {}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [Version; 4] = [Version::V1, Version::V2, Version::V3_0, Version::V3_1];

    fn message(parts: &[&[u8]]) -> Message {
        let frames = parts
            .iter()
            .map(|p| Frame::copy_from_slice(p))
            .collect::<Vec<_>>();
        Message::try_from(frames).unwrap()
    }

    fn encode(version: Version, msg: &Message) -> Vec<u8> {
        let mut out = Vec::new();
        Encoder::new(version).encode(msg, &mut out);
        out
    }

    fn decoder(version: Version) -> Decoder {
        Decoder::new(version, DecoderConfig::default())
    }

    fn decode_all(decoder: &mut Decoder, input: &[u8]) -> Vec<Incoming> {
        decoder.extend_from_slice(input);
        std::iter::from_fn(|| decoder.decode().unwrap()).collect()
    }

    // ---- encoding ----

    #[test]
    fn v3_short_frame() {
        assert_eq!(encode(Version::V3_1, &message(&[b"abc"])), b"\x00\x03abc");
    }

    #[test]
    fn v3_more_on_all_but_last_frame() {
        let out = encode(Version::V3_1, &message(&[b"a", b"", b"b"]));
        assert_eq!(out, b"\x01\x01a\x01\x00\x00\x01b");
    }

    #[test]
    fn v3_long_frame_above_255_octets() {
        let short = encode(Version::V3_1, &message(&[&[7; 255]]));
        assert_eq!(short[..2], [0x00, 255]);
        let long = encode(Version::V3_1, &message(&[&[7; 256]]));
        assert_eq!(long[..9], [0x02, 0, 0, 0, 0, 0, 0, 1, 0]);
        assert_eq!(long.len(), 9 + 256);
    }

    #[test]
    fn v3_commands() {
        let mut out = Vec::new();
        let encoder = Encoder::new(Version::V3_0);
        encoder
            .encode_command(b"\x04PING\x00\x00", &mut out)
            .unwrap();
        assert_eq!(out, b"\x04\x07\x04PING\x00\x00");
        out.clear();
        encoder.encode_command(&[1; 300], &mut out).unwrap();
        assert_eq!(out[..9], [0x06, 0, 0, 0, 0, 0, 0, 1, 44]);
    }

    #[test]
    fn v3_0_and_v3_1_frame_identically() {
        let msg = message(&[b"x", &[1; 1000]]);
        assert_eq!(encode(Version::V3_0, &msg), encode(Version::V3_1, &msg));
    }

    #[test]
    fn v2_frames_like_v3_without_commands() {
        let msg = message(&[b"a", &[2; 300]]);
        assert_eq!(encode(Version::V2, &msg), encode(Version::V3_1, &msg));
        let mut out = Vec::new();
        assert_eq!(
            Encoder::new(Version::V2).encode_command(b"\x04PING", &mut out),
            Err(CommandsUnsupported)
        );
        assert!(out.is_empty());
    }

    #[test]
    fn v1_length_counts_the_flags_octet() {
        assert_eq!(encode(Version::V1, &message(&[b"abc"])), b"\x04\x00abc");
        assert_eq!(
            encode(Version::V1, &message(&[b"a", b"b"])),
            b"\x02\x01a\x02\x00b"
        );
    }

    #[test]
    fn v1_long_length_from_254_octets() {
        let short = encode(Version::V1, &message(&[&[1; 253]]));
        assert_eq!(short[..2], [254, 0x00]);
        let long = encode(Version::V1, &message(&[&[1; 254]]));
        assert_eq!(long[..10], [0xFF, 0, 0, 0, 0, 0, 0, 0, 255, 0x00]);
        assert_eq!(long.len(), 10 + 254);
    }

    #[test]
    fn v1_has_no_commands() {
        let mut out = Vec::new();
        assert_eq!(
            Encoder::new(Version::V1).encode_command(b"\x04PING", &mut out),
            Err(CommandsUnsupported)
        );
        assert_eq!(
            CommandsUnsupported.to_string(),
            "commands require ZMTP 3.0 or later"
        );
    }

    // ---- decoding ----

    #[test]
    fn round_trips_every_version_and_size_class() {
        let sizes = [0, 1, 23, 24, 253, 254, 255, 256, 70_000];
        for version in ALL {
            for size in sizes {
                let body = vec![0xA5; size];
                let msg = message(&[b"head", &body, b""]);
                let decoded = decode_all(&mut decoder(version), &encode(version, &msg));
                assert_eq!(decoded, [Incoming::Message(msg)], "{version:?} size {size}");
            }
        }
    }

    #[test]
    fn decodes_consecutive_messages() {
        for version in ALL {
            let (a, b) = (message(&[b"a"]), message(&[b"b", b"c"]));
            let mut input = encode(version, &a);
            input.extend(encode(version, &b));
            let decoded = decode_all(&mut decoder(version), &input);
            assert_eq!(
                decoded,
                [Incoming::Message(a), Incoming::Message(b)],
                "{version:?}"
            );
        }
    }

    #[test]
    fn waits_for_complete_input() {
        for version in ALL {
            let msg = message(&[b"a", &[3; 300]]);
            let input = encode(version, &msg);
            let mut d = decoder(version);
            for (i, octet) in input.iter().enumerate() {
                d.extend_from_slice(std::slice::from_ref(octet));
                let item = d.decode().unwrap();
                if i + 1 < input.len() {
                    assert_eq!(item, None, "{version:?} at octet {i}");
                } else {
                    assert_eq!(item, Some(Incoming::Message(msg.clone())), "{version:?}");
                }
            }
        }
    }

    #[test]
    fn fill_reads_into_the_decoder_buffer() {
        let input = encode(Version::V3_1, &message(&[b"hello"]));
        let mut d = decoder(Version::V3_1);
        let n = d
            .fill(|buf| {
                buf[..input.len()].copy_from_slice(&input);
                Ok::<_, ()>(input.len())
            })
            .unwrap();
        assert_eq!(n, input.len());
        assert_eq!(
            d.decode().unwrap(),
            Some(Incoming::Message(message(&[b"hello"])))
        );
        assert_eq!(d.decode().unwrap(), None);
    }

    #[test]
    fn fill_reuses_space_without_zeroing_it_again() {
        let input = encode(Version::V3_1, &message(&[b"hello"]));
        let mut d = decoder(Version::V3_1);
        // The reader scribbles past what it reports, as `read` may.
        d.fill(|buf| {
            buf.fill(0xEE);
            buf[..2].copy_from_slice(&input[..2]);
            Ok::<_, ()>(2)
        })
        .unwrap();
        assert_eq!(d.decode().unwrap(), None);
        d.fill(|buf| {
            assert_eq!(buf[0], 0xEE, "spare space was zeroed a second time");
            buf[..5].copy_from_slice(&input[2..]);
            Ok::<_, ()>(5)
        })
        .unwrap();
        assert_eq!(
            d.decode().unwrap(),
            Some(Incoming::Message(message(&[b"hello"])))
        );
        assert_eq!(
            d.decode().unwrap(),
            None,
            "unreported octets are never decoded"
        );
    }

    #[test]
    fn fill_passes_read_errors_through() {
        let mut d = decoder(Version::V3_1);
        assert_eq!(d.fill(|_| Err("boom")), Err("boom"));
        assert_eq!(d.decode().unwrap(), None, "nothing was buffered");
    }

    #[test]
    fn large_frames_are_slices_of_the_read_buffer() {
        let input = encode(Version::V3_1, &message(&[&[9; 4096]]));
        let mut d = decoder(Version::V3_1);
        let mut base = std::ptr::null();
        d.fill(|buf| {
            base = buf.as_ptr();
            buf[..input.len()].copy_from_slice(&input);
            Ok::<_, ()>(input.len())
        })
        .unwrap();
        let Some(Incoming::Message(msg)) = d.decode().unwrap() else {
            panic!("expected a message");
        };
        assert_eq!(
            msg.get(0).unwrap().as_ptr(),
            base.wrapping_add(9),
            "body follows the 9-octet header"
        );
    }

    #[test]
    fn declared_size_does_not_preallocate() {
        let mut d = decoder(Version::V3_1);
        d.extend_from_slice(&[0x02, 0x40, 0, 0, 0, 0, 0, 0, 0]); // 2^62 octets
        assert_eq!(d.decode().unwrap(), None);
        let offered = d.fill(|buf| Ok::<_, ()>(buf.len())).unwrap();
        assert!(offered <= 2 << 20, "offered {offered} octets");
    }

    #[test]
    fn commands_are_passed_through() {
        let mut input = Vec::new();
        let encoder = Encoder::new(Version::V3_1);
        encoder.encode_command(b"\x05READY", &mut input).unwrap();
        let decoded = decode_all(&mut decoder(Version::V3_1), &input);
        assert_eq!(
            decoded,
            [Incoming::Command(Frame::copy_from_slice(b"\x05READY"))]
        );
    }

    #[test]
    fn command_inside_a_multipart_message_does_not_break_it() {
        let mut input = b"\x01\x01a".to_vec();
        Encoder::new(Version::V3_1)
            .encode_command(b"\x04PING\x00\x00", &mut input)
            .unwrap();
        input.extend_from_slice(b"\x00\x01b");
        let decoded = decode_all(&mut decoder(Version::V3_1), &input);
        assert_eq!(
            decoded,
            [
                Incoming::Command(Frame::copy_from_slice(b"\x04PING\x00\x00")),
                Incoming::Message(message(&[b"a", b"b"])),
            ]
        );
    }

    #[test]
    fn more_on_a_command_is_ignored() {
        let input = b"\x05\x01X\x00\x01m";
        let decoded = decode_all(&mut decoder(Version::V3_1), input);
        assert_eq!(
            decoded,
            [
                Incoming::Command(Frame::copy_from_slice(b"X")),
                Incoming::Message(message(&[b"m"])),
            ]
        );
    }

    #[test]
    fn reserved_flag_bits_are_ignored() {
        let decoded = decode_all(&mut decoder(Version::V3_1), b"\xF8\x01a");
        assert_eq!(decoded, [Incoming::Message(message(&[b"a"]))]);
    }

    #[test]
    fn v2_treats_the_command_bit_as_reserved() {
        let decoded = decode_all(&mut decoder(Version::V2), b"\x04\x01a");
        assert_eq!(decoded, [Incoming::Message(message(&[b"a"]))]);
    }

    #[test]
    fn v1_zero_length_fails_and_stays_failed() {
        let mut d = decoder(Version::V1);
        d.extend_from_slice(b"\x00");
        assert_eq!(d.decode(), Err(DecodeError::ZeroLength));
        d.extend_from_slice(b"\x02\x00a");
        assert_eq!(d.decode(), Err(DecodeError::ZeroLength));
    }

    #[test]
    fn v1_long_zero_length_fails() {
        let mut d = decoder(Version::V1);
        d.extend_from_slice(&[0xFF, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(d.decode(), Err(DecodeError::ZeroLength));
    }

    #[test]
    fn long_size_with_top_bit_set_fails() {
        let mut d = decoder(Version::V3_1);
        d.extend_from_slice(&[0x02, 0x80, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(d.decode(), Err(DecodeError::SizeOverflow));
    }

    #[test]
    fn max_message_size_is_enforced_from_the_header() {
        let config = DecoderConfig {
            max_message_size: Some(4),
        };
        for version in ALL {
            let mut d = Decoder::new(version, config);
            d.extend_from_slice(&encode(version, &message(&[b"four"])));
            assert!(
                matches!(d.decode(), Ok(Some(_))),
                "{version:?}: at the limit"
            );

            let mut d = Decoder::new(version, config);
            let input = encode(version, &message(&[b"fives"]));
            let header = input.len() - 5;
            d.extend_from_slice(&input[..header]);
            assert_eq!(
                d.decode(),
                Err(DecodeError::MessageTooLarge { size: 5 }),
                "{version:?}"
            );
        }
    }

    #[test]
    fn max_message_size_counts_every_frame() {
        let config = DecoderConfig {
            max_message_size: Some(4),
        };
        for version in ALL {
            let mut d = Decoder::new(version, config);
            let input = encode(version, &message(&[b"ab", b"cd", b"e"]));
            d.extend_from_slice(&input);
            assert_eq!(
                d.decode(),
                Err(DecodeError::MessageTooLarge { size: 5 }),
                "{version:?}"
            );

            let mut d = Decoder::new(version, config);
            let mut input = encode(version, &message(&[b"ab", b"cd"]));
            input.extend(encode(version, &message(&[b"wxyz"])));
            assert_eq!(
                decode_all(&mut d, &input),
                [
                    Incoming::Message(message(&[b"ab", b"cd"])),
                    Incoming::Message(message(&[b"wxyz"])),
                ],
                "{version:?}: the count restarts with each message"
            );
        }
    }

    // GHSA-h9gj-c9m7-qxqm: a message whose frames all have MORE set, which
    // libzmq buffers without bound.
    #[test]
    fn endless_empty_frames_reach_the_limit() {
        let config = DecoderConfig {
            max_message_size: Some(1000),
        };
        for version in ALL {
            let mut d = Decoder::new(version, config);
            let empty_with_more = encode(version, &message(&[b"", b""]));
            let empty_with_more = &empty_with_more[..empty_with_more.len() / 2];
            d.extend_from_slice(&empty_with_more.repeat(1001));
            assert_eq!(
                d.decode(),
                Err(DecodeError::MessageTooLarge { size: 1001 }),
                "{version:?}"
            );
        }
    }

    #[test]
    fn a_command_counts_alone() {
        let config = DecoderConfig {
            max_message_size: Some(4),
        };
        let mut input = b"\x01\x03abc".to_vec();
        Encoder::new(Version::V3_1)
            .encode_command(b"\x03ABC", &mut input)
            .unwrap();
        input.extend_from_slice(b"\x00\x01d");
        assert_eq!(
            decode_all(&mut Decoder::new(Version::V3_1, config), &input),
            [
                Incoming::Command(Frame::copy_from_slice(b"\x03ABC")),
                Incoming::Message(message(&[b"abc", b"d"])),
            ]
        );
    }

    #[test]
    fn v1_replay_starts_the_identity_frame() {
        let mut d = decoder(Version::V1);
        d.extend_from_slice(b"\x04"); // replay from the greeting
        d.extend_from_slice(b"\x00xyz\x02\x00m");
        assert_eq!(
            d.decode().unwrap(),
            Some(Incoming::Message(message(&[b"xyz"])))
        );
        assert_eq!(
            d.decode().unwrap(),
            Some(Incoming::Message(message(&[b"m"])))
        );
    }

    /// Feeds `wire` to `d` at most `chunk` octets per fill, appending to
    /// large bodies when `append`, otherwise through the slice fallback,
    /// and returns what was decoded.
    fn decode_chunked(d: &mut Decoder, wire: &[u8], chunk: usize, append: bool) -> Vec<Incoming> {
        let mut pos = 0;
        let mut out = Vec::new();
        loop {
            while let Some(item) = d.decode().unwrap() {
                out.push(item);
            }
            if pos == wire.len() {
                return out;
            }
            let take = |n: usize, pos: &mut usize| {
                let n = n.min(chunk).min(wire.len() - *pos);
                *pos += n;
                &wire[*pos - n..*pos]
            };
            if append {
                d.fill_into(|into| match into {
                    ReadInto::Slice(buf) => {
                        let src = take(buf.len(), &mut pos);
                        buf[..src.len()].copy_from_slice(src);
                        Ok::<_, ()>(src.len())
                    }
                    ReadInto::Append(body, limit) => {
                        let src = take(limit, &mut pos);
                        body.extend_from_slice(src);
                        Ok(src.len())
                    }
                })
                .unwrap();
            } else {
                d.fill(|buf| {
                    let src = take(buf.len(), &mut pos);
                    buf[..src.len()].copy_from_slice(src);
                    Ok::<_, ()>(src.len())
                })
                .unwrap();
            }
        }
    }

    #[test]
    fn large_bodies_are_read_apart_in_any_chunks() {
        let big: Vec<u8> = (0..BODY_APART * 3 + 5).map(|i| i as u8).collect();
        let edge = vec![7; BODY_APART];
        let messages = [
            message(&[b"small"]),
            message(&[&big]),
            message(&[b"id", &edge, &big, b"tail"]),
            message(&[&big[..BODY_APART - 1]]),
        ];
        let mut wire = Vec::new();
        for msg in &messages {
            Encoder::new(Version::V3_1).encode(msg, &mut wire);
        }
        let expected: Vec<Incoming> = messages.iter().cloned().map(Incoming::Message).collect();
        for chunk in [1, 7, 1000, BODY_APART - 3, BODY_APART * 4, usize::MAX] {
            for append in [true, false] {
                let mut d = decoder(Version::V3_1);
                assert_eq!(
                    decode_chunked(&mut d, &wire, chunk, append),
                    expected,
                    "chunk {chunk}, append {append}"
                );
            }
        }
    }

    #[test]
    fn a_large_body_is_offered_as_a_vector_with_its_remaining_size() {
        let mut d = decoder(Version::V3_1);
        let mut wire = Vec::new();
        Encoder::new(Version::V3_1).encode(&message(&[&vec![1; BODY_APART]]), &mut wire);
        d.extend_from_slice(&wire[..9 + 100]);
        assert_eq!(d.decode().unwrap(), None);
        let mut offered = None;
        d.fill_into(|into| {
            if let ReadInto::Append(body, limit) = into {
                offered = Some((body.len(), limit));
            }
            Ok::<_, ()>(0)
        })
        .unwrap();
        assert_eq!(offered, Some((100, BODY_APART - 100)));
    }

    #[test]
    fn a_declared_size_is_not_reserved_beyond_the_cap() {
        let mut d = decoder(Version::V3_1);
        let mut header = vec![0x02];
        header.extend_from_slice(&(1u64 << 40).to_be_bytes());
        d.extend_from_slice(&header);
        assert_eq!(d.decode().unwrap(), None);
        let mut capacity = 0;
        d.fill_into(|into| {
            if let ReadInto::Append(body, _) = into {
                capacity = body.capacity();
            }
            Ok::<_, ()>(0)
        })
        .unwrap();
        assert!(capacity <= MAX_RESERVE, "reserved {capacity}");
    }

    #[test]
    fn error_messages() {
        assert_eq!(
            DecodeError::MessageTooLarge { size: 9 }.to_string(),
            "message of 9 octets exceeds the maximum size"
        );
        assert_eq!(
            DecodeError::ZeroLength.to_string(),
            "ZMTP 1.0 frame length of zero"
        );
        assert_eq!(
            DecodeError::SizeOverflow.to_string(),
            "frame size exceeds 2^63 - 1 octets"
        );
    }
}
