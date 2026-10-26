//! What an engine has yet to write: copied octets, and large payloads sent
//! from their own buffers, as libzmq's encoder sends a message too large
//! for its batch straight from the message.

use std::collections::VecDeque;
use std::io::{self, IoSlice, Write};

use bytes::{Buf, Bytes};
use zmtp::{Encoder, Message};

/// Payloads at least this large are sent from their own buffers rather
/// than copied, as libzmq's 8 KiB batch.
const SHARED_MIN: usize = 8 * 1024;

/// Slices handed to one vectored write.
const MAX_SLICES: usize = 64;

/// Output in order: `buf[pos..]`, with each shared payload sent where it
/// was added, before the octets copied after it.
#[derive(Debug, Default)]
pub(crate) struct Output {
    /// Copied octets: headers, small frames, commands, boxes.
    pub(crate) buf: Vec<u8>,
    /// Octets of `buf` already written.
    pos: usize,
    /// Payloads sent from their own buffers, each with the length `buf`
    /// had when it was added. Written ones are dropped; a partly written
    /// one is advanced.
    shared: VecDeque<(usize, Bytes)>,
    shared_len: usize,
}

impl Output {
    /// Encodes `message`, sending its large frames from their own buffers.
    #[inline]
    pub(crate) fn encode(&mut self, encoder: Encoder, message: Message) {
        let Self {
            buf,
            shared,
            shared_len,
            ..
        } = self;
        encoder.encode_parts(message, SHARED_MIN, buf, |at, payload| {
            *shared_len += payload.len();
            shared.push_back((at, payload));
        });
    }

    /// Queues `payload` after everything added so far.
    #[cfg(test)]
    fn push_shared(&mut self, payload: Bytes) {
        self.shared_len += payload.len();
        self.shared.push_back((self.buf.len(), payload));
    }

    /// Octets not yet written.
    pub(crate) fn len(&self) -> usize {
        self.buf.len() - self.pos + self.shared_len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Writes until done or the stream would block. Returns whether
    /// anything was written.
    pub(crate) fn write_to(&mut self, stream: &mut impl Write) -> io::Result<bool> {
        let mut progressed = false;
        loop {
            // Without shared payloads, the common case, a plain write.
            let plain = self.shared.is_empty();
            let written = if plain {
                if self.pos == self.buf.len() {
                    break;
                }
                stream.write(&self.buf[self.pos..])
            } else {
                let mut slices = [IoSlice::new(&[]); MAX_SLICES];
                let n = self.slices(&mut slices);
                stream.write_vectored(&slices[..n])
            };
            match written {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => {
                    if plain {
                        self.pos += n;
                    } else {
                        self.advance(n);
                    }
                    progressed = true;
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => return Err(err),
            }
        }
        if self.is_empty() {
            self.buf.clear();
            self.pos = 0;
        } else if self.pos > 0 && self.pos >= self.buf.len() - self.pos {
            // A connection that never drains fully would otherwise keep
            // everything it sent. Moving what is left costs no more than
            // writing what was dropped did.
            self.buf.drain(..self.pos);
            for (at, _) in &mut self.shared {
                *at -= self.pos;
            }
            self.pos = 0;
        }
        Ok(progressed)
    }

    /// Fills `slices` with the unwritten output in order, on the stack
    /// rather than in a vector allocated per write. Returns how many.
    fn slices<'a>(&'a self, slices: &mut [IoSlice<'a>; MAX_SLICES]) -> usize {
        let mut n = 0;
        let mut pos = self.pos;
        for (at, payload) in &self.shared {
            if n + 2 > MAX_SLICES {
                return n;
            }
            if *at > pos {
                slices[n] = IoSlice::new(&self.buf[pos..*at]);
                n += 1;
            }
            slices[n] = IoSlice::new(payload);
            n += 1;
            pos = *at;
        }
        if pos < self.buf.len() && n < MAX_SLICES {
            slices[n] = IoSlice::new(&self.buf[pos..]);
            n += 1;
        }
        n
    }

    /// Marks `n` more octets written.
    fn advance(&mut self, mut n: usize) {
        while n > 0 {
            let next = self.shared.front().map_or(self.buf.len(), |(at, _)| *at);
            let copied = n.min(next - self.pos);
            self.pos += copied;
            n -= copied;
            if n == 0 {
                break;
            }
            let (_, payload) = self
                .shared
                .front_mut()
                .expect("written past the copied octets");
            let sent = n.min(payload.len());
            payload.advance(sent);
            self.shared_len -= sent;
            n -= sent;
            if payload.is_empty() {
                self.shared.pop_front();
            }
        }
        // A payload whose turn has come and is empty would stall `next`.
        while self
            .shared
            .front()
            .is_some_and(|(at, payload)| *at == self.pos && payload.is_empty())
        {
            self.shared.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Accepts at most `limit` octets per call, then would block once
    /// `budget` octets are taken.
    struct Trickle {
        written: Vec<u8>,
        limit: usize,
        budget: usize,
        vectored: usize,
    }

    impl Write for Trickle {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.write_vectored(&[IoSlice::new(buf)])
        }

        fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            self.vectored += usize::from(bufs.len() > 1);
            if self.budget == 0 {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let mut room = self.limit.min(self.budget);
            let mut n = 0;
            for buf in bufs {
                let take = room.min(buf.len());
                self.written.extend_from_slice(&buf[..take]);
                room -= take;
                n += take;
            }
            self.budget -= n;
            Ok(n)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn sample() -> (Output, Vec<u8>) {
        let mut output = Output::default();
        let mut expected = Vec::new();
        for i in 0..5u8 {
            output.buf.extend_from_slice(&[i; 3]);
            expected.extend_from_slice(&[i; 3]);
            let payload: Vec<u8> = (0..20).map(|j| i * 20 + j).collect();
            expected.extend_from_slice(&payload);
            output.push_shared(Bytes::from(payload));
        }
        output.buf.extend_from_slice(b"tail");
        expected.extend_from_slice(b"tail");
        (output, expected)
    }

    #[test]
    fn writes_copied_and_shared_octets_in_order_whatever_the_stream_takes() {
        for limit in [1, 2, 7, 23, 1000] {
            let (mut output, expected) = sample();
            let mut stream = Trickle {
                written: Vec::new(),
                limit,
                budget: usize::MAX,
                vectored: 0,
            };
            assert!(output.write_to(&mut stream).unwrap());
            assert_eq!(stream.written, expected, "limit {limit}");
            assert!(output.is_empty());
            assert!(output.buf.is_empty(), "cleared once written");
        }
    }

    #[test]
    fn resumes_where_a_blocked_write_stopped() {
        let (mut output, expected) = sample();
        let total = output.len();
        let mut stream = Trickle {
            written: Vec::new(),
            limit: 9,
            budget: 31,
            vectored: 0,
        };
        assert!(output.write_to(&mut stream).unwrap());
        assert_eq!(output.len(), total - 31);
        output.buf.extend_from_slice(b"more");
        stream.budget = usize::MAX;
        output.write_to(&mut stream).unwrap();
        let mut all = expected;
        all.extend_from_slice(b"more");
        assert_eq!(stream.written, all);
    }

    #[test]
    fn a_connection_that_never_drains_fully_keeps_only_what_is_unwritten() {
        let mut output = Output::default();
        let mut expected = Vec::new();
        let mut stream = Trickle {
            written: Vec::new(),
            limit: usize::MAX,
            budget: 0,
            vectored: 0,
        };
        for round in 0..1000u32 {
            // Refilled while under a batch, as the engine refills, and
            // drained by less than that each time.
            while output.len() < 4096 {
                let copied = round.to_be_bytes().repeat(64);
                expected.extend_from_slice(&copied);
                output.buf.extend_from_slice(&copied);
                let shared = Bytes::from(vec![round as u8; 300]);
                expected.extend_from_slice(&shared);
                output.push_shared(shared);
            }
            stream.budget = 1000;
            output.write_to(&mut stream).unwrap();
            assert!(output.buf.len() <= 2 * output.len(), "round {round}");
        }
        stream.budget = usize::MAX;
        output.write_to(&mut stream).unwrap();
        assert_eq!(stream.written, expected);
    }

    #[test]
    fn a_shared_payload_added_first_or_empty_is_handled() {
        let mut output = Output::default();
        output.push_shared(Bytes::from_static(b""));
        output.push_shared(Bytes::from_static(b"abc"));
        output.buf.extend_from_slice(b"de");
        output.push_shared(Bytes::from_static(b""));
        let mut stream = Trickle {
            written: Vec::new(),
            limit: 2,
            budget: usize::MAX,
            vectored: 0,
        };
        output.write_to(&mut stream).unwrap();
        assert_eq!(stream.written, b"abcde");
        assert!(output.is_empty());
    }

    #[test]
    fn many_payloads_are_written_a_batch_of_slices_at_a_time() {
        let mut output = Output::default();
        let mut expected = Vec::new();
        for i in 0..200u8 {
            output.buf.push(i);
            output.push_shared(Bytes::from(vec![i; 4]));
            expected.push(i);
            expected.extend_from_slice(&[i; 4]);
        }
        let mut stream = Trickle {
            written: Vec::new(),
            limit: usize::MAX,
            budget: usize::MAX,
            vectored: 0,
        };
        output.write_to(&mut stream).unwrap();
        assert_eq!(stream.written, expected);
        assert!(stream.vectored >= 200 * 2 / MAX_SLICES);
    }
}
