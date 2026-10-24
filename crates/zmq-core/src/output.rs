//! What an engine has yet to write: copied octets, and large payloads sent
//! from their own buffers, as libzmq's encoder sends a message too large
//! for its batch straight from the message.

use std::collections::VecDeque;
use std::io::{self, IoSlice, Write};

use bytes::{Buf, Bytes};

/// Payloads at least this large are sent from their own buffers rather
/// than copied, as libzmq's 8 KiB batch.
pub(crate) const SHARED_MIN: usize = 8 * 1024;

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
    /// Queues `payload` after everything added so far.
    pub(crate) fn push_shared(&mut self, payload: Bytes) {
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
        while !self.is_empty() {
            let written = if self.shared.is_empty() {
                stream.write(&self.buf[self.pos..])
            } else {
                stream.write_vectored(&self.slices())
            };
            match written {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => {
                    self.advance(n);
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
        }
        Ok(progressed)
    }

    /// The unwritten output in order, up to [`MAX_SLICES`] slices.
    fn slices(&self) -> Vec<IoSlice<'_>> {
        let mut slices = Vec::with_capacity(MAX_SLICES.min(2 * self.shared.len() + 1));
        let mut pos = self.pos;
        for (at, payload) in &self.shared {
            if slices.len() + 2 > MAX_SLICES {
                return slices;
            }
            if *at > pos {
                slices.push(IoSlice::new(&self.buf[pos..*at]));
            }
            slices.push(IoSlice::new(payload));
            pos = *at;
        }
        if pos < self.buf.len() && slices.len() < MAX_SLICES {
            slices.push(IoSlice::new(&self.buf[pos..]));
        }
        slices
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
