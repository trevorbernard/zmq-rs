//! A single message part.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use bytes::Bytes;

/// One part of a ZeroMQ message.
///
/// Payloads of up to [`Frame::INLINE_CAPACITY`] bytes are stored inline, so
/// routing ids, delimiters and short topics never allocate and never keep a
/// larger buffer alive. Larger payloads are reference counted: cloning a frame
/// or sending it to many peers never copies the data.
#[derive(Clone)]
pub struct Frame(Repr);

#[derive(Clone)]
enum Repr {
    Inline {
        len: u8,
        buf: [u8; Frame::INLINE_CAPACITY],
    },
    Shared(Bytes),
}

impl Frame {
    /// Payloads up to this many bytes are stored inside the `Frame` itself.
    ///
    /// Chosen so that `Frame` stays 32 bytes on 64-bit targets: the inline
    /// variant fits beside the niche in `Bytes`, so no separate tag is needed.
    pub const INLINE_CAPACITY: usize = 23;

    /// An empty frame.
    pub const fn new() -> Self {
        Self(Repr::Inline {
            len: 0,
            buf: [0; Self::INLINE_CAPACITY],
        })
    }

    /// Copies `data` into a new frame.
    pub fn copy_from_slice(data: &[u8]) -> Self {
        Self::inline(data).unwrap_or_else(|| Self(Repr::Shared(Bytes::copy_from_slice(data))))
    }

    /// The payload.
    pub fn as_slice(&self) -> &[u8] {
        match &self.0 {
            Repr::Inline { len, buf } => &buf[..usize::from(*len)],
            Repr::Shared(bytes) => bytes,
        }
    }

    /// Wraps `bytes` without copying, unless it is small enough to inline.
    fn from_shared(bytes: Bytes) -> Self {
        Self::inline(&bytes).unwrap_or(Self(Repr::Shared(bytes)))
    }

    fn inline(data: &[u8]) -> Option<Self> {
        let len = u8::try_from(data.len()).ok()?;
        let mut buf = [0; Self::INLINE_CAPACITY];
        buf.get_mut(..data.len())?.copy_from_slice(data);
        Some(Self(Repr::Inline { len, buf }))
    }
}

impl Default for Frame {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for Frame {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq for Frame {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for Frame {}

impl PartialEq<[u8]> for Frame {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl Hash for Frame {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Frame(b\"{}\")", self.as_slice().escape_ascii())
    }
}

impl From<Vec<u8>> for Frame {
    fn from(data: Vec<u8>) -> Self {
        Self::from_shared(Bytes::from(data))
    }
}

impl From<Box<[u8]>> for Frame {
    fn from(data: Box<[u8]>) -> Self {
        Self::from_shared(Bytes::from(data))
    }
}

impl From<String> for Frame {
    fn from(data: String) -> Self {
        Self::from_shared(Bytes::from(data))
    }
}

impl From<&'static [u8]> for Frame {
    fn from(data: &'static [u8]) -> Self {
        Self::from_shared(Bytes::from_static(data))
    }
}

impl From<&'static str> for Frame {
    fn from(data: &'static str) -> Self {
        Self::from(data.as_bytes())
    }
}

#[cfg(feature = "bytes")]
impl From<Bytes> for Frame {
    fn from(bytes: Bytes) -> Self {
        Self::from_shared(bytes)
    }
}

#[cfg(feature = "bytes")]
impl From<Frame> for Bytes {
    fn from(frame: Frame) -> Self {
        match frame.0 {
            Repr::Inline { .. } => Bytes::copy_from_slice(frame.as_slice()),
            Repr::Shared(bytes) => bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::{BuildHasher, RandomState};

    const N: usize = Frame::INLINE_CAPACITY;

    fn is_inline(frame: &Frame) -> bool {
        matches!(frame.0, Repr::Inline { .. })
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn frame_is_32_bytes() {
        assert_eq!(size_of::<Frame>(), 32);
    }

    #[test]
    fn frame_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Frame>();
    }

    #[test]
    fn new_frame_is_empty_and_inline() {
        let frame = Frame::new();
        assert!(frame.is_empty());
        assert!(is_inline(&frame));
        assert!(Frame::default().is_empty());
    }

    #[test]
    fn copy_up_to_inline_capacity_is_inline() {
        let data = [7u8; N];
        let frame = Frame::copy_from_slice(&data);
        assert!(is_inline(&frame));
        assert_eq!(&*frame, &data[..]);
    }

    #[test]
    fn copy_over_inline_capacity_is_shared() {
        let data = [7u8; N + 1];
        let frame = Frame::copy_from_slice(&data);
        assert!(!is_inline(&frame));
        assert_eq!(&*frame, &data[..]);
    }

    #[test]
    fn large_vec_is_not_copied() {
        let data = vec![1u8; 1024];
        let ptr = data.as_ptr();
        let frame = Frame::from(data);
        assert_eq!(frame.as_ptr(), ptr);
    }

    #[test]
    fn small_vec_is_inlined() {
        let frame = Frame::from(b"hello".to_vec());
        assert!(is_inline(&frame));
        assert_eq!(&*frame, b"hello");
    }

    #[test]
    fn large_static_is_not_copied() {
        static DATA: [u8; 64] = [3; 64];
        let frame = Frame::from(&DATA[..]);
        assert_eq!(frame.as_ptr(), DATA.as_ptr());
    }

    #[test]
    fn small_static_is_inlined() {
        let frame = Frame::from(&b"topic"[..]);
        assert!(is_inline(&frame));
        assert_eq!(&*frame, b"topic");
    }

    #[test]
    fn strings_convert() {
        assert_eq!(&*Frame::from("hi"), b"hi");
        assert_eq!(&*Frame::from(String::from("hi")), b"hi");
    }

    #[test]
    fn clone_of_shared_frame_shares_the_buffer() {
        let frame = Frame::from(vec![9u8; 1024]);
        let copy = frame.clone();
        assert_eq!(copy.as_ptr(), frame.as_ptr());
    }

    #[test]
    fn clone_of_inline_frame_is_equal() {
        let frame = Frame::copy_from_slice(b"abc");
        assert_eq!(frame.clone(), frame);
    }

    #[test]
    fn equality_and_hash_follow_contents() {
        let a = Frame::copy_from_slice(&[5u8; 100]);
        let b = Frame::from(vec![5u8; 100]);
        assert_eq!(a, b);
        assert_ne!(a, Frame::copy_from_slice(&[5u8; 99]));
        let hasher = RandomState::new();
        assert_eq!(hasher.hash_one(&a), hasher.hash_one(&b));
        assert_eq!(a, [5u8; 100][..]);
    }

    #[test]
    fn debug_shows_escaped_contents() {
        let frame = Frame::copy_from_slice(b"hi\x00");
        assert_eq!(format!("{frame:?}"), r#"Frame(b"hi\x00")"#);
    }

    #[cfg(feature = "bytes")]
    mod bytes_feature {
        use super::*;

        #[test]
        fn large_bytes_is_not_copied() {
            let bytes = Bytes::from(vec![1u8; 1024]);
            let ptr = bytes.as_ptr();
            assert_eq!(Frame::from(bytes).as_ptr(), ptr);
        }

        #[test]
        fn small_bytes_is_inlined() {
            let frame = Frame::from(Bytes::from_static(b"abc"));
            assert!(is_inline(&frame));
        }

        #[test]
        fn shared_frame_into_bytes_is_not_copied() {
            let frame = Frame::from(vec![1u8; 1024]);
            let ptr = frame.as_ptr();
            assert_eq!(Bytes::from(frame).as_ptr(), ptr);
        }

        #[test]
        fn inline_frame_into_bytes_keeps_contents() {
            let frame = Frame::copy_from_slice(b"abc");
            assert_eq!(Bytes::from(frame), &b"abc"[..]);
        }
    }
}
