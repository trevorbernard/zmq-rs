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
///
/// With the `bytes` feature a frame converts to and from [`bytes::Bytes`],
/// sharing a larger payload's buffer instead of copying it.
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

    /// Copies `data` into a new frame: the way to make one from bytes that
    /// are borrowed, as `From` takes only `'static` ones, which it does not
    /// copy.
    pub fn copy_from_slice(data: &[u8]) -> Self {
        Self::inline(data).unwrap_or_else(|| Self(Repr::Shared(Bytes::copy_from_slice(data))))
    }

    /// The payload.
    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            Repr::Inline { len, buf } => &buf[..usize::from(*len)],
            Repr::Shared(bytes) => bytes,
        }
    }

    /// The payload as `Bytes`: a larger payload's own buffer, not a copy;
    /// an inline one copied.
    pub(crate) fn into_shared(self) -> Bytes {
        match self.0 {
            Repr::Inline { .. } => Bytes::copy_from_slice(self.as_bytes()),
            Repr::Shared(bytes) => bytes,
        }
    }

    /// Wraps `bytes` without copying, unless it is small enough to inline.
    pub(crate) fn from_shared(bytes: Bytes) -> Self {
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
        self.as_bytes()
    }
}

impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl PartialEq for Frame {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for Frame {}

/// A frame equals bytes or text with its payload, so that
/// `assert_eq!(frame, "hello")` and `frame == b"hello"` work.
macro_rules! eq_bytes {
    ($([$($generics:tt)*] $other:ty,)*) => {$(
        impl<$($generics)*> PartialEq<$other> for Frame {
            fn eq(&self, other: &$other) -> bool {
                self.as_bytes() == AsRef::<[u8]>::as_ref(other)
            }
        }
    )*};
}

eq_bytes! {
    [] [u8],
    [] &[u8],
    [] str,
    [] &str,
    [] Vec<u8>,
    [] String,
    [const N: usize] [u8; N],
    [const N: usize] &[u8; N],
}

impl Hash for Frame {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Frame(b\"{}\")", self.as_bytes().escape_ascii())
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

/// Wraps `data` without copying, which is why it must be `'static`. Bytes
/// borrowed for less are copied with [`Frame::copy_from_slice`].
impl From<&'static [u8]> for Frame {
    fn from(data: &'static [u8]) -> Self {
        Self::from_shared(Bytes::from_static(data))
    }
}

/// Wraps `data` without copying, as a `&'static [u8]` is. Text borrowed
/// for less is copied with [`Frame::copy_from_slice`], or sent as a `String`.
impl From<&'static str> for Frame {
    fn from(data: &'static str) -> Self {
        Self::from(data.as_bytes())
    }
}

/// Wraps `bytes` without copying, unless it is small enough to inline.
#[cfg(feature = "bytes")]
impl From<Bytes> for Frame {
    fn from(bytes: Bytes) -> Self {
        Self::from_shared(bytes)
    }
}

/// A larger payload's own buffer, not a copy; an inline one copied.
#[cfg(feature = "bytes")]
impl From<Frame> for Bytes {
    fn from(frame: Frame) -> Self {
        frame.into_shared()
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
    fn a_frame_equals_text_and_bytes_with_its_payload() {
        let frame = Frame::from("hello");
        assert_eq!(frame, "hello");
        assert_eq!(frame, *"hello");
        assert_eq!(frame, b"hello");
        assert_eq!(frame, *b"hello");
        assert_eq!(frame, &b"hello"[..]);
        assert_eq!(frame, b"hello"[..]);
        assert_eq!(frame, b"hello".to_vec());
        assert_eq!(frame, String::from("hello"));
        assert_ne!(frame, "hell");
        assert_ne!(frame, b"hello!");
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
    #[test]
    fn a_frame_is_its_octets_to_what_takes_them() {
        let frame = Frame::from(Box::<[u8]>::from(&b"boxed"[..]));
        assert_eq!(frame.as_bytes(), b"boxed");
        assert_eq!(AsRef::<[u8]>::as_ref(&frame), b"boxed");
        assert!(frame == b"boxed"[..]);
        assert!(frame != b"other"[..]);
    }
}
