//! Properties of the frame codec over generated traffic: whatever messages
//! and commands are encoded come back out of the decoder, however the input
//! is split into reads, and limits and errors fall where the input puts
//! them.

use proptest::prelude::*;
use zmtp::{DecodeError, Decoder, DecoderConfig, Encoder, Frame, Incoming, Message, Version};

const VERSIONS: [Version; 4] = [Version::V1, Version::V2, Version::V3_0, Version::V3_1];

/// A frame body: its length, chosen to straddle every size class the codecs
/// switch on and the decoder's buffer sizes, and a seed for its contents.
fn body() -> impl Strategy<Value = Vec<u8>> {
    let len = prop_oneof![
        4 => 0usize..64,
        // Short/long switches: 253 and 254 for ZMTP 1.0, 255 and 256 after.
        3 => prop::sample::select(vec![0usize, 1, 252, 253, 254, 255, 256, 257]),
        2 => 64usize..9_000,
        // Past the 8 KiB read size and the 64 KiB read cap.
        1 => 60_000usize..140_000,
    ];
    (len, any::<u8>()).prop_map(|(len, seed)| {
        (0..len)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
            .collect()
    })
}

#[derive(Debug, Clone)]
enum Item {
    Message(Vec<Vec<u8>>),
    Command(Vec<u8>),
}

impl Item {
    fn encode(&self, encoder: &Encoder, out: &mut Vec<u8>) {
        match self {
            Self::Message(frames) => encoder.encode(&message(frames), out),
            Self::Command(body) => encoder
                .encode_command(body, out)
                .expect("commands are only generated for ZMTP 3.x"),
        }
    }

    fn incoming(&self) -> Incoming {
        match self {
            Self::Message(frames) => Incoming::Message(message(frames)),
            Self::Command(body) => Incoming::Command(Frame::copy_from_slice(body)),
        }
    }

    fn frames(&self) -> Vec<&[u8]> {
        match self {
            Self::Message(frames) => frames.iter().map(Vec::as_slice).collect(),
            Self::Command(body) => vec![body.as_slice()],
        }
    }
}

fn message(frames: &[Vec<u8>]) -> Message {
    Message::try_from(
        frames
            .iter()
            .map(|f| Frame::copy_from_slice(f))
            .collect::<Vec<_>>(),
    )
    .expect("at least one frame")
}

fn item(version: Version) -> BoxedStrategy<Item> {
    let message = prop::collection::vec(body(), 1..4).prop_map(Item::Message);
    match version {
        Version::V1 | Version::V2 => message.boxed(),
        Version::V3_0 | Version::V3_1 => {
            prop_oneof![4 => message, 1 => body().prop_map(Item::Command)].boxed()
        }
    }
}

/// A version and some traffic for it.
fn traffic() -> impl Strategy<Value = (Version, Vec<Item>)> {
    prop::sample::select(VERSIONS.to_vec())
        .prop_flat_map(|version| (Just(version), prop::collection::vec(item(version), 1..6)))
}

/// How a stream arrives: an optional prefix copied in first, as a ZMTP 1.0
/// greeting's replay is, then reads of these sizes, cycled.
#[derive(Debug, Clone)]
struct Arrival {
    prefix: usize,
    reads: Vec<usize>,
}

fn arrival() -> impl Strategy<Value = Arrival> {
    let read = prop_oneof![
        3 => 1usize..16,
        2 => 16usize..2_000,
        1 => 2_000usize..100_000,
    ];
    (
        prop_oneof![3 => Just(0usize), 1 => 1usize..300],
        prop::collection::vec(read, 1..16),
    )
        .prop_map(|(prefix, reads)| Arrival { prefix, reads })
}

/// Everything the decoder produced from `input`, up to and including the
/// first error, which must then repeat.
fn decode(
    version: Version,
    config: DecoderConfig,
    input: &[u8],
    arrival: &Arrival,
) -> (Vec<Incoming>, Option<DecodeError>) {
    let mut decoder = Decoder::new(version, config);
    let mut decoded = Vec::new();
    let prefix = arrival.prefix.min(input.len());
    decoder.extend_from_slice(&input[..prefix]);
    let mut rest = &input[prefix..];
    let mut reads = arrival.reads.iter().cycle();
    loop {
        loop {
            match decoder.decode() {
                Ok(Some(item)) => decoded.push(item),
                Ok(None) => break,
                Err(err) => {
                    assert_eq!(decoder.decode(), Err(err.clone()), "errors are sticky");
                    return (decoded, Some(err));
                }
            }
        }
        if rest.is_empty() {
            return (decoded, None);
        }
        let wanted = *reads.next().expect("cycled");
        let n = decoder
            .fill(|buf| {
                let n = wanted.min(buf.len()).min(rest.len());
                buf[..n].copy_from_slice(&rest[..n]);
                // A reader may scribble past what it reports.
                buf[n..].fill(0xEE);
                Ok::<_, ()>(n)
            })
            .expect("reads never fail");
        assert!(n > 0, "the decoder always offers space");
        rest = &rest[n..];
    }
}

fn encode(version: Version, items: &[Item]) -> Vec<u8> {
    let encoder = Encoder::new(version);
    let mut out = Vec::new();
    for item in items {
        item.encode(&encoder, &mut out);
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Whatever is encoded is decoded, in order, however it arrives.
    #[test]
    fn round_trips_however_the_input_is_split(
        (version, items) in traffic(),
        arrival in arrival(),
    ) {
        let input = encode(version, &items);
        let (decoded, error) = decode(version, DecoderConfig::default(), &input, &arrival);
        prop_assert_eq!(error, None);
        let expected: Vec<Incoming> = items.iter().map(Item::incoming).collect();
        prop_assert_eq!(decoded, expected);
    }

    /// Decoding stops at the first frame over the limit, before its body
    /// arrives, having delivered every whole message before it.
    #[test]
    fn stops_at_the_first_frame_over_the_limit(
        (version, items) in traffic(),
        arrival in arrival(),
        max in prop_oneof![Just(0u64), Just(255), Just(256), 0u64..10_000],
    ) {
        let config = DecoderConfig { max_frame_size: Some(max) };
        let input = encode(version, &items);
        let (decoded, error) = decode(version, config, &input, &arrival);
        let first_over = items.iter().position(|item| {
            item.frames().iter().any(|f| f.len() as u64 > max)
        });
        let whole = first_over.unwrap_or(items.len());
        let expected: Vec<Incoming> = items[..whole].iter().map(Item::incoming).collect();
        prop_assert_eq!(decoded, expected);
        let expected_error = first_over.map(|i| {
            let size = items[i].frames().into_iter().find(|f| f.len() as u64 > max).map(<[u8]>::len);
            DecodeError::FrameTooLarge { size: size.expect("over the limit") as u64 }
        });
        prop_assert_eq!(error, expected_error);
    }

    /// On any input, valid or not, the result depends only on the octets,
    /// never on how they were split into reads; and nothing panics.
    #[test]
    fn corrupted_input_decodes_the_same_however_it_is_split(
        (version, items) in traffic(),
        corruption in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 0..8),
        truncate in any::<prop::sample::Index>(),
        max in prop::option::of(0u64..100_000),
        arrival in arrival(),
    ) {
        let mut input = encode(version, &items);
        for (at, octet) in corruption {
            let at = at.index(input.len());
            input[at] = octet;
        }
        input.truncate(truncate.index(input.len() + 1));
        let config = DecoderConfig { max_frame_size: max };
        let whole = Arrival { prefix: input.len(), reads: vec![1] };
        prop_assert_eq!(
            decode(version, config, &input, &arrival),
            decode(version, config, &input, &whole)
        );
    }
}
