//! Inputs from libzmq security advisories, where the bug class can apply to
//! this implementation.

use zmtp::{
    DecodeError, Decoder, DecoderConfig, Frame, Greeting, GreetingConfig, Identity, Incoming,
    LegacyPeers, Mechanism, Message, PeerGreeting, SocketType,
};

/// Runs greeting and decoding over `input` delivered `chunk` octets at a
/// time, as the engine does: the peer's greeting, what decoded, and the
/// error that stopped decoding.
fn receive(
    input: &[u8],
    chunk: usize,
) -> (Option<PeerGreeting>, Vec<Incoming>, Option<DecodeError>) {
    let mut greeting = Greeting::new(GreetingConfig {
        socket_type: SocketType::Pub,
        identity: Identity::new(Vec::new()).unwrap(),
        mechanism: Mechanism::Null {
            legacy_peers: LegacyPeers::Allow,
        },
    });
    let mut peer = None;
    let mut decoder: Option<Decoder> = None;
    let mut decoded = Vec::new();
    for piece in input.chunks(chunk) {
        let decoder = match &mut decoder {
            Some(decoder) => {
                decoder.extend_from_slice(piece);
                decoder
            }
            None => {
                let feed = greeting.feed(piece).expect("greeting accepted");
                let Some(greeted) = feed.peer else { continue };
                let mut fresh = Decoder::new(greeted.version(), DecoderConfig::default());
                if let PeerGreeting::Zmtp1Unversioned { replay } = &greeted {
                    fresh.extend_from_slice(replay);
                }
                fresh.extend_from_slice(&piece[feed.consumed..]);
                peer = Some(greeted);
                decoder.insert(fresh)
            }
        };
        loop {
            match decoder.decode() {
                Ok(Some(item)) => decoded.push(item),
                Ok(None) => break,
                Err(err) => return (peer, decoded, Some(err)),
            }
        }
    }
    (peer, decoded, None)
}

/// GHSA-fc3w-qxf5-7hp6: a ZMTP 1.0 peer whose identity frame uses the long
/// length form overflowed libzmq's heap when read in small batches
/// (`ZMQ_IN_BATCH_SIZE` 4). The greeting's octets must be replayed as the
/// start of that frame however the input is split.
#[test]
fn zmtp1_long_identity_frame_in_small_reads() {
    let input =
        b"\xff\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
    let (peer, decoded, error) = receive(input, input.len());
    assert!(
        matches!(peer, Some(PeerGreeting::Zmtp1Unversioned { .. })),
        "{peer:?}"
    );
    let identity = Message::new(Frame::copy_from_slice(&[0; 7]));
    assert_eq!(decoded, [Incoming::Message(identity)]);
    assert_eq!(error, Some(DecodeError::ZeroLength));
    for chunk in 1..input.len() {
        assert_eq!(
            receive(input, chunk),
            (peer.clone(), decoded.clone(), error.clone()),
            "read {chunk} octets at a time"
        );
    }
}
