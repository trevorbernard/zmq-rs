//! Shared drivers for the fuzz targets.
//!
//! Wire targets take the raw octets a peer sends after connecting, the same
//! input shape as libzmq's `test_bind_*_fuzzer` harnesses, so their seed
//! corpora and `zmtp.dict` apply unchanged.

use zmtp::{Decoder, DecoderConfig, Greeting, GreetingConfig, Incoming, PeerGreeting};

/// libzmq's harnesses cap ZMQ_MAXMSGSIZE at 64 MiB to keep the sanitizer
/// allocator happy; so do we.
pub const MAX_FRAME_SIZE: u64 = 64 * 1024 * 1024;

pub fn decoder_config() -> DecoderConfig {
    DecoderConfig {
        max_frame_size: Some(MAX_FRAME_SIZE),
    }
}

/// Runs version negotiation over `data`, returning the peer's greeting and a
/// decoder holding every octet after it, as the engine sets one up.
pub fn negotiate(config: GreetingConfig, data: &[u8]) -> Option<(PeerGreeting, Decoder)> {
    let mut greeting = Greeting::new(config);
    let feed = greeting.feed(data).ok()?;
    let peer = feed.peer?;
    let mut decoder = Decoder::new(peer.version(), decoder_config());
    if let PeerGreeting::Zmtp1Unversioned { replay } = &peer {
        decoder.extend_from_slice(replay);
    }
    decoder.extend_from_slice(&data[feed.consumed..]);
    Some((peer, decoder))
}

/// Decodes until the decoder needs more input or fails.
pub fn drain(decoder: &mut Decoder) -> Vec<Incoming> {
    let mut items = Vec::new();
    while let Ok(Some(item)) = decoder.decode() {
        items.push(item);
    }
    items
}
