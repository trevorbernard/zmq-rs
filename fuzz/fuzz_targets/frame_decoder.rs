#![no_main]

use libfuzzer_sys::fuzz_target;
use zmtp::{Decoder, Version};

// The first octet picks the framing, the rest is the peer's traffic.
fuzz_target!(|data: &[u8]| {
    let Some((&selector, traffic)) = data.split_first() else {
        return;
    };
    let version =
        [Version::V1, Version::V2, Version::V3_0, Version::V3_1][usize::from(selector % 4)];
    let mut decoder = Decoder::new(version, zmtp_fuzz::decoder_config());
    decoder.extend_from_slice(traffic);
    zmtp_fuzz::drain(&mut decoder);
});
