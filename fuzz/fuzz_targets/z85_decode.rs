#![no_main]

use libfuzzer_sys::fuzz_target;
use zmtp::z85;

// Z85 has one encoding per value, so anything accepted must re-encode to
// the same text.
fuzz_target!(|text: &str| {
    if let Ok(data) = z85::decode(text) {
        assert_eq!(
            z85::encode(&data).expect("decoded length is a multiple of 4"),
            text
        );
    }
});
