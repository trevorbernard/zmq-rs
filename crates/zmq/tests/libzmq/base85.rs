//! libzmq's `tests/test_base85.cpp`: Z85 (RFC 32), as `zmq_z85_encode`
//! and `zmq_z85_decode`. zmq-rs's codec is `zmtp::z85`; its errors stand
//! for libzmq's `EINVAL`.

use zmtp::z85;

/// RFC 32's test vector.
const DECODED: [u8; 8] = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
const ENCODED: &str = "HelloWorld";

#[test]
fn z85_encode_valid_success() {
    assert_eq!(z85::encode(&DECODED).unwrap(), ENCODED);
}

/// Input whose length is not a multiple of 4 is refused.
#[test]
fn z85_encode_invalid_failure_1() {
    assert!(z85::encode(&[0; 1]).is_err());
}

#[test]
fn z85_encode_invalid_failure_42() {
    assert!(z85::encode(&[0; 42]).is_err());
}

#[test]
fn z85_decode_valid_success() {
    assert_eq!(z85::decode(ENCODED).unwrap(), DECODED);
}

/// Text whose length is not a multiple of 5 is refused.
#[test]
fn z85_decode_invalid_failure_indivisble_by_5_multiple_chars() {
    assert!(z85::decode("01234567").is_err());
}

#[test]
fn z85_decode_invalid_failure_indivisble_by_5_one_char() {
    assert!(z85::decode("0").is_err());
}

/// The largest representable group is beyond 2^32 - 1.
#[test]
fn z85_decode_invalid_failure_max() {
    assert!(z85::decode("#####").is_err());
}

/// "%nSc0" is 0xffffffff; one more overflows.
#[test]
fn z85_decode_invalid_failure_above_limit() {
    assert!(z85::decode("%nSc1").is_err());
}

// The next three are libzmq's C strings as written: an octal escape takes up
// to three digits, so "####\0047" is "####", U+0004 and "7".

#[test]
fn z85_decode_invalid_failure_char_within() {
    assert!(z85::decode("####\u{4}7").is_err());
}

#[test]
fn z85_decode_invalid_failure_char_adjacent_below() {
    assert!(z85::decode("####\u{10}0").is_err());
}

#[test]
fn z85_decode_invalid_failure_char_adjacent_above() {
    assert!(z85::decode("####\u{3}7").is_err());
}

fn encode_then_decode(data: &[u8]) {
    let encoded = z85::encode(data).unwrap();
    assert_eq!(z85::decode(&encoded).unwrap(), data);
}

#[test]
fn z85_encode_z85_decode_roundtrip_min() {
    encode_then_decode(&[0x00; 4]);
}

#[test]
fn z85_encode_z85_decode_roundtrip_max() {
    encode_then_decode(&[0xff; 4]);
}

#[test]
fn z85_decode_z85_encode_roundtrip_regular() {
    let text = "r^/rM9M=rMToK)63O8dCvd9D<PY<7iGlC+{BiSnG";
    let decoded = z85::decode(text).unwrap();
    assert_eq!(z85::encode(&decoded).unwrap(), text);
}
