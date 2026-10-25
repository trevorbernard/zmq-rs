//! Z85 (RFC 32): binary data as printable text, four octets to five
//! characters. Used for CURVE keys.

use std::fmt;

/// Why Z85 encoding or decoding failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Z85Error {
    /// Binary input must be a multiple of 4 octets; text a multiple of 5
    /// characters. Z85 never pads.
    Length,
    /// A character outside the Z85 alphabet.
    Character(char),
    /// Five characters whose value exceeds 32 bits.
    Overflow,
}

const ALPHABET: &[u8; 85] =
    b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

/// Encodes `data`, whose length must be a multiple of 4.
pub fn encode(data: &[u8]) -> Result<String, Z85Error> {
    if !data.len().is_multiple_of(4) {
        return Err(Z85Error::Length);
    }
    let mut text = String::with_capacity(data.len() / 4 * 5);
    for group in data.as_chunks::<4>().0 {
        let mut value = u32::from_be_bytes(*group);
        let mut digits = [0u8; 5];
        for digit in digits.iter_mut().rev() {
            *digit = ALPHABET[(value % 85) as usize];
            value /= 85;
        }
        text.extend(digits.iter().map(|&digit| char::from(digit)));
    }
    Ok(text)
}

/// Decodes `text`, whose length must be a multiple of 5.
pub fn decode(text: &str) -> Result<Vec<u8>, Z85Error> {
    let octets = text.as_bytes();
    if !octets.len().is_multiple_of(5) {
        return Err(Z85Error::Length);
    }
    let mut data = Vec::with_capacity(octets.len() / 5 * 4);
    for (start, group) in (0..).step_by(5).zip(octets.as_chunks::<5>().0) {
        let mut value: u64 = 0;
        for (at, &c) in (start..).zip(group) {
            // The alphabet is ASCII, so the first octet outside it starts
            // a character.
            let digit = ALPHABET.iter().position(|&a| a == c).ok_or_else(|| {
                Z85Error::Character(text[at..].chars().next().expect("a character starts here"))
            })?;
            value = value * 85 + digit as u64;
        }
        let value = u32::try_from(value).map_err(|_| Z85Error::Overflow)?;
        data.extend_from_slice(&value.to_be_bytes());
    }
    Ok(data)
}

impl fmt::Display for Z85Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Length => {
                f.write_str("Z85 length must be a multiple of 4 octets or 5 characters")
            }
            Self::Character(c) => write!(f, "{c:?} is not a Z85 character"),
            Self::Overflow => f.write_str("Z85 group exceeds 32 bits"),
        }
    }
}

impl std::error::Error for Z85Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc_32_test_vector() {
        let binary = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
        assert_eq!(encode(&binary).unwrap(), "HelloWorld");
        assert_eq!(decode("HelloWorld").unwrap(), binary);
    }

    #[test]
    fn round_trips_every_octet_value() {
        let data: Vec<u8> = (0..=255).collect();
        assert_eq!(decode(&encode(&data).unwrap()).unwrap(), data);
        assert_eq!(encode(&[]).unwrap(), "");
        assert_eq!(encode(&[0; 4]).unwrap(), "00000");
        assert_eq!(encode(&[0xff; 4]).unwrap(), "%nSc0");
    }

    #[test]
    fn lengths_must_be_whole_groups() {
        assert_eq!(encode(&[1, 2, 3]), Err(Z85Error::Length));
        assert_eq!(decode("Hell"), Err(Z85Error::Length));
    }

    #[test]
    fn rejects_foreign_characters_and_overflow() {
        assert_eq!(decode("Hell~"), Err(Z85Error::Character('~')));
        assert_eq!(decode("Hellé"), Err(Z85Error::Length), "é is two octets");
        assert_eq!(decode("He€"), Err(Z85Error::Character('€')));
        assert_eq!(decode("%nSc1"), Err(Z85Error::Overflow));
        assert_eq!(decode("#####"), Err(Z85Error::Overflow));
    }
}
