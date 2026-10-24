//! CURVE long-term keys (RFC 26): Curve25519 key pairs, written in Z85.

use std::fmt;

use zeroize::Zeroizing;

use crate::z85::{self, Z85Error};

/// A CURVE public key. Shared freely: a server's is given to its clients.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PublicKey([u8; 32]);

/// A CURVE secret key. Zeroized when dropped, never printed, and not
/// `Clone`, so it exists in one place.
pub struct SecretKey(Zeroizing<[u8; 32]>);

/// A long-term key pair. Only generated or derived from its secret key, so
/// the two halves always belong together.
#[derive(Debug)]
pub struct Keypair {
    public: PublicKey,
    secret: SecretKey,
}

/// Why a key could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyError {
    /// Not 40 Z85 characters, or not 32 octets.
    Length,
    /// Not valid Z85.
    Z85(Z85Error),
}

impl PublicKey {
    /// The key from its 32 octets.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The key from its 40-character Z85 form.
    pub fn from_z85(text: &str) -> Result<Self, KeyError> {
        decode_key(text).map(|bytes| Self(*bytes))
    }

    /// The 32 octets.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The 40-character Z85 form.
    pub fn to_z85(&self) -> String {
        z85::encode(&self.0).expect("32 octets are whole Z85 groups")
    }
}

impl SecretKey {
    /// The key from its 32 octets.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// The key from its 40-character Z85 form.
    pub fn from_z85(text: &str) -> Result<Self, KeyError> {
        decode_key(text).map(Self)
    }

    /// The 32 octets. Keep them secret.
    pub fn expose(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Keypair {
    /// A fresh key pair from the operating system's random source.
    pub fn generate() -> Self {
        let (public, secret) = dryoc::classic::crypto_box::crypto_box_keypair();
        Self {
            public: PublicKey(public),
            secret: SecretKey::from_bytes(secret),
        }
    }

    /// The key pair whose secret key is `secret`.
    pub fn from_secret(secret: SecretKey) -> Self {
        let mut public = [0; 32];
        dryoc::classic::crypto_core::crypto_scalarmult_base(&mut public, secret.expose());
        Self {
            public: PublicKey(public),
            secret,
        }
    }

    /// The public half.
    pub fn public(&self) -> PublicKey {
        self.public
    }

    /// The secret half.
    pub fn secret(&self) -> &SecretKey {
        &self.secret
    }
}

/// Decodes a 40-character Z85 key into its 32 octets.
fn decode_key(text: &str) -> Result<Zeroizing<[u8; 32]>, KeyError> {
    if text.len() != 40 {
        return Err(KeyError::Length);
    }
    let decoded = Zeroizing::new(z85::decode(text).map_err(KeyError::Z85)?);
    let mut key = Zeroizing::new([0u8; 32]);
    key.copy_from_slice(&decoded);
    Ok(key)
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PublicKey").field(&self.to_z85()).finish()
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_z85())
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretKey(..)")
    }
}

impl fmt::Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Length => f.write_str("a CURVE key is 32 octets, or 40 Z85 characters"),
            Self::Z85(err) => write!(f, "invalid CURVE key: {err}"),
        }
    }
}

impl std::error::Error for KeyError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(text: &str) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).unwrap();
        }
        bytes
    }

    // The test key pairs from libzmq's zmq_curve(7).
    const CLIENT_PUBLIC: &str = "Yne@$w-vo<fVvi]a<NY6T1ed:M$fCG*[IaLV{hID";
    const CLIENT_SECRET: &str = "D:)Q[IlAW!ahhC2ac:9*A}h:p?([4%wOTJ%JR%cs";
    const SERVER_PUBLIC: &str = "rq:rM>}U?@Lns47E1%kR.o@n%FcmmsL/@{H8]yf7";
    const SERVER_SECRET: &str = "JTKVSB%%)wK0E.X)V>+}o?pNmC{O&4W4b!Ni{Lh6";

    #[test]
    fn z85_keys_match_their_hex() {
        assert_eq!(
            PublicKey::from_z85(CLIENT_PUBLIC).unwrap().as_bytes(),
            &hex("BB88471D65E2659B30C55A5321CEBB5AAB2B70A398645C26DCA2B2FCB43FC518")
        );
        assert_eq!(
            SecretKey::from_z85(SERVER_SECRET).unwrap().expose(),
            &hex("8E0BDD697628B91D8F245587EE95C5B04D48963F79259877B49CD9063AEAD3B7")
        );
        assert_eq!(
            PublicKey::from_z85(SERVER_PUBLIC).unwrap().to_z85(),
            SERVER_PUBLIC
        );
    }

    #[test]
    fn public_keys_derive_from_secret_keys() {
        for (secret, public) in [
            (CLIENT_SECRET, CLIENT_PUBLIC),
            (SERVER_SECRET, SERVER_PUBLIC),
        ] {
            let pair = Keypair::from_secret(SecretKey::from_z85(secret).unwrap());
            assert_eq!(pair.public(), PublicKey::from_z85(public).unwrap());
        }
    }

    #[test]
    fn generated_pairs_are_consistent_and_distinct() {
        let pair = Keypair::generate();
        let again = Keypair::from_secret(SecretKey::from_bytes(*pair.secret().expose()));
        assert_eq!(pair.public(), again.public());
        assert_ne!(pair.public(), Keypair::generate().public());
    }

    #[test]
    fn malformed_keys_are_refused() {
        assert_eq!(PublicKey::from_z85("short"), Err(KeyError::Length));
        assert_eq!(
            PublicKey::from_z85(&CLIENT_PUBLIC.replace('Y', "~")),
            Err(KeyError::Z85(Z85Error::Character('~')))
        );
        assert!(matches!(SecretKey::from_z85(""), Err(KeyError::Length)));
    }

    #[test]
    fn secret_keys_are_never_printed() {
        let secret = SecretKey::from_z85(CLIENT_SECRET).unwrap();
        assert_eq!(format!("{secret:?}"), "SecretKey(..)");
        let pair = Keypair::from_secret(secret);
        assert!(!format!("{pair:?}").contains(CLIENT_SECRET));
        assert!(format!("{pair:?}").contains(CLIENT_PUBLIC));
    }
}
