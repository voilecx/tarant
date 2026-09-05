//! The `chap-sha1` handshake.
//!
//! Tarantool never sees the password. The greeting carries a per-session
//! salt; the client folds the password and the salt through SHA-1 into a
//! 20-byte scramble, and the server checks it against the hash it stores.

use base64::prelude::{BASE64_STANDARD, Engine};
use sha1::{Digest, Sha1};

use crate::error::{Error, Result};

/// Length of the base64 salt in the greeting's second line.
const SALT_B64_LEN: usize = 44;
/// How many bytes of the decoded salt take part in the scramble.
const SALT_USED_LEN: usize = 20;

/// Name of the mechanism, as sent in `IPROTO_TUPLE[0]` of `IPROTO_AUTH`.
pub(crate) const CHAP_SHA1: &str = "chap-sha1";

/// Compute the `chap-sha1` scramble for `password` under `salt_line`.
///
/// `salt_line` is the second 64-byte line of the greeting, padding included.
pub(crate) fn chap_sha1(salt_line: &[u8], password: &str) -> Result<[u8; 20]> {
    let encoded = salt_line
        .get(..SALT_B64_LEN)
        .ok_or_else(|| Error::protocol("greeting salt line is shorter than 44 bytes"))?;
    let salt = BASE64_STANDARD
        .decode(encoded)
        .map_err(|_| Error::protocol("greeting salt is not valid base64"))?;
    let salt = salt
        .get(..SALT_USED_LEN)
        .ok_or_else(|| Error::protocol("greeting salt decodes to fewer than 20 bytes"))?;

    let step_1 = Sha1::digest(password.as_bytes());
    let step_2 = Sha1::digest(step_1);
    let step_3 = Sha1::new().chain_update(salt).chain_update(step_2).finalize();

    let mut scramble = [0u8; 20];
    for (out, (a, b)) in scramble.iter_mut().zip(step_1.iter().zip(step_3.iter())) {
        *out = a ^ b;
    }
    Ok(scramble)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scramble_is_deterministic_and_password_bound() {
        // A salt line shaped like a real greeting: 44 base64 chars, padded to
        // 64 bytes. Whether the maths matches the server is proven by the
        // integration tests, which authenticate against a live instance.
        let salt = "WPFdTx1HcOeKnfsZ+Qm6GkaMbBeZoi4Ov8yGPBnPuCeNk8Jr0KYhKyYUWq+gtHJn";
        let mut line = [0u8; 64];
        line[..salt.len()].copy_from_slice(salt.as_bytes());
        let scramble = chap_sha1(&line, "tarant").expect("valid salt");
        assert_eq!(scramble, chap_sha1(&line, "tarant").unwrap());
        assert_ne!(scramble, chap_sha1(&line, "other").unwrap());
    }

    #[test]
    fn short_salt_is_a_protocol_error() {
        let err = chap_sha1(b"too short", "x").unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }
}
