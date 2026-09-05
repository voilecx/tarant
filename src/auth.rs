//! The `chap-sha1` handshake.
//!
//! Tarantool never sees the password. The greeting carries a per-session
//! salt; the client folds the password and the salt through SHA-1 into a
//! 20-byte scramble, and the server checks it against the hash it stores.

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
    let salt = decode_base64(encoded)
        .ok_or_else(|| Error::protocol("greeting salt is not valid base64"))?;
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

/// Decode standard base64 (RFC 4648, `+/` alphabet, optional `=` padding).
///
/// Just enough to turn a greeting salt into bytes: any symbol outside the
/// alphabet fails the decode, which the caller reports as a protocol error.
/// The shift-and-mask arithmetic keeps every intermediate within a `u8`.
fn decode_base64(input: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    for chunk in input.chunks(4) {
        let mut sextet = [0u8; 4];
        let mut symbols = 0usize;
        for (slot, &c) in sextet.iter_mut().zip(chunk) {
            if c == b'=' {
                break;
            }
            *slot = decode_symbol(c)?;
            symbols += 1;
        }
        match symbols {
            0 => break,
            1 => return None, // a base64 group is never a single symbol
            2 => out.push((sextet[0] << 2) | (sextet[1] >> 4)),
            3 => {
                out.push((sextet[0] << 2) | (sextet[1] >> 4));
                out.push((sextet[1] << 4) | (sextet[2] >> 2));
            }
            _ => {
                out.push((sextet[0] << 2) | (sextet[1] >> 4));
                out.push((sextet[1] << 4) | (sextet[2] >> 2));
                out.push((sextet[2] << 6) | sextet[3]);
            }
        }
    }
    Some(out)
}

/// Map one base64 symbol to its 6-bit value, or `None` if it is not one.
const fn decode_symbol(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
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

    #[test]
    fn base64_matches_the_reference_alphabet() {
        // Vectors from RFC 4648, exercising every padding length.
        assert_eq!(decode_base64(b"").unwrap(), b"");
        assert_eq!(decode_base64(b"Zg==").unwrap(), b"f");
        assert_eq!(decode_base64(b"Zm8=").unwrap(), b"fo");
        assert_eq!(decode_base64(b"Zm9v").unwrap(), b"foo");
        assert_eq!(decode_base64(b"Zm9vYmFy").unwrap(), b"foobar");
        // The `+` and `/` symbols and the full byte range.
        assert_eq!(decode_base64(b"/+8=").unwrap(), [0xff, 0xef]);
        assert!(decode_base64(b"not base64!").is_none());
    }
}
