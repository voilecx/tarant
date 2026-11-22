//! Tarantool's `uuid` field type.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::ext;
use crate::iproto::ext::UUID;

/// A 128-bit UUID, sent as the `MP_UUID` extension.
///
/// This is what a `uuid` field must hold: a plain 16-byte binary or a string
/// would be rejected by the server. With the `uuid` feature it converts to
/// and from a `uuid::Uuid` in both directions.
///
/// ```
/// use tarant::Uuid;
///
/// let id: Uuid = "f6423bdf-b49e-4913-b361-0740c9702e4b".parse().unwrap();
/// assert_eq!(id.to_string(), "f6423bdf-b49e-4913-b361-0740c9702e4b");
/// assert!(!id.is_nil());
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Uuid([u8; 16]);

impl Uuid {
    /// The all-zero UUID.
    pub const NIL: Self = Self([0; 16]);

    /// A UUID from its 16 bytes, in the order they are written.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// The 16 bytes, in the order they are written.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// The 16 bytes, by value.
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    /// Whether this is [`NIL`](Self::NIL).
    pub fn is_nil(&self) -> bool {
        self.0 == [0; 16]
    }
}

impl fmt::Display for Uuid {
    /// Lowercase, hyphenated: `8-4-4-4-12` hex digits.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = [0u8; 36];
        let mut pos = 0;
        for (i, byte) in self.0.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                out[pos] = b'-';
                pos += 1;
            }
            out[pos] = HEX[usize::from(byte >> 4)];
            out[pos + 1] = HEX[usize::from(byte & 0x0f)];
            pos += 2;
        }
        f.pad(std::str::from_utf8(&out).expect("hex digits and hyphens"))
    }
}

const HEX: &[u8; 16] = b"0123456789abcdef";

impl fmt::Debug for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Uuid({self})")
    }
}

impl FromStr for Uuid {
    type Err = UuidError;

    /// Accepts the hyphenated form and the bare 32 hex digits, any case.
    ///
    /// In the hyphenated form the hyphens sit at byte offsets 8, 13, 18 and
    /// 23 and nowhere else; the remaining 32 bytes must all be hex digits.
    fn from_str(s: &str) -> Result<Self, UuidError> {
        let mut hex = [0u8; 32];
        match s.len() {
            36 => {
                let bytes = s.as_bytes();
                if !matches!((bytes[8], bytes[13], bytes[18], bytes[23]), (b'-', b'-', b'-', b'-'))
                {
                    return Err(UuidError);
                }
                let mut out = 0;
                for (i, &byte) in bytes.iter().enumerate() {
                    if matches!(i, 8 | 13 | 18 | 23) {
                        continue;
                    }
                    hex[out] = byte;
                    out += 1;
                }
            }
            32 => hex.copy_from_slice(s.as_bytes()),
            _ => return Err(UuidError),
        }
        let mut bytes = [0u8; 16];
        for (i, pair) in hex.chunks_exact(2).enumerate() {
            let hi = nibble(pair[0]).ok_or(UuidError)?;
            let lo = nibble(pair[1]).ok_or(UuidError)?;
            bytes[i] = (hi << 4) | lo;
        }
        Ok(Self(bytes))
    }
}

const fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

impl From<[u8; 16]> for Uuid {
    fn from(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl From<Uuid> for [u8; 16] {
    fn from(uuid: Uuid) -> Self {
        uuid.0
    }
}

impl Serialize for Uuid {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        ext::serialize(UUID, &self.0, s)
    }
}

impl<'de> Deserialize<'de> for Uuid {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let payload = ext::deserialize_tagged(d, UUID, "a uuid")?;
        <[u8; 16]>::try_from(payload.as_slice())
            .map(Self)
            .map_err(|_| serde::de::Error::custom("MP_UUID payload is not 16 bytes"))
    }
}

impl From<Uuid> for rmpv::Value {
    fn from(uuid: Uuid) -> Self {
        Self::Ext(UUID, uuid.0.to_vec())
    }
}

impl TryFrom<&rmpv::Value> for Uuid {
    type Error = UuidError;

    fn try_from(value: &rmpv::Value) -> Result<Self, UuidError> {
        match value {
            rmpv::Value::Ext(UUID, payload) => {
                <[u8; 16]>::try_from(payload.as_slice()).map(Self).map_err(|_| UuidError)
            }
            _ => Err(UuidError),
        }
    }
}

#[cfg(feature = "uuid")]
impl From<uuid::Uuid> for Uuid {
    fn from(value: uuid::Uuid) -> Self {
        Self(value.into_bytes())
    }
}

#[cfg(feature = "uuid")]
impl From<Uuid> for uuid::Uuid {
    fn from(value: Uuid) -> Self {
        Self::from_bytes(value.0)
    }
}

/// The text or payload was not a UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UuidError;

impl fmt::Display for UuidError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid uuid")
    }
}

impl std::error::Error for UuidError {}

#[cfg(test)]
mod tests {
    use super::*;

    const TEXT: &str = "f6423bdf-b49e-4913-b361-0740c9702e4b";

    #[test]
    fn matches_the_documented_encoding() {
        let id: Uuid = TEXT.parse().unwrap();
        let expected = [
            0xd8, 0x02, 0xf6, 0x42, 0x3b, 0xdf, 0xb4, 0x9e, 0x49, 0x13, 0xb3, 0x61, 0x07, 0x40,
            0xc9, 0x70, 0x2e, 0x4b,
        ];
        assert_eq!(rmp_serde::to_vec(&id).unwrap(), expected);
        let back: Uuid = rmp_serde::from_slice(&expected).unwrap();
        assert_eq!(back, id);
        assert_eq!(back.to_string(), TEXT);
    }

    #[test]
    fn parses_both_forms_and_rejects_the_rest() {
        let hyphenated: Uuid = TEXT.parse().unwrap();
        let bare: Uuid = TEXT.replace('-', "").to_uppercase().parse().unwrap();
        assert_eq!(hyphenated, bare);
        assert!("not-a-uuid".parse::<Uuid>().is_err());
        assert!("f6423bdf-b49e-4913-b361-0740c9702e4".parse::<Uuid>().is_err());
        assert!("g6423bdfb49e4913b3610740c9702e4b".parse::<Uuid>().is_err());
        // A hyphen in the wrong place is an error, not a panic.
        assert!("f6423bdf-b49e-4913-b361-0740c9702e-b".parse::<Uuid>().is_err());
        assert!("f6423bdf-b49e-4913-b361-0740c9702-4-".parse::<Uuid>().is_err());
        // 36 bytes with a multi-byte character: the bytes are not hex.
        let utf8 = "f6423bdf-b49e-4913-b361-0740c970\u{e9}4b";
        assert_eq!(utf8.len(), 36);
        assert!(utf8.parse::<Uuid>().is_err());
        assert!(Uuid::NIL.is_nil());
        assert_eq!(Uuid::NIL.to_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn debug_shows_the_text() {
        let id: Uuid = TEXT.parse().unwrap();
        assert_eq!(format!("{id:?}"), format!("Uuid({TEXT})"));
    }
}
