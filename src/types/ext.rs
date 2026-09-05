//! The serde side of Tarantool's `MessagePack` extension types.
//!
//! `rmp_serde` and `rmpv` agree on one representation for an extension
//! value: a newtype struct named [`MSGPACK_EXT_STRUCT_NAME`] wrapping the
//! pair `(type tag, payload bytes)`. The value types in this module speak
//! that representation, so a `struct` field typed [`Decimal`](super::Decimal)
//! or [`Datetime`](super::Datetime) goes over the wire as the extension the
//! server expects, with no attributes on the field.

use std::fmt;

use rmp_serde::MSGPACK_EXT_STRUCT_NAME;
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::ser::{SerializeTuple, Serializer};
use serde::{Deserialize, Serialize};

/// Serialise `payload` as the extension `tag`.
pub(super) fn serialize<S: Serializer>(tag: i8, payload: &[u8], s: S) -> Result<S::Ok, S::Error> {
    struct Payload<'a>(&'a [u8]);

    impl Serialize for Payload<'_> {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.serialize_bytes(self.0)
        }
    }

    struct Ext<'a> {
        tag: i8,
        payload: &'a [u8],
    }

    impl Serialize for Ext<'_> {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            let mut pair = s.serialize_tuple(2)?;
            pair.serialize_element(&self.tag)?;
            pair.serialize_element(&Payload(self.payload))?;
            pair.end()
        }
    }

    s.serialize_newtype_struct(MSGPACK_EXT_STRUCT_NAME, &Ext { tag, payload })
}

/// Deserialise any extension as its `(tag, payload)`.
pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<(i8, Vec<u8>), D::Error> {
    d.deserialize_newtype_struct(MSGPACK_EXT_STRUCT_NAME, ExtVisitor)
}

/// Deserialise the extension `tag`; any other tag is an error naming `what`.
pub(super) fn deserialize_tagged<'de, D: Deserializer<'de>>(
    d: D,
    tag: i8,
    what: &'static str,
) -> Result<Vec<u8>, D::Error> {
    let (found, payload) = deserialize(d)?;
    if found == tag {
        Ok(payload)
    } else {
        Err(de::Error::custom(format!(
            "expected {what} (MessagePack extension type {tag}), found extension type {found}"
        )))
    }
}

struct ExtVisitor;

impl<'de> Visitor<'de> for ExtVisitor {
    type Value = (i8, Vec<u8>);

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a MessagePack extension")
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_tuple(2, PairVisitor)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
        PairVisitor.visit_seq(seq)
    }
}

struct PairVisitor;

impl<'de> Visitor<'de> for PairVisitor {
    type Value = (i8, Vec<u8>);

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an extension type tag followed by its payload")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let tag: i8 = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let payload: Payload =
            seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
        Ok((tag, payload.0))
    }
}

/// Owned bytes that accept every way a format may hand them over.
struct Payload(Vec<u8>);

impl<'de> Deserialize<'de> for Payload {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct BytesVisitor;

        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = Payload;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("extension payload bytes")
            }

            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                Ok(Payload(v.to_vec()))
            }

            fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                Ok(Payload(v))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(byte) = seq.next_element::<u8>()? {
                    out.push(byte);
                }
                Ok(Payload(out))
            }
        }

        d.deserialize_byte_buf(BytesVisitor)
    }
}
