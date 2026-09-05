//! Small, allocation-free helpers over raw `MessagePack`.
//!
//! The codec needs to walk a response body without materialising it: find
//! `IPROTO_DATA`, remember where it starts and ends, and hand that exact
//! slice to serde later. That takes one primitive — *how long is the value
//! at this offset?* — which is what [`value_len`] answers.

use crate::error::{Error, Result};

/// Byte length of the `MessagePack` value that starts at `buf[0]`.
///
/// Fails if the buffer ends inside the value or begins with the reserved
/// `0xc1` marker. Nested containers are walked iteratively, so depth is not
/// bounded by the stack.
pub(crate) fn value_len(buf: &[u8]) -> Result<usize> {
    let mut pos = 0usize;
    // Number of values still to consume; one for the root, plus every child
    // a container announces.
    let mut pending = 1usize;

    while pending > 0 {
        pending -= 1;
        let marker = *buf.get(pos).ok_or_else(truncated)?;
        pos += 1;

        let (payload, children) = match marker {
            0x00..=0x7f | 0xc0 | 0xc2 | 0xc3 | 0xe0..=0xff => (0, 0),
            0x80..=0x8f => (0, 2 * usize::from(marker & 0x0f)),
            0x90..=0x9f => (0, usize::from(marker & 0x0f)),
            0xa0..=0xbf => (usize::from(marker & 0x1f), 0),
            0xc1 => return Err(Error::protocol("reserved MessagePack marker 0xc1")),
            0xc4 | 0xd9 => (read_len(buf, pos, 1)?, 0),
            0xc5 | 0xda => (read_len(buf, pos, 2)?, 0),
            0xc6 | 0xdb => (read_len(buf, pos, 4)?, 0),
            // ext: length prefix, then one type byte, then the payload
            0xc7 => (1 + read_len(buf, pos, 1)?, 0),
            0xc8 => (1 + read_len(buf, pos, 2)?, 0),
            0xc9 => (1 + read_len(buf, pos, 4)?, 0),
            0xca | 0xcc | 0xd0 => (if marker == 0xca { 4 } else { 1 }, 0),
            0xcb | 0xcd | 0xd1 => (if marker == 0xcb { 8 } else { 2 }, 0),
            0xce | 0xd2 => (4, 0),
            0xcf | 0xd3 => (8, 0),
            0xd4 => (2, 0),
            0xd5 => (3, 0),
            0xd6 => (5, 0),
            0xd7 => (9, 0),
            0xd8 => (17, 0),
            0xdc => (0, read_len(buf, pos, 2)?),
            0xdd => (0, read_len(buf, pos, 4)?),
            0xde => (0, 2 * read_len(buf, pos, 2)?),
            0xdf => (0, 2 * read_len(buf, pos, 4)?),
        };

        // Skip the length prefix the match consumed logically but not positionally.
        pos += length_prefix_width(marker);
        pos = pos.checked_add(payload).ok_or_else(truncated)?;
        if pos > buf.len() {
            return Err(truncated());
        }
        pending = pending.checked_add(children).ok_or_else(truncated)?;
    }
    Ok(pos)
}

/// Width of the explicit length prefix following `marker`, if any.
const fn length_prefix_width(marker: u8) -> usize {
    match marker {
        0xc4 | 0xc7 | 0xd9 => 1,
        0xc5 | 0xc8 | 0xda | 0xdc | 0xde => 2,
        0xc6 | 0xc9 | 0xdb | 0xdd | 0xdf => 4,
        _ => 0,
    }
}

fn read_len(buf: &[u8], pos: usize, width: usize) -> Result<usize> {
    let bytes = buf.get(pos..pos + width).ok_or_else(truncated)?;
    let mut n = 0usize;
    for b in bytes {
        n = (n << 8) | usize::from(*b);
    }
    Ok(n)
}

fn truncated() -> Error {
    Error::protocol("MessagePack value is truncated")
}

/// A cursor over a `MessagePack` map, yielding `(key, value bytes)` pairs.
///
/// Keys in iproto maps are small unsigned integers; anything else is a
/// protocol violation and ends the walk with an error.
pub(crate) struct MapCursor<'a> {
    buf: &'a [u8],
    pos: usize,
    remaining: usize,
}

impl<'a> MapCursor<'a> {
    /// Start walking the map at the beginning of `buf`.
    ///
    /// Returns the cursor and the offset just past the whole map, so the
    /// caller can keep parsing whatever follows.
    pub(crate) fn new(buf: &'a [u8]) -> Result<(Self, usize)> {
        let total = value_len(buf)?;
        let marker = buf[0];
        let (remaining, header) = match marker {
            0x80..=0x8f => (usize::from(marker & 0x0f), 1),
            0xde => (read_len(buf, 1, 2)?, 3),
            0xdf => (read_len(buf, 1, 4)?, 5),
            _ => return Err(Error::protocol("expected a MessagePack map")),
        };
        Ok((MapCursor { buf, pos: header, remaining }, total))
    }

    /// The next entry as `(key, value offset, value bytes)`, or `None` when
    /// the map is exhausted. The offset is relative to the buffer the cursor
    /// was created over, so callers can remember where a value lives
    /// without holding a borrow.
    pub(crate) fn next(&mut self) -> Result<Option<(u64, usize, &'a [u8])>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let key_len = value_len(&self.buf[self.pos..])?;
        let key = read_uint(&self.buf[self.pos..self.pos + key_len])?;
        self.pos += key_len;
        let value_len = value_len(&self.buf[self.pos..])?;
        let offset = self.pos;
        let value = &self.buf[self.pos..self.pos + value_len];
        self.pos += value_len;
        Ok(Some((key, offset, value)))
    }
}

/// Decode an unsigned `MessagePack` integer from a complete value slice.
pub(crate) fn read_uint(value: &[u8]) -> Result<u64> {
    let mut cursor = value;
    rmp::decode::read_int::<u64, _>(&mut cursor)
        .map_err(|_| Error::protocol("expected an unsigned integer"))
}

/// Decode a `MessagePack` string from a complete value slice.
pub(crate) fn read_str(value: &[u8]) -> Result<&str> {
    let mut cursor = value;
    let len =
        rmp::decode::read_str_len(&mut cursor).map_err(|_| Error::protocol("expected a string"))?;
    let bytes = cursor.get(..len as usize).ok_or_else(|| Error::protocol("string is truncated"))?;
    std::str::from_utf8(bytes).map_err(|_| Error::protocol("string is not UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(value: &rmpv::Value) -> Vec<u8> {
        let mut out = Vec::new();
        rmpv::encode::write_value(&mut out, value).unwrap();
        out
    }

    #[test]
    fn measures_scalars_and_containers() {
        use rmpv::Value;
        let samples = [
            Value::Nil,
            Value::from(7),
            Value::from(-3),
            Value::from(70_000u64),
            Value::from(-70_000i64),
            Value::from(1.5f64),
            Value::from("hello"),
            Value::from("x".repeat(300)),
            Value::Binary(vec![1, 2, 3]),
            Value::Array(vec![Value::from(1), Value::from("a"), Value::Nil]),
            Value::Map(vec![(Value::from(1), Value::Array(vec![Value::from(2)]))]),
            Value::Ext(2, vec![0; 16]),
            Value::Ext(6, vec![1, 2, 3]),
        ];
        for sample in &samples {
            let bytes = encoded(sample);
            let mut with_tail = bytes.clone();
            with_tail.extend_from_slice(b"tail");
            assert_eq!(value_len(&with_tail).unwrap(), bytes.len(), "{sample:?}");
        }
    }

    #[test]
    fn truncated_input_is_reported() {
        let bytes = encoded(&rmpv::Value::from("hello"));
        assert!(value_len(&bytes[..3]).is_err());
        assert!(value_len(&[]).is_err());
    }

    #[test]
    fn map_cursor_walks_entries_in_order() {
        use rmpv::Value;
        let bytes = encoded(&Value::Map(vec![
            (Value::from(0x30), Value::Array(vec![Value::from(1)])),
            (Value::from(0x05), Value::from(42)),
        ]));
        let (mut cursor, total) = MapCursor::new(&bytes).unwrap();
        assert_eq!(total, bytes.len());
        let (k, offset, v) = cursor.next().unwrap().unwrap();
        assert_eq!(k, 0x30);
        assert_eq!(v, &encoded(&Value::Array(vec![Value::from(1)]))[..]);
        assert_eq!(&bytes[offset..offset + v.len()], v);
        let (k, _, v) = cursor.next().unwrap().unwrap();
        assert_eq!(k, 0x05);
        assert_eq!(read_uint(v).unwrap(), 42);
        assert!(cursor.next().unwrap().is_none());
    }
}
