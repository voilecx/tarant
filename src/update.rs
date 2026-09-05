//! Field-level modifications for `update` and `upsert`.
//!
//! An [`Update`] is an ordered list of operations, each applied to one field
//! of the stored tuple. Fields are addressed the way Tarantool documents them:
//! **1-based**, negative numbers counting from the end (`-1` is the last
//! field), or a JSON path such as `"[3].name"` into a map or nested tuple.
//! The client sends `IPROTO_INDEX_BASE = 1` so the numbers on the wire mean
//! what they mean in the manual.

use serde::Serialize;

use crate::codec::to_msgpack;
use crate::error::Result;

/// A field to modify: a 1-based position or a JSON path.
///
/// Built implicitly from `i32` and `&str`, so `update.set(2, v)` and
/// `update.set("[2].name", v)` both read naturally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldRef {
    /// A 1-based field number; negative counts from the end.
    Number(i32),
    /// A JSON path relative to the tuple, e.g. `"[2].name"` or `"data.items[1]"`.
    Path(String),
}

impl From<i32> for FieldRef {
    fn from(n: i32) -> Self {
        Self::Number(n)
    }
}

impl From<u32> for FieldRef {
    /// Saturates at [`i32::MAX`]: a tuple never has that many fields, so a
    /// number this large is a mistake the server will reject anyway.
    fn from(n: u32) -> Self {
        Self::Number(i32::try_from(n).unwrap_or(i32::MAX))
    }
}

impl From<&str> for FieldRef {
    fn from(path: &str) -> Self {
        Self::Path(path.to_owned())
    }
}

impl From<String> for FieldRef {
    fn from(path: String) -> Self {
        Self::Path(path)
    }
}

impl Serialize for FieldRef {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Number(n) => serializer.serialize_i32(*n),
            Self::Path(p) => serializer.serialize_str(p),
        }
    }
}

/// An ordered list of field operations.
///
/// ```
/// use tarant::Update;
///
/// let ops = Update::new()
///     .set(2, "ann")        // field 2 = "ann"
///     .add(3, 1)            // field 3 += 1
///     .delete(4, 1);        // remove one field starting at 4
/// assert_eq!(ops.len(), 3);
/// ```
///
/// Operations are encoded as they are added; an argument that cannot be
/// serialised is reported by the request that carries the update.
#[derive(Debug, Clone, Default)]
#[must_use = "an Update does nothing until passed to `update` or `upsert`"]
pub struct Update {
    ops: Vec<Vec<u8>>,
    error: Option<String>,
}

impl Update {
    /// No operations yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// `=`: assign `value` to `field`, extending the tuple if it is the next field.
    pub fn set(self, field: impl Into<FieldRef>, value: impl Serialize) -> Self {
        self.push(("=", field.into(), value))
    }

    /// `+`: add `delta` to a numeric field.
    pub fn add(self, field: impl Into<FieldRef>, delta: impl Serialize) -> Self {
        self.push(("+", field.into(), delta))
    }

    /// `-`: subtract `delta` from a numeric field.
    pub fn sub(self, field: impl Into<FieldRef>, delta: impl Serialize) -> Self {
        self.push(("-", field.into(), delta))
    }

    /// `&`: bitwise AND with `mask` on an unsigned field.
    pub fn bit_and(self, field: impl Into<FieldRef>, mask: u64) -> Self {
        self.push(("&", field.into(), mask))
    }

    /// `|`: bitwise OR with `mask` on an unsigned field.
    pub fn bit_or(self, field: impl Into<FieldRef>, mask: u64) -> Self {
        self.push(("|", field.into(), mask))
    }

    /// `^`: bitwise XOR with `mask` on an unsigned field.
    pub fn bit_xor(self, field: impl Into<FieldRef>, mask: u64) -> Self {
        self.push(("^", field.into(), mask))
    }

    /// `!`: insert `value` before `field`, shifting the rest right.
    pub fn insert(self, field: impl Into<FieldRef>, value: impl Serialize) -> Self {
        self.push(("!", field.into(), value))
    }

    /// `#`: delete `count` fields starting at `field`.
    pub fn delete(self, field: impl Into<FieldRef>, count: u32) -> Self {
        self.push(("#", field.into(), count))
    }

    /// `:`: replace `len` bytes at byte `offset` (1-based) of a string field with `replacement`.
    pub fn splice(
        self,
        field: impl Into<FieldRef>,
        offset: i32,
        len: u32,
        replacement: &str,
    ) -> Self {
        self.push((":", field.into(), offset, len, replacement))
    }

    /// Number of operations.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether no operations were added.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    fn push<T: Serialize>(mut self, op: T) -> Self {
        match to_msgpack(&op) {
            Ok(bytes) => self.ops.push(bytes),
            Err(err) => {
                self.error.get_or_insert_with(|| err.to_string());
            }
        }
        self
    }

    pub(crate) fn into_ops(self) -> Result<Vec<Vec<u8>>> {
        match self.error {
            Some(message) => Err(crate::error::Error::Encode(message.into())),
            None => Ok(self.ops),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmpv::Value;

    fn ops(update: Update) -> Vec<Value> {
        update
            .into_ops()
            .unwrap()
            .iter()
            .map(|bytes| rmpv::decode::read_value(&mut &bytes[..]).unwrap())
            .collect()
    }

    #[test]
    fn encodes_documented_shapes() {
        let decoded = ops(Update::new().set(2, "B").delete(3, 1).splice(4, 1, 2, "xy"));
        assert_eq!(
            decoded,
            vec![
                Value::Array(vec![Value::from("="), Value::from(2), Value::from("B")]),
                Value::Array(vec![Value::from("#"), Value::from(3), Value::from(1)]),
                Value::Array(vec![
                    Value::from(":"),
                    Value::from(4),
                    Value::from(1),
                    Value::from(2),
                    Value::from("xy")
                ]),
            ]
        );
    }

    #[test]
    fn paths_and_negative_fields() {
        let decoded = ops(Update::new().add("[2].hits", 1).set(-1, true));
        assert_eq!(decoded[0][1], Value::from("[2].hits"));
        assert_eq!(decoded[1][1], Value::from(-1));
    }
}
