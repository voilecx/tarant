//! What may stand where the protocol wants an array.
//!
//! Tarantool encodes three things as `MP_ARRAY`: the tuple in a DML request,
//! the key in a lookup, and the argument list of a call. Rust callers should
//! not have to remember that — a single scalar key is `5`, not `(5,)`, and an
//! argument-less call is `()`. The sealed [`Key`] and [`Args`] traits accept
//! exactly the Rust shapes that have one obvious array encoding and reject
//! everything else at compile time.

use rmpv::Value;
use serde::Serialize;

use crate::codec::to_msgpack;
use crate::error::Result;

mod sealed {
    pub trait ArrayLike {
        /// Append this value as a `MessagePack` array.
        fn encode(&self, buf: &mut Vec<u8>) -> crate::error::Result<()>;
    }
}

pub(crate) use sealed::ArrayLike;

/// A lookup key: one or more field values, in index order.
///
/// Implemented for scalars (a single-field key), tuples up to eight fields,
/// arrays, slices and `Vec`s of serialisable values, and [`Value`]. The unit
/// type `()` is the empty key, which every iterator except `Eq`/`Req`
/// accepts to mean "from the start".
///
/// ```
/// # fn takes_key<K: tarant::Key>(_: K) {}
/// takes_key(42u64);                    // single-field key
/// takes_key(("ann", 30u32));           // composite key
/// takes_key(());                       // empty key
/// takes_key(vec![tarant::Value::from(1)]);
/// ```
pub trait Key: ArrayLike {}

/// Arguments of a stored-procedure call or a Lua `eval`.
///
/// Same shapes as [`Key`]: `()` for no arguments, a tuple for several, a
/// scalar for one.
pub trait Args: ArrayLike {}

impl<T: ArrayLike + ?Sized> Key for T {}
impl<T: ArrayLike + ?Sized> Args for T {}

impl ArrayLike for () {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.push(0x90);
        Ok(())
    }
}

macro_rules! scalar_keys {
    ($($ty:ty),* $(,)?) => {$(
        impl ArrayLike for $ty {
            fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
                buf.push(0x91);
                buf.extend_from_slice(&to_msgpack(self)?);
                Ok(())
            }
        }
    )*};
}

scalar_keys!(u8, u16, u32, u64, i8, i16, i32, i64, f32, f64, bool, str, String);

#[cfg(feature = "uuid")]
scalar_keys!(uuid::Uuid);

impl<T: ArrayLike + ?Sized> ArrayLike for &T {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        (**self).encode(buf)
    }
}

impl ArrayLike for Value {
    /// An array is sent as-is; any other value becomes a single-field key.
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Array(_) => {
                rmpv::encode::write_value(buf, self).map_err(crate::error::Error::encode)
            }
            other => {
                buf.push(0x91);
                rmpv::encode::write_value(buf, other).map_err(crate::error::Error::encode)
            }
        }
    }
}

impl<T: Serialize> ArrayLike for Vec<T> {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        self.as_slice().encode(buf)
    }
}

impl<T: Serialize> ArrayLike for [T] {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(&to_msgpack(self)?);
        Ok(())
    }
}

impl<T: Serialize, const N: usize> ArrayLike for [T; N] {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(&to_msgpack(self.as_slice())?);
        Ok(())
    }
}

macro_rules! tuple_keys {
    ($($name:ident),+) => {
        impl<$($name: Serialize),+> ArrayLike for ($($name,)+) {
            fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
                buf.extend_from_slice(&to_msgpack(self)?);
                Ok(())
            }
        }
    };
}

tuple_keys!(A);
tuple_keys!(A, B);
tuple_keys!(A, B, C);
tuple_keys!(A, B, C, D);
tuple_keys!(A, B, C, D, E);
tuple_keys!(A, B, C, D, E, F);
tuple_keys!(A, B, C, D, E, F, G);
tuple_keys!(A, B, C, D, E, F, G, H);

/// Encode any [`ArrayLike`] to a standalone buffer.
pub(crate) fn encode_array<T: ArrayLike + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    value.encode(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decoded<T: ArrayLike + ?Sized>(value: &T) -> Value {
        let bytes = encode_array(value).unwrap();
        rmpv::decode::read_value(&mut &bytes[..]).unwrap()
    }

    #[test]
    fn scalars_become_one_field_keys() {
        assert_eq!(decoded(&7u64), Value::Array(vec![Value::from(7)]));
        assert_eq!(decoded("ann"), Value::Array(vec![Value::from("ann")]));
        assert_eq!(decoded(&true), Value::Array(vec![Value::from(true)]));
    }

    #[test]
    fn tuples_and_units_are_arrays() {
        assert_eq!(decoded(&()), Value::Array(vec![]));
        assert_eq!(
            decoded(&("ann", 30u32)),
            Value::Array(vec![Value::from("ann"), Value::from(30)])
        );
        assert_eq!(decoded(&[1u8, 2]), Value::Array(vec![Value::from(1), Value::from(2)]));
    }

    #[test]
    fn value_arrays_pass_through_and_scalars_wrap() {
        let arr = Value::Array(vec![Value::from(1)]);
        assert_eq!(decoded(&arr), arr);
        assert_eq!(decoded(&Value::from("x")), Value::Array(vec![Value::from("x")]));
    }
}
