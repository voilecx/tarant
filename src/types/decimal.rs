//! Tarantool's `decimal` field type: up to 38 significant digits, exact.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::ext;
use crate::iproto::ext::DECIMAL;

/// An exact decimal number, as Tarantool stores it.
///
/// Holds up to [`MAX_DIGITS`](Self::MAX_DIGITS) significant digits and a
/// scale, so money and measurements survive the round trip untouched. Parse
/// one from a string, or convert from an integer; with the `rust_decimal`
/// feature, from a [`rust_decimal::Decimal`] too.
///
/// ```
/// use tarant::Decimal;
///
/// let price: Decimal = "19.99".parse().unwrap();
/// assert_eq!(price.to_string(), "19.99");
/// assert_eq!(price.scale(), 2);
/// assert_eq!(price, "19.990".parse().unwrap()); // compares by value
/// ```
///
/// Comparison and hashing are numeric: `1.0` equals `1.00`. Display keeps
/// the scale the value was created with, as Tarantool does.
#[derive(Clone)]
pub struct Decimal {
    negative: bool,
    scale: i32,
    /// Significant digits, most significant first, each `0..=9`, no leading
    /// zeros. Zero is the single digit `0`, never negative.
    digits: Vec<u8>,
}

impl Decimal {
    /// The most significant digits Tarantool accepts.
    pub const MAX_DIGITS: usize = 38;

    /// The number zero.
    pub fn zero() -> Self {
        Self { negative: false, scale: 0, digits: vec![0] }
    }

    /// `mantissa × 10^(-scale)`, e.g. `from_parts(1999, 2)` is `19.99`.
    ///
    /// A negative scale is allowed and means trailing zeros: `from_parts(5, -3)`
    /// is `5000`. Fails when the mantissa has more than 38 digits.
    pub fn from_parts(mantissa: i128, scale: i32) -> Result<Self, DecimalError> {
        let digits: Vec<u8> =
            mantissa.unsigned_abs().to_string().bytes().map(|b| b - b'0').collect();
        Self::assemble(mantissa < 0, scale, digits)
    }

    /// The number of digits after the decimal point (negative for trailing zeros).
    pub const fn scale(&self) -> i32 {
        self.scale
    }

    /// The number of significant digits.
    pub fn precision(&self) -> usize {
        self.digits.len()
    }

    /// Whether the value is below zero.
    pub const fn is_negative(&self) -> bool {
        self.negative
    }

    /// Whether the value is exactly zero.
    pub fn is_zero(&self) -> bool {
        self.digits == [0]
    }

    /// The significant digits as an integer: the value is `mantissa() × 10^(-scale())`.
    pub fn mantissa(&self) -> i128 {
        let magnitude = self.digits.iter().fold(0i128, |acc, &d| acc * 10 + i128::from(d));
        if self.negative { -magnitude } else { magnitude }
    }

    /// The nearest `f64`. Lossy for more than 15–17 significant digits.
    pub fn to_f64(&self) -> f64 {
        self.to_string().parse().unwrap_or(f64::NAN)
    }

    fn assemble(negative: bool, scale: i32, mut digits: Vec<u8>) -> Result<Self, DecimalError> {
        let leading = digits.iter().take_while(|&&d| d == 0).count();
        digits.drain(..leading);
        if digits.is_empty() {
            return Ok(Self::zero());
        }
        if digits.len() > Self::MAX_DIGITS {
            return Err(DecimalError("more than 38 significant digits"));
        }
        Ok(Self { negative, scale, digits })
    }

    /// The `(negative, scale, digits)` with trailing zeros folded into the scale.
    fn canonical(&self) -> (bool, i64, &[u8]) {
        if self.is_zero() {
            return (false, 0, &self.digits);
        }
        let trailing = self.digits.iter().rev().take_while(|&&d| d == 0).count();
        let kept = &self.digits[..self.digits.len() - trailing];
        let scale = i64::from(self.scale) - i64::try_from(trailing).unwrap_or(i64::MAX);
        (self.negative, scale, kept)
    }

    /// Digits before the decimal point, as a signed count (negative when the
    /// value is below `0.1` and needs leading zeros after the point).
    fn integer_len(&self) -> i64 {
        i64::try_from(self.digits.len()).unwrap_or(i64::MAX) - i64::from(self.scale)
    }

    fn cmp_magnitude(&self, other: &Self) -> Ordering {
        match self.integer_len().cmp(&other.integer_len()) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
        let width = self.digits.len().max(other.digits.len());
        let a = self.digits.iter().copied().chain(std::iter::repeat(0)).take(width);
        let b = other.digits.iter().copied().chain(std::iter::repeat(0)).take(width);
        a.cmp(b)
    }

    /// The `MP_DECIMAL` payload: the scale, then packed BCD digits and a sign nibble.
    pub(crate) fn to_payload(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.digits.len() / 2 + 6);
        rmp::encode::write_sint(&mut out, i64::from(self.scale)).expect("vec write");
        let mut nibbles = Vec::with_capacity(self.digits.len() + 2);
        if self.digits.len() % 2 == 0 {
            nibbles.push(0);
        }
        nibbles.extend_from_slice(&self.digits);
        nibbles.push(if self.negative { 0x0d } else { 0x0c });
        out.extend(nibbles.chunks(2).map(|pair| (pair[0] << 4) | pair[1]));
        out
    }

    /// Decode an `MP_DECIMAL` payload.
    pub(crate) fn from_payload(payload: &[u8]) -> Result<Self, DecimalError> {
        let mut cursor = payload;
        let scale: i64 = rmp::decode::read_int(&mut cursor)
            .map_err(|_| DecimalError("payload does not start with a scale"))?;
        let scale = i32::try_from(scale).map_err(|_| DecimalError("scale out of range"))?;
        let mut nibbles = Vec::with_capacity(cursor.len() * 2);
        for byte in cursor {
            nibbles.push(byte >> 4);
            nibbles.push(byte & 0x0f);
        }
        let negative = match nibbles.pop() {
            Some(0x0a | 0x0c | 0x0e | 0x0f) => false,
            Some(0x0b | 0x0d) => true,
            _ => return Err(DecimalError("payload has no sign nibble")),
        };
        if nibbles.iter().any(|&d| d > 9) {
            return Err(DecimalError("payload has a non-decimal digit"));
        }
        Self::assemble(negative, scale, nibbles)
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = String::with_capacity(self.digits.len() + 4);
        if self.negative {
            out.push('-');
        }
        let digit = |d: u8| char::from(b'0' + d);
        if self.scale <= 0 {
            out.extend(self.digits.iter().map(|&d| digit(d)));
            out.extend(std::iter::repeat_n('0', self.scale.unsigned_abs() as usize));
        } else {
            let scale = self.scale.unsigned_abs() as usize;
            if self.digits.len() > scale {
                let (int, frac) = self.digits.split_at(self.digits.len() - scale);
                out.extend(int.iter().map(|&d| digit(d)));
                out.push('.');
                out.extend(frac.iter().map(|&d| digit(d)));
            } else {
                out.push_str("0.");
                out.extend(std::iter::repeat_n('0', scale - self.digits.len()));
                out.extend(self.digits.iter().map(|&d| digit(d)));
            }
        }
        f.pad(&out)
    }
}

impl fmt::Debug for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Decimal({self})")
    }
}

impl FromStr for Decimal {
    type Err = DecimalError;

    /// Accepts `[+-]digits[.digits][e[+-]digits]`.
    fn from_str(s: &str) -> Result<Self, DecimalError> {
        let (negative, rest) = match s.as_bytes().first() {
            Some(b'-') => (true, &s[1..]),
            Some(b'+') => (false, &s[1..]),
            _ => (false, s),
        };
        let (number, exponent) = match rest.find(['e', 'E']) {
            Some(at) => (&rest[..at], Some(&rest[at + 1..])),
            None => (rest, None),
        };
        let exponent: i32 = match exponent {
            Some(text) => text.parse().map_err(|_| DecimalError("bad exponent"))?,
            None => 0,
        };
        let (int, frac) = number.split_once('.').unwrap_or((number, ""));
        if int.is_empty() && frac.is_empty() {
            return Err(DecimalError("no digits"));
        }
        let mut digits = Vec::with_capacity(int.len() + frac.len());
        for byte in int.bytes().chain(frac.bytes()) {
            if !byte.is_ascii_digit() {
                return Err(DecimalError("not a decimal number"));
            }
            digits.push(byte - b'0');
        }
        let scale = i32::try_from(frac.len())
            .ok()
            .and_then(|s| s.checked_sub(exponent))
            .ok_or(DecimalError("scale out of range"))?;
        Self::assemble(negative, scale, digits)
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.canonical() == other.canonical()
    }
}

impl Eq for Decimal {}

impl Hash for Decimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.canonical().hash(state);
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.is_zero(), other.is_zero()) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if other.negative { Ordering::Greater } else { Ordering::Less };
            }
            (false, true) => return if self.negative { Ordering::Less } else { Ordering::Greater },
            (false, false) => {}
        }
        match (self.negative, other.negative) {
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            (false, false) => self.cmp_magnitude(other),
            (true, true) => other.cmp_magnitude(self),
        }
    }
}

macro_rules! from_ints {
    ($($ty:ty),*) => {$(
        impl From<$ty> for Decimal {
            fn from(value: $ty) -> Self {
                Self::from_parts(i128::from(value), 0).expect("at most 20 digits")
            }
        }
    )*};
}

from_ints!(i8, i16, i32, i64, u8, u16, u32, u64);

impl TryFrom<i128> for Decimal {
    type Error = DecimalError;

    fn try_from(value: i128) -> Result<Self, DecimalError> {
        Self::from_parts(value, 0)
    }
}

impl TryFrom<u128> for Decimal {
    type Error = DecimalError;

    fn try_from(value: u128) -> Result<Self, DecimalError> {
        value.to_string().parse()
    }
}

impl Serialize for Decimal {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        ext::serialize(DECIMAL, &self.to_payload(), s)
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let payload = ext::deserialize_tagged(d, DECIMAL, "a decimal")?;
        Self::from_payload(&payload).map_err(serde::de::Error::custom)
    }
}

impl From<Decimal> for rmpv::Value {
    fn from(value: Decimal) -> Self {
        Self::Ext(DECIMAL, value.to_payload())
    }
}

impl TryFrom<&rmpv::Value> for Decimal {
    type Error = DecimalError;

    fn try_from(value: &rmpv::Value) -> Result<Self, DecimalError> {
        match value {
            rmpv::Value::Ext(DECIMAL, payload) => Self::from_payload(payload),
            _ => Err(DecimalError("not an MP_DECIMAL value")),
        }
    }
}

#[cfg(feature = "rust_decimal")]
impl From<rust_decimal::Decimal> for Decimal {
    fn from(value: rust_decimal::Decimal) -> Self {
        Self::from_parts(value.mantissa(), i32::try_from(value.scale()).unwrap_or(i32::MAX))
            .expect("rust_decimal holds at most 29 digits")
    }
}

#[cfg(feature = "rust_decimal")]
impl TryFrom<Decimal> for rust_decimal::Decimal {
    type Error = DecimalError;

    /// Fails when the value needs more precision than `rust_decimal` holds.
    fn try_from(value: Decimal) -> Result<Self, DecimalError> {
        Self::from_str_exact(&value.to_string())
            .map_err(|_| DecimalError("does not fit in a rust_decimal::Decimal"))
    }
}

/// Why a string or payload could not become a [`Decimal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalError(&'static str);

impl fmt::Display for DecimalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid decimal: {}", self.0)
    }
}

impl std::error::Error for DecimalError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_documented_encodings() {
        // -12.34 → d6 01 | 02 01 23 4d (fixext 4, scale 2, digits 1 2 3 4, minus).
        let value: Decimal = "-12.34".parse().unwrap();
        assert_eq!(value.to_payload(), [0x02, 0x01, 0x23, 0x4d]);
        assert_eq!(rmp_serde::to_vec(&value).unwrap(), [0xd6, 0x01, 0x02, 0x01, 0x23, 0x4d]);
        // 0.000000000000000000000000000000000010 → c7 03 01 | 24 01 0c.
        let tiny: Decimal = "0.000000000000000000000000000000000010".parse().unwrap();
        assert_eq!(tiny.to_payload(), [0x24, 0x01, 0x0c]);
        assert_eq!(rmp_serde::to_vec(&tiny).unwrap(), [0xc7, 0x03, 0x01, 0x24, 0x01, 0x0c]);
    }

    #[test]
    fn round_trips_through_serde_and_value() {
        for text in
            ["0", "1", "-1", "19.99", "-0.5", "123456789012345678901234567890.12345678", "5000"]
        {
            let value: Decimal = text.parse().unwrap();
            let bytes = rmp_serde::to_vec(&value).unwrap();
            let back: Decimal = rmp_serde::from_slice(&bytes).unwrap();
            assert_eq!(back, value, "{text}");
            assert_eq!(back.to_string(), text, "{text}");
            let via_value = Decimal::try_from(&rmpv::Value::from(value.clone())).unwrap();
            assert_eq!(via_value, value);
        }
    }

    #[test]
    fn displays_with_the_scale_it_was_given() {
        assert_eq!(Decimal::from_parts(1999, 2).unwrap().to_string(), "19.99");
        assert_eq!(Decimal::from_parts(5, -3).unwrap().to_string(), "5000");
        assert_eq!(Decimal::from_parts(7, 3).unwrap().to_string(), "0.007");
        assert_eq!("1.50".parse::<Decimal>().unwrap().to_string(), "1.50");
        assert_eq!("1e3".parse::<Decimal>().unwrap().to_string(), "1000");
        assert_eq!("-0".parse::<Decimal>().unwrap().to_string(), "0");
        assert_eq!(Decimal::from(-42i64).to_string(), "-42");
    }

    #[test]
    fn compares_numerically() {
        let d = |s: &str| s.parse::<Decimal>().unwrap();
        assert_eq!(d("1.0"), d("1.00"));
        assert_eq!(d("100"), d("1e2"));
        assert!(d("0.01") > d("0.001"));
        assert!(d("-5") < d("-4.9"));
        assert!(d("-0.1") < d("0"));
        assert!(d("10") > d("9.99999"));
        assert_eq!(d("0.5").cmp(&d("0.5")), Ordering::Equal);
        assert_eq!(d("1.0").mantissa(), 10);
        assert_eq!(d("-12.34").mantissa(), -1234);
    }

    #[test]
    fn rejects_garbage() {
        assert!("".parse::<Decimal>().is_err());
        assert!("1.2.3".parse::<Decimal>().is_err());
        assert!("abc".parse::<Decimal>().is_err());
        assert!("1".repeat(39).parse::<Decimal>().is_err());
        assert!(Decimal::from_payload(&[0x02, 0x01, 0x2f, 0x40]).is_err()); // digit 0xf
        assert!(Decimal::from_payload(&[]).is_err());
    }
}
