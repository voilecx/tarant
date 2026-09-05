//! Tarantool's `interval` type: a calendar span, field by field.

use std::fmt;
use std::ops::{Add, Neg};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::ext;
use crate::iproto::ext::INTERVAL;

/// A span of calendar time, as Tarantool's `datetime.interval` holds it.
///
/// Each unit is kept separately — a month is a month, not thirty days —
/// so the value means the same thing on the server as it does here. Build
/// one from the unit constructors and add them together:
///
/// ```
/// use tarant::Interval;
///
/// let span = Interval::years(1) + Interval::months(200) + Interval::days(-77);
/// assert_eq!(span.to_string(), "+1 years, 200 months, -77 days");
/// assert_eq!(span.days, -77);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Interval {
    /// Years.
    pub years: i64,
    /// Months.
    pub months: i64,
    /// Weeks.
    pub weeks: i64,
    /// Days.
    pub days: i64,
    /// Hours.
    pub hours: i64,
    /// Minutes.
    pub minutes: i64,
    /// Seconds.
    pub seconds: i64,
    /// Nanoseconds.
    pub nanoseconds: i64,
    /// What happens when adding months lands past the end of a month.
    pub adjust: Adjust,
}

/// How month arithmetic treats a day that does not exist in the target month.
///
/// Adding one month to January 31st needs a decision; these are Tarantool's
/// three, under the names it gives them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Adjust {
    /// Tarantool's `'none'`, and the default: clamp to the last day, so
    /// January 31st plus a month is February 28th (or 29th).
    #[default]
    Limit,
    /// Tarantool's `'excess'`: carry the overflow into the next month, so
    /// January 31st plus a month is March 3rd (or 2nd).
    Excess,
    /// Tarantool's `'last'`: if the start was the last day of its month,
    /// snap to the last day of the target month.
    Snap,
}

impl Adjust {
    const fn code(self) -> i64 {
        match self {
            Self::Excess => 0,
            Self::Limit => 1,
            Self::Snap => 2,
        }
    }

    const fn from_code(code: i64) -> Option<Self> {
        match code {
            0 => Some(Self::Excess),
            1 => Some(Self::Limit),
            2 => Some(Self::Snap),
            _ => None,
        }
    }
}

/// Field ids on the wire, in the order Tarantool defines them.
const YEAR: u64 = 0;
const MONTH: u64 = 1;
const WEEK: u64 = 2;
const DAY: u64 = 3;
const HOUR: u64 = 4;
const MINUTE: u64 = 5;
const SECOND: u64 = 6;
const NANOSECOND: u64 = 7;
const ADJUST: u64 = 8;

impl Interval {
    /// No time at all.
    pub const ZERO: Self = Self {
        years: 0,
        months: 0,
        weeks: 0,
        days: 0,
        hours: 0,
        minutes: 0,
        seconds: 0,
        nanoseconds: 0,
        adjust: Adjust::Limit,
    };

    /// `n` years.
    pub const fn years(n: i64) -> Self {
        Self { years: n, ..Self::ZERO }
    }

    /// `n` months.
    pub const fn months(n: i64) -> Self {
        Self { months: n, ..Self::ZERO }
    }

    /// `n` weeks.
    pub const fn weeks(n: i64) -> Self {
        Self { weeks: n, ..Self::ZERO }
    }

    /// `n` days.
    pub const fn days(n: i64) -> Self {
        Self { days: n, ..Self::ZERO }
    }

    /// `n` hours.
    pub const fn hours(n: i64) -> Self {
        Self { hours: n, ..Self::ZERO }
    }

    /// `n` minutes.
    pub const fn minutes(n: i64) -> Self {
        Self { minutes: n, ..Self::ZERO }
    }

    /// `n` seconds.
    pub const fn seconds(n: i64) -> Self {
        Self { seconds: n, ..Self::ZERO }
    }

    /// `n` nanoseconds.
    pub const fn nanoseconds(n: i64) -> Self {
        Self { nanoseconds: n, ..Self::ZERO }
    }

    /// The same span with a different month-end rule.
    #[must_use]
    pub const fn with_adjust(mut self, adjust: Adjust) -> Self {
        self.adjust = adjust;
        self
    }

    /// Whether every unit is zero.
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        self.years == 0
            && self.months == 0
            && self.weeks == 0
            && self.days == 0
            && self.hours == 0
            && self.minutes == 0
            && self.seconds == 0
            && self.nanoseconds == 0
    }

    /// Every unit with its wire id and display name.
    const fn fields(&self) -> [(u64, i64, &'static str); 8] {
        [
            (YEAR, self.years, "years"),
            (MONTH, self.months, "months"),
            (WEEK, self.weeks, "weeks"),
            (DAY, self.days, "days"),
            (HOUR, self.hours, "hours"),
            (MINUTE, self.minutes, "minutes"),
            (SECOND, self.seconds, "seconds"),
            (NANOSECOND, self.nanoseconds, "nanoseconds"),
        ]
    }

    /// The `MP_INTERVAL` payload: a count, then `(field id, value)` pairs for
    /// every non-zero unit. `adjust` is written unless it is `Excess`, which
    /// is what an absent field means to the server.
    pub(crate) fn to_payload(self) -> Vec<u8> {
        let fields = self.fields();
        let present = fields.iter().filter(|(_, value, _)| *value != 0).count()
            + usize::from(self.adjust != Adjust::Excess);
        let mut out = Vec::with_capacity(2 + present * 10);
        let count = u64::try_from(present).expect("at most 9");
        rmp::encode::write_uint(&mut out, count).expect("vec write");
        for (id, value, _) in fields.into_iter().filter(|(_, value, _)| *value != 0) {
            rmp::encode::write_uint(&mut out, id).expect("vec write");
            rmp::encode::write_sint(&mut out, value).expect("vec write");
        }
        if self.adjust != Adjust::Excess {
            rmp::encode::write_uint(&mut out, ADJUST).expect("vec write");
            rmp::encode::write_sint(&mut out, self.adjust.code()).expect("vec write");
        }
        out
    }

    /// Decode an `MP_INTERVAL` payload.
    pub(crate) fn from_payload(payload: &[u8]) -> Result<Self, IntervalError> {
        let mut cursor = payload;
        let count: u64 = rmp::decode::read_int(&mut cursor)
            .map_err(|_| IntervalError("payload does not start with a field count"))?;
        let mut interval = Self::ZERO.with_adjust(Adjust::Excess);
        for _ in 0..count {
            let id: u64 =
                rmp::decode::read_int(&mut cursor).map_err(|_| IntervalError("bad field id"))?;
            let value: i64 =
                rmp::decode::read_int(&mut cursor).map_err(|_| IntervalError("bad field value"))?;
            match id {
                YEAR => interval.years = value,
                MONTH => interval.months = value,
                WEEK => interval.weeks = value,
                DAY => interval.days = value,
                HOUR => interval.hours = value,
                MINUTE => interval.minutes = value,
                SECOND => interval.seconds = value,
                NANOSECOND => interval.nanoseconds = value,
                ADJUST => {
                    interval.adjust =
                        Adjust::from_code(value).ok_or(IntervalError("unknown adjust"))?;
                }
                _ => return Err(IntervalError("unknown field id")),
            }
        }
        if !cursor.is_empty() {
            return Err(IntervalError("trailing bytes"));
        }
        Ok(interval)
    }
}

impl Add for Interval {
    type Output = Self;

    /// Unit-wise, saturating. The rule comes from the left side unless it
    /// is the default, in which case the right side's is used.
    fn add(self, rhs: Self) -> Self {
        Self {
            years: self.years.saturating_add(rhs.years),
            months: self.months.saturating_add(rhs.months),
            weeks: self.weeks.saturating_add(rhs.weeks),
            days: self.days.saturating_add(rhs.days),
            hours: self.hours.saturating_add(rhs.hours),
            minutes: self.minutes.saturating_add(rhs.minutes),
            seconds: self.seconds.saturating_add(rhs.seconds),
            nanoseconds: self.nanoseconds.saturating_add(rhs.nanoseconds),
            adjust: if self.adjust == Adjust::Limit { rhs.adjust } else { self.adjust },
        }
    }
}

impl Neg for Interval {
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            years: self.years.saturating_neg(),
            months: self.months.saturating_neg(),
            weeks: self.weeks.saturating_neg(),
            days: self.days.saturating_neg(),
            hours: self.hours.saturating_neg(),
            minutes: self.minutes.saturating_neg(),
            seconds: self.seconds.saturating_neg(),
            nanoseconds: self.nanoseconds.saturating_neg(),
            adjust: self.adjust,
        }
    }
}

impl fmt::Display for Interval {
    /// Tarantool's own notation: `+1 years, 200 months, -77 days`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use std::fmt::Write as _;
        let mut out = String::new();
        for (_, value, unit) in self.fields().into_iter().filter(|(_, value, _)| *value != 0) {
            if out.is_empty() {
                write!(out, "{value:+} {unit}")?;
            } else {
                write!(out, ", {value} {unit}")?;
            }
        }
        if out.is_empty() {
            out.push_str("0 seconds");
        }
        f.pad(&out)
    }
}

impl Serialize for Interval {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        ext::serialize(INTERVAL, &(*self).to_payload(), s)
    }
}

impl<'de> Deserialize<'de> for Interval {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let payload = ext::deserialize_tagged(d, INTERVAL, "an interval")?;
        Self::from_payload(&payload).map_err(serde::de::Error::custom)
    }
}

impl From<Interval> for rmpv::Value {
    fn from(value: Interval) -> Self {
        Self::Ext(INTERVAL, value.to_payload())
    }
}

impl TryFrom<&rmpv::Value> for Interval {
    type Error = IntervalError;

    fn try_from(value: &rmpv::Value) -> Result<Self, IntervalError> {
        match value {
            rmpv::Value::Ext(INTERVAL, payload) => Self::from_payload(payload),
            _ => Err(IntervalError("not an MP_INTERVAL value")),
        }
    }
}

/// The payload was not a valid interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntervalError(&'static str);

impl fmt::Display for IntervalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid interval: {}", self.0)
    }
}

impl std::error::Error for IntervalError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_documented_encoding() {
        // datetime.interval.new{year = 1, month = 200, day = -77}:
        // C7 0B 06 | 04 | 00 01 | 01 CC C8 | 03 D0 B3 | 08 01
        let span = Interval::years(1) + Interval::months(200) + Interval::days(-77);
        let bytes = rmp_serde::to_vec(&span).unwrap();
        assert_eq!(
            bytes,
            [0xc7, 0x0b, 0x06, 0x04, 0x00, 0x01, 0x01, 0xcc, 0xc8, 0x03, 0xd0, 0xb3, 0x08, 0x01]
        );
        assert_eq!(rmp_serde::from_slice::<Interval>(&bytes).unwrap(), span);
    }

    #[test]
    fn zero_with_excess_is_a_bare_zero_count() {
        let nothing = Interval::ZERO.with_adjust(Adjust::Excess);
        assert_eq!(nothing.to_payload(), [0x00]);
        assert_eq!(Interval::from_payload(&[0x00]).unwrap(), nothing);
        assert!(nothing.is_zero());
        assert_eq!(nothing.to_string(), "0 seconds");
    }

    #[test]
    fn arithmetic_and_display() {
        let span = Interval::hours(1) + Interval::minutes(-30) + Interval::nanoseconds(5);
        assert_eq!(span.to_string(), "+1 hours, -30 minutes, 5 nanoseconds");
        assert_eq!((-span).minutes, 30);
        assert_eq!(
            (Interval::days(1).with_adjust(Adjust::Snap) + Interval::days(1)).adjust,
            Adjust::Snap
        );
        assert_eq!(
            (Interval::days(1) + Interval::days(1).with_adjust(Adjust::Excess)).adjust,
            Adjust::Excess
        );
    }

    #[test]
    fn rejects_unknown_fields_and_trailing_bytes() {
        assert!(Interval::from_payload(&[0x01, 0x09, 0x01]).is_err());
        assert!(Interval::from_payload(&[0x01, 0x08, 0x07]).is_err());
        assert!(Interval::from_payload(&[0x00, 0x00]).is_err());
        assert!(Interval::from_payload(&[]).is_err());
    }
}
