//! Tarantool's `datetime` field type.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::ext;
use crate::iproto::ext::DATETIME;

const NANOS_PER_SECOND: u32 = 1_000_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

/// A point in time with nanosecond precision, plus the zone it was written in.
///
/// The instant is Unix time. The zone — a fixed offset in minutes, a
/// Tarantool zone index, or both — rides alongside for display and never
/// moves the instant, so two values with the same instant are equal whatever
/// their zones, exactly as in Tarantool.
///
/// Converts to and from [`SystemTime`]; with the `time`, `chrono` and `jiff`
/// features, to and from their datetime types as well.
///
/// ```
/// use tarant::Datetime;
///
/// let moment = Datetime::from_unix(1_700_000_000, 500_000_000);
/// assert_eq!(moment.to_string(), "2023-11-14T22:13:20.5Z");
/// assert_eq!(moment.with_tz_offset(180).to_string(), "2023-11-15T01:13:20.5+03:00");
/// assert_eq!(moment, moment.with_tz_offset(180)); // same instant
/// ```
#[derive(Clone, Copy)]
pub struct Datetime {
    epoch: i64,
    nsec: u32,
    tz_offset: i16,
    tz_index: i16,
}

impl Datetime {
    /// `1970-01-01T00:00:00Z`.
    pub const UNIX_EPOCH: Self = Self { epoch: 0, nsec: 0, tz_offset: 0, tz_index: 0 };

    /// The instant `seconds` after the Unix epoch, plus `nanoseconds`.
    ///
    /// Nanoseconds of a second or more carry into the seconds, so
    /// `from_unix(0, 1_500_000_000)` is `from_unix(1, 500_000_000)`.
    pub fn from_unix(seconds: i64, nanoseconds: u32) -> Self {
        Self {
            epoch: seconds.saturating_add(i64::from(nanoseconds / NANOS_PER_SECOND)),
            nsec: nanoseconds % NANOS_PER_SECOND,
            tz_offset: 0,
            tz_index: 0,
        }
    }

    /// The current instant, in UTC.
    pub fn now() -> Self {
        SystemTime::now().into()
    }

    /// Whole seconds since the Unix epoch.
    pub const fn unix_seconds(&self) -> i64 {
        self.epoch
    }

    /// The sub-second part, in nanoseconds (`0..1_000_000_000`).
    pub const fn nanosecond(&self) -> u32 {
        self.nsec
    }

    /// The zone offset from UTC, in minutes.
    pub const fn tz_offset_minutes(&self) -> i16 {
        self.tz_offset
    }

    /// The Tarantool time zone index (`0` when none was set).
    pub const fn tz_index(&self) -> i16 {
        self.tz_index
    }

    /// The same instant, displayed at `minutes` east of UTC.
    #[must_use]
    pub const fn with_tz_offset(mut self, minutes: i16) -> Self {
        self.tz_offset = minutes;
        self
    }

    /// The same instant, tagged with a Tarantool time zone index.
    #[must_use]
    pub const fn with_tz_index(mut self, index: i16) -> Self {
        self.tz_index = index;
        self
    }

    /// The `MP_DATETIME` payload: seconds, then the optional tail.
    pub(crate) fn to_payload(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&self.epoch.to_le_bytes());
        if self.nsec != 0 || self.tz_offset != 0 || self.tz_index != 0 {
            out.extend_from_slice(&self.nsec.to_le_bytes());
            out.extend_from_slice(&self.tz_offset.to_le_bytes());
            out.extend_from_slice(&self.tz_index.to_le_bytes());
        }
        out
    }

    /// Decode an `MP_DATETIME` payload of 8 or 16 bytes.
    pub(crate) fn from_payload(payload: &[u8]) -> Result<Self, DatetimeError> {
        let seconds = |b: &[u8]| i64::from_le_bytes(b[..8].try_into().expect("8 bytes"));
        match payload.len() {
            8 => Ok(Self::from_unix(seconds(payload), 0)),
            16 => {
                let nsec = u32::from_le_bytes(payload[8..12].try_into().expect("4 bytes"));
                let tz_offset = i16::from_le_bytes(payload[12..14].try_into().expect("2 bytes"));
                let tz_index = i16::from_le_bytes(payload[14..16].try_into().expect("2 bytes"));
                if nsec >= NANOS_PER_SECOND {
                    return Err(DatetimeError("nanoseconds out of range"));
                }
                Ok(Self { epoch: seconds(payload), nsec, tz_offset, tz_index })
            }
            _ => Err(DatetimeError("payload is not 8 or 16 bytes")),
        }
    }
}

impl fmt::Display for Datetime {
    /// RFC 3339 at the value's own offset: `2023-11-15T01:13:20.5+03:00`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use std::fmt::Write as _;
        let local = self.epoch.saturating_add(i64::from(self.tz_offset) * 60);
        let (year, month, day) = civil_from_days(local.div_euclid(SECONDS_PER_DAY));
        let second_of_day = local.rem_euclid(SECONDS_PER_DAY);
        let mut out = String::with_capacity(35);
        if year < 0 {
            out.push('-');
        }
        write!(
            out,
            "{:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}",
            year.abs(),
            second_of_day / 3600,
            second_of_day / 60 % 60,
            second_of_day % 60
        )?;
        if self.nsec != 0 {
            let fraction = format!("{:09}", self.nsec);
            out.push('.');
            out.push_str(fraction.trim_end_matches('0'));
        }
        if self.tz_offset == 0 {
            out.push('Z');
        } else {
            let sign = if self.tz_offset < 0 { '-' } else { '+' };
            let minutes = self.tz_offset.unsigned_abs();
            write!(out, "{sign}{:02}:{:02}", minutes / 60, minutes % 60)?;
        }
        f.pad(&out)
    }
}

/// Proleptic Gregorian date for a count of days since 1970-01-01.
///
/// Howard Hinnant's `civil_from_days`, valid for every `i64` day.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = u32::try_from(doy - (153 * mp + 2) / 5 + 1).expect("1..=31");
    let month = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).expect("1..=12");
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

impl fmt::Debug for Datetime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Datetime({self})")?;
        if self.tz_index != 0 {
            write!(f, "[tz #{}]", self.tz_index)?;
        }
        Ok(())
    }
}

impl PartialEq for Datetime {
    fn eq(&self, other: &Self) -> bool {
        (self.epoch, self.nsec) == (other.epoch, other.nsec)
    }
}

impl Eq for Datetime {}

impl Hash for Datetime {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.epoch, self.nsec).hash(state);
    }
}

impl PartialOrd for Datetime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Datetime {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.epoch, self.nsec).cmp(&(other.epoch, other.nsec))
    }
}

impl From<SystemTime> for Datetime {
    fn from(time: SystemTime) -> Self {
        match time.duration_since(UNIX_EPOCH) {
            Ok(after) => Self::from_unix(
                i64::try_from(after.as_secs()).unwrap_or(i64::MAX),
                after.subsec_nanos(),
            ),
            Err(before) => {
                let before = before.duration();
                let seconds = -i64::try_from(before.as_secs()).unwrap_or(i64::MAX);
                let nanos = before.subsec_nanos();
                if nanos == 0 {
                    Self::from_unix(seconds, 0)
                } else {
                    Self::from_unix(seconds - 1, NANOS_PER_SECOND - nanos)
                }
            }
        }
    }
}

impl TryFrom<Datetime> for SystemTime {
    type Error = DatetimeError;

    /// Fails only for instants the platform's `SystemTime` cannot represent.
    fn try_from(value: Datetime) -> Result<Self, DatetimeError> {
        let out_of_range = DatetimeError("out of range for SystemTime");
        let nanos = Duration::from_nanos(u64::from(value.nsec));
        let seconds = Duration::from_secs(value.epoch.unsigned_abs());
        if value.epoch >= 0 {
            UNIX_EPOCH.checked_add(seconds + nanos).ok_or(out_of_range)
        } else {
            UNIX_EPOCH.checked_sub(seconds).and_then(|t| t.checked_add(nanos)).ok_or(out_of_range)
        }
    }
}

impl Serialize for Datetime {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        ext::serialize(DATETIME, &(*self).to_payload(), s)
    }
}

impl<'de> Deserialize<'de> for Datetime {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let payload = ext::deserialize_tagged(d, DATETIME, "a datetime")?;
        Self::from_payload(&payload).map_err(serde::de::Error::custom)
    }
}

impl From<Datetime> for rmpv::Value {
    fn from(value: Datetime) -> Self {
        Self::Ext(DATETIME, value.to_payload())
    }
}

impl TryFrom<&rmpv::Value> for Datetime {
    type Error = DatetimeError;

    fn try_from(value: &rmpv::Value) -> Result<Self, DatetimeError> {
        match value {
            rmpv::Value::Ext(DATETIME, payload) => Self::from_payload(payload),
            _ => Err(DatetimeError("not an MP_DATETIME value")),
        }
    }
}

#[cfg(feature = "time")]
impl From<time::OffsetDateTime> for Datetime {
    fn from(value: time::OffsetDateTime) -> Self {
        Self::from_unix(value.unix_timestamp(), value.nanosecond())
            .with_tz_offset(value.offset().whole_minutes())
    }
}

#[cfg(feature = "time")]
impl TryFrom<Datetime> for time::OffsetDateTime {
    type Error = DatetimeError;

    fn try_from(value: Datetime) -> Result<Self, DatetimeError> {
        let nanos = i128::from(value.epoch) * i128::from(NANOS_PER_SECOND) + i128::from(value.nsec);
        let utc = Self::from_unix_timestamp_nanos(nanos)
            .map_err(|_| DatetimeError("out of range for time::OffsetDateTime"))?;
        let offset = time::UtcOffset::from_whole_seconds(i32::from(value.tz_offset) * 60)
            .map_err(|_| DatetimeError("offset out of range for time::UtcOffset"))?;
        Ok(utc.to_offset(offset))
    }
}

#[cfg(feature = "chrono")]
impl<Tz: chrono::TimeZone> From<chrono::DateTime<Tz>> for Datetime {
    fn from(value: chrono::DateTime<Tz>) -> Self {
        use chrono::Offset;
        let minutes = value.offset().fix().local_minus_utc() / 60;
        Self::from_unix(value.timestamp(), value.timestamp_subsec_nanos())
            .with_tz_offset(i16::try_from(minutes).unwrap_or(0))
    }
}

#[cfg(feature = "chrono")]
impl TryFrom<Datetime> for chrono::DateTime<chrono::FixedOffset> {
    type Error = DatetimeError;

    fn try_from(value: Datetime) -> Result<Self, DatetimeError> {
        let utc = chrono::DateTime::<chrono::Utc>::from_timestamp(value.epoch, value.nsec)
            .ok_or(DatetimeError("out of range for chrono::DateTime"))?;
        let offset = chrono::FixedOffset::east_opt(i32::from(value.tz_offset) * 60)
            .ok_or(DatetimeError("offset out of range for chrono::FixedOffset"))?;
        Ok(utc.with_timezone(&offset))
    }
}

#[cfg(feature = "jiff")]
impl From<jiff::Timestamp> for Datetime {
    fn from(value: jiff::Timestamp) -> Self {
        let (mut seconds, mut nanos) = (value.as_second(), value.subsec_nanosecond());
        if nanos < 0 {
            seconds -= 1;
            nanos += i32::try_from(NANOS_PER_SECOND).expect("fits");
        }
        Self::from_unix(seconds, u32::try_from(nanos).expect("0..1e9"))
    }
}

#[cfg(feature = "jiff")]
impl From<&jiff::Zoned> for Datetime {
    fn from(value: &jiff::Zoned) -> Self {
        let minutes = value.offset().seconds() / 60;
        Self::from(value.timestamp()).with_tz_offset(i16::try_from(minutes).unwrap_or(0))
    }
}

#[cfg(feature = "jiff")]
impl From<jiff::Zoned> for Datetime {
    fn from(value: jiff::Zoned) -> Self {
        Self::from(&value)
    }
}

#[cfg(feature = "jiff")]
impl TryFrom<Datetime> for jiff::Timestamp {
    type Error = DatetimeError;

    fn try_from(value: Datetime) -> Result<Self, DatetimeError> {
        Self::new(value.epoch, i32::try_from(value.nsec).expect("0..1e9"))
            .map_err(|_| DatetimeError("out of range for jiff::Timestamp"))
    }
}

#[cfg(feature = "jiff")]
impl TryFrom<Datetime> for jiff::Zoned {
    type Error = DatetimeError;

    /// A `Zoned` in the fixed offset the value carries.
    fn try_from(value: Datetime) -> Result<Self, DatetimeError> {
        let timestamp = jiff::Timestamp::try_from(value)?;
        let offset = jiff::tz::Offset::from_seconds(i32::from(value.tz_offset) * 60)
            .map_err(|_| DatetimeError("offset out of range for jiff::tz::Offset"))?;
        Ok(timestamp.to_zoned(jiff::tz::TimeZone::fixed(offset)))
    }
}

/// The payload or conversion was not a valid datetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatetimeError(&'static str);

impl fmt::Display for DatetimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid datetime: {}", self.0)
    }
}

impl std::error::Error for DatetimeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_the_short_form_for_whole_utc_seconds() {
        let moment = Datetime::from_unix(1_700_000_000, 0);
        let bytes = rmp_serde::to_vec(&moment).unwrap();
        // fixext 8, type 4, then the seconds little-endian.
        assert_eq!(bytes[..2], [0xd7, 0x04]);
        assert_eq!(bytes[2..], 1_700_000_000i64.to_le_bytes());
        assert_eq!(rmp_serde::from_slice::<Datetime>(&bytes).unwrap(), moment);
    }

    #[test]
    fn encodes_the_long_form_with_nanoseconds_and_zone() {
        let moment = Datetime::from_unix(-1, 999_999_999).with_tz_offset(-330).with_tz_index(7);
        let bytes = rmp_serde::to_vec(&moment).unwrap();
        assert_eq!(bytes[..2], [0xd8, 0x04]);
        assert_eq!(bytes.len(), 18);
        let back: Datetime = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(back, moment);
        assert_eq!(back.tz_offset_minutes(), -330);
        assert_eq!(back.tz_index(), 7);
        assert_eq!(back.nanosecond(), 999_999_999);
    }

    #[test]
    fn displays_rfc3339_at_the_carried_offset() {
        assert_eq!(Datetime::UNIX_EPOCH.to_string(), "1970-01-01T00:00:00Z");
        assert_eq!(Datetime::from_unix(-1, 0).to_string(), "1969-12-31T23:59:59Z");
        assert_eq!(
            Datetime::from_unix(951_782_400, 0).to_string(),
            "2000-02-29T00:00:00Z",
            "leap day"
        );
        assert_eq!(
            Datetime::from_unix(1_700_000_000, 123_000_000).with_tz_offset(-90).to_string(),
            "2023-11-14T20:43:20.123-01:30"
        );
    }

    #[test]
    fn system_time_round_trips_on_both_sides_of_the_epoch() {
        for (secs, nanos) in [(0, 0), (1_700_000_000, 42), (-1, 0), (-2, 999_999_999)] {
            let moment = Datetime::from_unix(secs, nanos);
            let system = SystemTime::try_from(moment).unwrap();
            assert_eq!(Datetime::from(system), moment, "{secs}.{nanos}");
        }
        assert_eq!(Datetime::from_unix(0, 1_500_000_000), Datetime::from_unix(1, 500_000_000));
    }

    #[test]
    fn rejects_malformed_payloads() {
        assert!(Datetime::from_payload(&[0; 7]).is_err());
        assert!(Datetime::from_payload(&[0; 12]).is_err());
        let mut too_many_nanos = [0u8; 16];
        too_many_nanos[8..12].copy_from_slice(&NANOS_PER_SECOND.to_le_bytes());
        assert!(Datetime::from_payload(&too_many_nanos).is_err());
    }
}
