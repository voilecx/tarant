//! Tarantool's extension value types: [`Decimal`], [`Uuid`], [`Datetime`], [`Interval`].
//!
//! Tarantool carries these as `MessagePack` extensions rather than as
//! strings or numbers, and a field declared `decimal`, `uuid`, `datetime` or
//! `interval` accepts nothing else. Each type here encodes itself as the
//! right extension through `serde`, so it can sit in a tuple `struct` with
//! no attributes, be used as a [`Key`](crate::Key), or be pulled out of a
//! dynamic [`Value`](crate::Value) with `TryFrom`.
//!
//! Every type is self-contained. Cargo features add conversions to the
//! crates you already use: `uuid`, `rust_decimal`, `time`, `chrono`, `jiff`.

mod datetime;
mod decimal;
mod ext;
mod interval;
mod uuid;

pub use datetime::{Datetime, DatetimeError};
pub use decimal::{Decimal, DecimalError};
pub use interval::{Adjust, Interval, IntervalError};
pub use uuid::{Uuid, UuidError};
