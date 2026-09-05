//! Wire-level vocabulary of the iproto protocol.
//!
//! Everything here mirrors `src/box/iproto_constants.h` in the Tarantool
//! sources. The numbers are the protocol; the names are ours. Nothing in this
//! module allocates or does I/O — it is the dictionary the codec speaks.

/// Keys of the request/response header and body maps.
///
/// The full set is listed even where this client has no use for a key yet:
/// the module is the protocol's vocabulary, and a missing constant is how a
/// future request type gets encoded wrong.
#[allow(dead_code)]
pub(crate) mod key {
    pub const REQUEST_TYPE: u8 = 0x00;
    pub const SYNC: u8 = 0x01;
    pub const SCHEMA_VERSION: u8 = 0x05;
    pub const STREAM_ID: u8 = 0x0a;
    pub const SPACE_ID: u8 = 0x10;
    pub const INDEX_ID: u8 = 0x11;
    pub const LIMIT: u8 = 0x12;
    pub const OFFSET: u8 = 0x13;
    pub const ITERATOR: u8 = 0x14;
    pub const INDEX_BASE: u8 = 0x15;
    pub const FETCH_POSITION: u8 = 0x1f;
    pub const KEY: u8 = 0x20;
    pub const TUPLE: u8 = 0x21;
    pub const FUNCTION_NAME: u8 = 0x22;
    pub const USER_NAME: u8 = 0x23;
    pub const EXPR: u8 = 0x27;
    pub const OPS: u8 = 0x28;
    pub const AFTER_POSITION: u8 = 0x2e;
    pub const AFTER_TUPLE: u8 = 0x2f;
    pub const DATA: u8 = 0x30;
    pub const ERROR_24: u8 = 0x31;
    pub const POSITION: u8 = 0x35;
    pub const ERROR: u8 = 0x52;
    pub const VERSION: u8 = 0x54;
    pub const FEATURES: u8 = 0x55;
    pub const TIMEOUT: u8 = 0x56;
    pub const EVENT_KEY: u8 = 0x57;
    pub const EVENT_DATA: u8 = 0x58;
    pub const TXN_ISOLATION: u8 = 0x59;
    pub const AUTH_TYPE: u8 = 0x5b;
    /// Since Tarantool 3.0: address a space by name, no schema round-trip.
    pub const SPACE_NAME: u8 = 0x5e;
    /// Since Tarantool 3.0: address an index by name.
    pub const INDEX_NAME: u8 = 0x5f;
}

/// Request types (the value under [`key::REQUEST_TYPE`] in a request header).
pub(crate) mod request {
    pub const SELECT: u64 = 0x01;
    pub const INSERT: u64 = 0x02;
    pub const REPLACE: u64 = 0x03;
    pub const UPDATE: u64 = 0x04;
    pub const DELETE: u64 = 0x05;
    pub const AUTH: u64 = 0x07;
    pub const EVAL: u64 = 0x08;
    pub const UPSERT: u64 = 0x09;
    pub const CALL: u64 = 0x0a;
    pub const BEGIN: u64 = 0x0e;
    pub const COMMIT: u64 = 0x0f;
    pub const ROLLBACK: u64 = 0x10;
    pub const PING: u64 = 0x40;
    pub const ID: u64 = 0x49;
    pub const WATCH: u64 = 0x4a;
    pub const UNWATCH: u64 = 0x4b;
    pub const WATCH_ONCE: u64 = 0x4d;
}

/// Response types (the value under [`key::REQUEST_TYPE`] in a response header).
pub(crate) mod response {
    pub const OK: u64 = 0x00;
    /// An out-of-band chunk produced by `box.session.push()`.
    pub const CHUNK: u64 = 0x80;
    /// Pushed by the server for a watched key; carries no sync.
    pub const EVENT: u64 = 0x4c;
    /// Errors are `0x8000 | code`, where `code` is from `errcode.h`.
    pub const ERROR_FLAG: u64 = 0x8000;
}

/// Keys of the `MP_ERROR` extension payload and of each frame in its stack.
pub(crate) mod error {
    pub const STACK: u8 = 0x00;
    pub const TYPE: u8 = 0x00;
    pub const FILE: u8 = 0x01;
    pub const LINE: u8 = 0x02;
    pub const MESSAGE: u8 = 0x03;
    pub const ERRNO: u8 = 0x04;
    pub const ERRCODE: u8 = 0x05;
    pub const FIELDS: u8 = 0x06;
}

/// `MessagePack` extension type tags Tarantool defines.
///
/// Only [`ERROR`](ext::ERROR) is decoded today; the rest are here so the
/// numbers live in one place when `DECIMAL`, `UUID` and `DATETIME` gain
/// first-class support.
#[allow(dead_code)]
pub(crate) mod ext {
    pub const DECIMAL: i8 = 1;
    pub const UUID: i8 = 2;
    pub const ERROR: i8 = 3;
    pub const DATETIME: i8 = 4;
    pub const INTERVAL: i8 = 6;
}

/// The protocol version this client speaks; sent in `IPROTO_ID`.
pub(crate) const PROTOCOL_VERSION: u64 = 6;

/// Protocol features negotiated with `IPROTO_ID`.
///
/// The client announces the features it implements; the server answers with
/// the intersection it supports. Anything not in the answer is off for the
/// life of the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Feature {
    /// `IPROTO_STREAM_ID` in request headers: ordered request groups.
    Streams,
    /// `IPROTO_BEGIN` / `COMMIT` / `ROLLBACK` inside a stream.
    Transactions,
    /// Errors arrive as the `MP_ERROR` extension, not just a string.
    ErrorExtension,
    /// `IPROTO_WATCH` / `UNWATCH` / `EVENT`: server-pushed notifications.
    Watchers,
    /// `IPROTO_WATCH_ONCE`: read a broadcast key without subscribing.
    WatchOnce,
    /// Spaces and indexes may be addressed by name instead of id.
    SpaceAndIndexNames,
}

impl Feature {
    /// Every feature this client implements, in announcement order.
    pub(crate) const SUPPORTED: [Self; 6] = [
        Self::Streams,
        Self::Transactions,
        Self::ErrorExtension,
        Self::Watchers,
        Self::WatchOnce,
        Self::SpaceAndIndexNames,
    ];

    pub(crate) const fn code(self) -> u64 {
        match self {
            Self::Streams => 0,
            Self::Transactions => 1,
            Self::ErrorExtension => 2,
            Self::Watchers => 3,
            Self::WatchOnce => 4,
            Self::SpaceAndIndexNames => 5,
        }
    }

    pub(crate) fn from_code(code: u64) -> Option<Self> {
        Self::SUPPORTED.into_iter().find(|feature| feature.code() == code)
    }
}

/// How `select` walks an index.
///
/// The numeric values are Tarantool's `iterator_type.h`. Which iterators an
/// index accepts depends on its type: TREE takes all of the comparison ones,
/// HASH takes `Eq`/`All`, BITSET takes the `Bits*` family, RTREE takes
/// `Overlaps`/`Neighbor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Iter {
    /// Equal to the key. With a partial key, matches on the given prefix.
    #[default]
    Eq,
    /// Equal to the key, walked in reverse.
    Req,
    /// Every tuple, key ignored.
    All,
    /// Less than.
    Lt,
    /// Less than or equal.
    Le,
    /// Greater than or equal.
    Ge,
    /// Greater than.
    Gt,
    /// BITSET: all bits of the value are set in the key.
    BitsAllSet,
    /// BITSET: at least one bit is set.
    BitsAnySet,
    /// BITSET: no bits are set.
    BitsAllNotSet,
    /// RTREE: overlaps the rectangle or box.
    Overlaps,
    /// RTREE: nearest neighbours of the point.
    Neighbor,
}

impl Iter {
    pub(crate) const fn code(self) -> u64 {
        match self {
            Self::Eq => 0,
            Self::Req => 1,
            Self::All => 2,
            Self::Lt => 3,
            Self::Le => 4,
            Self::Ge => 5,
            Self::Gt => 6,
            Self::BitsAllSet => 7,
            Self::BitsAnySet => 8,
            Self::BitsAllNotSet => 9,
            Self::Overlaps => 10,
            Self::Neighbor => 11,
        }
    }
}

/// Isolation level of a stream transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Isolation {
    /// Whatever `box.cfg.txn_isolation` says on the server.
    #[default]
    Default,
    /// See changes that are committed but not yet confirmed by a quorum.
    ReadCommitted,
    /// See only confirmed changes.
    ReadConfirmed,
    /// Let the server pick per transaction.
    BestEffort,
}

impl Isolation {
    pub(crate) const fn code(self) -> u64 {
        match self {
            Self::Default => 0,
            Self::ReadCommitted => 1,
            Self::ReadConfirmed => 2,
            Self::BestEffort => 3,
        }
    }
}
