//! Wire-level vocabulary of the iproto protocol.
//!
//! Everything here mirrors `iproto_constants.h`, `iproto_features.h` and
//! `mp_extension_types.h` in the Tarantool sources. The numbers are the
//! protocol; the names are ours. Nothing in this module allocates or does
//! I/O — it is the dictionary the codec speaks.

/// Keys of the request/response header and body maps.
///
/// The full client-facing set is listed even where this client has no use
/// for a key yet: the module is the protocol's vocabulary, and a missing
/// constant is how a future request type gets encoded wrong.
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
    /// SQL: statement options. A map, in practice always empty.
    pub const OPTIONS: u8 = 0x2b;
    pub const AFTER_POSITION: u8 = 0x2e;
    pub const AFTER_TUPLE: u8 = 0x2f;
    pub const DATA: u8 = 0x30;
    pub const ERROR_24: u8 = 0x31;
    /// SQL: column descriptions of a result set.
    pub const METADATA: u8 = 0x32;
    /// SQL: parameter descriptions of a prepared statement.
    pub const BIND_METADATA: u8 = 0x33;
    /// SQL: number of parameters a prepared statement takes.
    pub const BIND_COUNT: u8 = 0x34;
    pub const POSITION: u8 = 0x35;
    /// An Arrow IPC stream (`MP_ARROW`) for `IPROTO_INSERT_ARROW`.
    pub const ARROW: u8 = 0x36;
    pub const SQL_TEXT: u8 = 0x40;
    pub const SQL_BIND: u8 = 0x41;
    pub const SQL_INFO: u8 = 0x42;
    pub const STMT_ID: u8 = 0x43;
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
    /// Since Tarantool 3.1: the formats of `MP_TUPLE`s in a response.
    pub const TUPLE_FORMATS: u8 = 0x60;
    /// Since Tarantool 3.3: make a transaction synchronous.
    pub const IS_SYNC: u8 = 0x61;
}

/// Keys nested inside `IPROTO_METADATA` entries and `IPROTO_SQL_INFO`.
pub(crate) mod sql {
    pub const FIELD_NAME: u8 = 0x00;
    pub const FIELD_TYPE: u8 = 0x01;
    pub const FIELD_COLL: u8 = 0x02;
    pub const FIELD_IS_NULLABLE: u8 = 0x03;
    pub const FIELD_IS_AUTOINCREMENT: u8 = 0x04;
    pub const FIELD_SPAN: u8 = 0x05;
    pub const INFO_ROW_COUNT: u8 = 0x00;
    pub const INFO_AUTOINCREMENT_IDS: u8 = 0x01;
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
    pub const EXECUTE: u64 = 0x0b;
    pub const PREPARE: u64 = 0x0d;
    pub const BEGIN: u64 = 0x0e;
    pub const COMMIT: u64 = 0x0f;
    pub const ROLLBACK: u64 = 0x10;
    pub const INSERT_ARROW: u64 = 0x11;
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
#[allow(dead_code)]
pub(crate) mod ext {
    pub const DECIMAL: i8 = 1;
    pub const UUID: i8 = 2;
    pub const ERROR: i8 = 3;
    pub const DATETIME: i8 = 4;
    /// Enterprise Edition only.
    pub const COMPRESSION: i8 = 5;
    pub const INTERVAL: i8 = 6;
    /// A tuple with its format id; sent only to clients that ask for it.
    pub const TUPLE: i8 = 7;
    pub const ARROW: i8 = 8;
}

/// The protocol version this client speaks; sent in `IPROTO_ID`.
pub(crate) const PROTOCOL_VERSION: u64 = 10;

/// Protocol features, as listed in `iproto_features.h`.
///
/// The handshake works in both directions: the client announces the
/// features it implements ([`Feature::ANNOUNCED`]), and the server replies
/// with everything *it* implements. [`ServerInfo::supports`] reports the
/// server's side, so it can be true for a feature this client does not use.
///
/// [`ServerInfo::supports`]: crate::ServerInfo::supports
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
    /// Cursor pagination: `AFTER_POSITION`, `AFTER_TUPLE`, `FETCH_POSITION`.
    Pagination,
    /// Spaces and indexes may be addressed by name instead of id.
    SpaceAndIndexNames,
    /// `IPROTO_WATCH_ONCE`: read a broadcast key without subscribing.
    WatchOnce,
    /// DML responses may carry tuples as `MP_TUPLE` with their formats.
    ///
    /// Not announced by this client: a format id only means something to a
    /// `box.tuple` object, and a typed client decodes rows into your types.
    DmlTupleExtension,
    /// `call`/`eval` results may carry `MP_TUPLE`s. Not announced; see
    /// [`DmlTupleExtension`](Self::DmlTupleExtension).
    CallRetTupleExtension,
    /// `call`/`eval` arguments may carry `MP_TUPLE`s. Not announced; see
    /// [`DmlTupleExtension`](Self::DmlTupleExtension).
    CallArgTupleExtension,
    /// `FETCH_SNAPSHOT` with a cursor. Replication only; not announced.
    FetchSnapshotCursor,
    /// `IPROTO_IS_SYNC` on `BEGIN`/`COMMIT`: synchronous transactions.
    IsSync,
    /// `IPROTO_INSERT_ARROW`: batch insertion of an Arrow IPC stream.
    InsertArrow,
}

impl Feature {
    /// What this client announces in `IPROTO_ID`: every feature it implements.
    pub const ANNOUNCED: [Self; 9] = [
        Self::Streams,
        Self::Transactions,
        Self::ErrorExtension,
        Self::Watchers,
        Self::Pagination,
        Self::SpaceAndIndexNames,
        Self::WatchOnce,
        Self::IsSync,
        Self::InsertArrow,
    ];

    const ALL: [Self; 13] = [
        Self::Streams,
        Self::Transactions,
        Self::ErrorExtension,
        Self::Watchers,
        Self::Pagination,
        Self::SpaceAndIndexNames,
        Self::WatchOnce,
        Self::DmlTupleExtension,
        Self::CallRetTupleExtension,
        Self::CallArgTupleExtension,
        Self::FetchSnapshotCursor,
        Self::IsSync,
        Self::InsertArrow,
    ];

    /// The feature's number on the wire.
    pub const fn code(self) -> u64 {
        match self {
            Self::Streams => 0,
            Self::Transactions => 1,
            Self::ErrorExtension => 2,
            Self::Watchers => 3,
            Self::Pagination => 4,
            Self::SpaceAndIndexNames => 5,
            Self::WatchOnce => 6,
            Self::DmlTupleExtension => 7,
            Self::CallRetTupleExtension => 8,
            Self::CallArgTupleExtension => 9,
            Self::FetchSnapshotCursor => 10,
            Self::IsSync => 11,
            Self::InsertArrow => 12,
        }
    }

    /// The feature with this number, if the client knows it.
    pub fn from_code(code: u64) -> Option<Self> {
        Self::ALL.into_iter().find(|feature| feature.code() == code)
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
