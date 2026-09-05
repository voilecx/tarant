//! What can go wrong, and how to tell the cases apart.
//!
//! There is one [`Error`] type for the whole crate. Its variants answer the
//! question a caller actually has — *did the server reject this, or did the
//! network fail, or did I encode something the server cannot read?* — rather
//! than mirroring internal layers. A server-side rejection carries the full
//! [`ServerError`] Tarantool sent, including the code from `errcode.h` and
//! the error stack, so a caller can branch on `err.code()` instead of
//! parsing messages.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use rmpv::Value;

/// A specialised `Result` for tarant operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Every failure a tarant operation can produce.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The socket failed underneath us.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// The server rejected the request. Inspect [`ServerError::code`].
    ///
    /// Boxed so that `Result<T, Error>` stays small: a rejection carries a
    /// message, an error class and a whole stack, and every call in this
    /// crate would otherwise pay for that in its return value.
    #[error(transparent)]
    Server(Box<ServerError>),

    /// The credentials were not accepted.
    #[error("authentication failed for user `{user}`: {source}")]
    Auth {
        /// The user name that was presented.
        user: String,
        /// What the server said.
        #[source]
        source: Box<ServerError>,
    },

    /// Bytes arrived that are not a well-formed iproto packet.
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// A request argument could not be turned into `MessagePack`.
    #[error("failed to encode request: {0}")]
    Encode(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// The response was valid `MessagePack`, but not the shape the caller asked for.
    #[error("failed to decode response: {0}")]
    Decode(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),

    /// The connection is gone and will not come back for this request.
    #[error("connection closed")]
    Closed,

    /// The server announced it is shutting down; no new requests are accepted.
    #[error("server is shutting down")]
    ShuttingDown,

    /// The request did not complete within the configured deadline.
    #[error("request timed out after {0:?}")]
    Timeout(Duration),

    /// The connection URL could not be understood.
    #[error("invalid connection url: {0}")]
    Url(String),

    /// The server did not negotiate a feature this operation needs.
    #[error("server does not support {0}")]
    Unsupported(&'static str),
}

impl From<ServerError> for Error {
    fn from(err: ServerError) -> Self {
        Self::Server(Box::new(err))
    }
}

impl Error {
    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }

    pub(crate) fn encode(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Encode(Box::new(source))
    }

    pub(crate) fn decode(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Decode(Box::new(source))
    }

    /// The server-side error, if that is what this is.
    ///
    /// Covers both [`Error::Server`] and the error inside [`Error::Auth`].
    pub fn as_server(&self) -> Option<&ServerError> {
        match self {
            Self::Server(err) | Self::Auth { source: err, .. } => Some(err),
            _ => None,
        }
    }

    /// Whether retrying the same request on a fresh connection could succeed.
    ///
    /// True for transport failures and timeouts, false for anything the
    /// server decided on purpose (bad arguments, access denied, conflicts).
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::Io(_) | Self::Closed | Self::Timeout(_))
    }
}

/// An error raised by the server, decoded from the `MP_ERROR` extension.
///
/// The `Display` form is the message alone — what you would see in the
/// Tarantool console. The rest is there when you need to branch or log.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ServerError {
    /// The numeric code from `errcode.h`. See [`ErrorCode`].
    pub code: ErrorCode,
    /// Human-readable reason.
    pub message: String,
    /// Tarantool's error class, e.g. `ClientError`, `AccessDeniedError`, `LuajitError`.
    pub kind: String,
    /// Source file the error was raised in, when the server reports one.
    pub file: Option<String>,
    /// Line in that file.
    pub line: Option<u64>,
    /// OS `errno` for `SystemError`s; zero otherwise.
    pub errno: u64,
    /// Extra fields specific to the error class (`object_type`, `access_type`, …).
    pub fields: BTreeMap<String, Value>,
    /// Errors this one was raised from, outermost first (`error.prev` chain).
    pub cause: Vec<Self>,
}

impl ServerError {
    /// The numeric error code.
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    pub(crate) fn from_message(code: u32, message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode(code),
            message: message.into(),
            kind: String::from("ClientError"),
            file: None,
            line: None,
            errno: 0,
            fields: BTreeMap::new(),
            cause: Vec::new(),
        }
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ServerError {}

/// A Tarantool error code (`box.error.*`, `errcode.h`).
///
/// The constants below are the ones a client is most likely to branch on.
/// Any other code is still representable; match on `.0` or use
/// [`ErrorCode::as_u32`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorCode(pub u32);

impl ErrorCode {
    /// `ER_UNKNOWN`: an error without a more specific code.
    pub const UNKNOWN: Self = Self(0);
    /// `ER_ILLEGAL_PARAMS`: the request is malformed or its arguments are wrong.
    pub const ILLEGAL_PARAMS: Self = Self(1);
    /// `ER_UNSUPPORTED`: the engine or edition does not support the operation.
    pub const UNSUPPORTED: Self = Self(5);
    /// `ER_TUPLE_FOUND`: `insert` hit an existing key.
    pub const TUPLE_FOUND: Self = Self(3);
    /// `ER_TUPLE_NOT_FOUND`: `update`/`delete` matched nothing.
    pub const TUPLE_NOT_FOUND: Self = Self(4);
    /// `ER_READONLY`: the instance is read-only.
    pub const READONLY: Self = Self(7);
    /// `ER_SPACE_EXISTS`.
    pub const SPACE_EXISTS: Self = Self(10);
    /// `ER_FIELD_TYPE`: a tuple field does not match the space format.
    pub const FIELD_TYPE: Self = Self(23);
    /// `ER_PROC_LUA`: a Lua error inside `call`/`eval`.
    pub const PROC_LUA: Self = Self(32);
    /// `ER_NO_SUCH_PROC`: `call` named a function that does not exist.
    pub const NO_SUCH_PROC: Self = Self(33);
    /// `ER_NO_SUCH_INDEX_ID`.
    pub const NO_SUCH_INDEX: Self = Self(35);
    /// `ER_NO_SUCH_SPACE`.
    pub const NO_SUCH_SPACE: Self = Self(36);
    /// `ER_ACCESS_DENIED`.
    pub const ACCESS_DENIED: Self = Self(42);
    /// `ER_NO_SUCH_USER`.
    pub const NO_SUCH_USER: Self = Self(45);
    /// `ER_CREDS_MISMATCH`: wrong password.
    pub const CREDS_MISMATCH: Self = Self(47);
    /// `ER_TIMEOUT`.
    pub const TIMEOUT: Self = Self(78);
    /// `ER_TRANSACTION_CONFLICT`: MVCC conflict, the transaction should be retried.
    pub const TRANSACTION_CONFLICT: Self = Self(97);
    /// `ER_NO_SUCH_FIELD_NAME_IN_SPACE`: a JSON path or update named an unknown field.
    pub const NO_SUCH_FIELD_NAME_IN_SPACE: Self = Self(153);
    /// `ER_SQL_EXECUTE`: the SQL statement failed while running.
    pub const SQL_EXECUTE: Self = Self(159);
    /// `ER_SQL_PREPARE`: the SQL statement could not be parsed or planned.
    pub const SQL_PREPARE: Self = Self(210);
    /// `ER_WRONG_QUERY_ID`: the prepared statement is gone (the session that made it ended).
    pub const WRONG_QUERY_ID: Self = Self(211);
    /// `ER_SYNC_QUORUM_TIMEOUT`: a synchronous transaction did not reach a quorum in time.
    pub const SYNC_QUORUM_TIMEOUT: Self = Self(216);

    /// The raw number.
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ER_{}", self.0)
    }
}

impl From<u32> for ErrorCode {
    fn from(code: u32) -> Self {
        Self(code)
    }
}
