//! An async [Tarantool](https://www.tarantool.io) client that speaks the
//! binary protocol natively, is typed end to end, and stays out of your way.
//!
//! ```no_run
//! use serde::{Deserialize, Serialize};
//! use tarant::{Client, Iter, Update};
//!
//! #[derive(Serialize, Deserialize)]
//! struct User {
//!     id: u64,
//!     name: String,
//!     age: u32,
//! }
//!
//! # async fn demo() -> tarant::Result<()> {
//! let client = Client::connect("tarantool://app:secret@127.0.0.1:3301").await?;
//! let users = client.space::<User>("users");
//!
//! users.insert(&User { id: 1, name: "ann".into(), age: 30 }).await?;
//!
//! let ann: Option<User> = users.get(1).await?;
//! let adults: Vec<User> = users.index("age").select(18).iterator(Iter::Ge).limit(100).await?;
//!
//! users.update(1, Update::new().set(3, 31)).await?;
//! # Ok(()) }
//! ```
//!
//! # How it fits together
//!
//! * **One connection, many requests.** A [`Client`] is a cheap handle onto a
//!   background task that owns the socket. Requests are pipelined and matched
//!   to replies by their `sync`, so concurrent callers never queue behind one
//!   another. Cloning a `Client` shares that connection.
//! * **Types at the boundary.** A tuple is anything `serde` turns into a
//!   `MessagePack` array: a struct, a tuple, a `Vec`. Keys ([`Key`]), call
//!   arguments ([`Args`]) and field operations ([`Update`]) are checked by the
//!   compiler before a byte reaches the wire.
//! * **Errors you can branch on.** A server rejection arrives as
//!   [`ServerError`] carrying the numeric [`ErrorCode`] and the full error
//!   stack — match on `ErrorCode::TUPLE_FOUND`, never on a message string.
//! * **Reconnects that keep their promises.** A dropped link is re-established
//!   with backoff, the handshake replayed and every [`Watcher`] re-subscribed.
//!   Requests that were in flight fail with [`Error::Closed`], because the
//!   client cannot know whether the server ran them.
//! * **Nothing unsafe, nothing hidden.** `#![forbid(unsafe_code)]`, every
//!   public item documented, no panics on user input.
//!
//! # Feature flags
//!
//! * `uuid` — accept `uuid::Uuid` directly as a key field.
//!
//! # Compatibility
//!
//! Targets Tarantool 3.0 and later, where spaces and indexes are addressed by
//! name and no schema fetch is needed. Against 2.10–2.11 everything works
//! except name addressing; [`Client::server_info`] reports what the handshake
//! negotiated.

#![forbid(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

mod auth;
mod client;
mod codec;
mod connection;
mod error;
mod iproto;
mod msgpack;
mod options;
mod tuple;
mod update;
mod watcher;

pub use client::{Client, Index, Page, Paged, Select, ServerInfo, Space, Stream, TxOptions};
pub use error::{Error, ErrorCode, Result, ServerError};
pub use iproto::{Feature, Isolation, Iter};
pub use options::{ConnectOptions, DEFAULT_PORT, Reconnect};
pub use tuple::{Args, Key};
pub use update::{FieldRef, Update};
pub use watcher::Watcher;

/// A dynamically typed `MessagePack` value, re-exported from [`rmpv`].
///
/// Use it where the tuple shape is not known at compile time — a generic
/// admin tool, a migration script — and a concrete `struct` everywhere else.
pub use rmpv::Value;

/// The examples in `README.md`, compiled and run as doctests so the front
/// page cannot drift from the API it advertises.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
