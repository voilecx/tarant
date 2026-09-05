# tarant

An async Rust client for [Tarantool](https://www.tarantool.io), built on
[tokio](https://tokio.rs). It speaks the binary protocol natively, is typed
end to end, and stays out of your way.

[![crates.io](https://img.shields.io/crates/v/tarant.svg)](https://crates.io/crates/tarant)
[![docs.rs](https://img.shields.io/docsrs/tarant)](https://docs.rs/tarant)
[![CI](https://github.com/voilecx/tarant/actions/workflows/ci.yml/badge.svg)](https://github.com/voilecx/tarant/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/crates/msrv/tarant)](#compatibility)
[![license](https://img.shields.io/crates/l/tarant.svg)](#license)

```toml
[dependencies]
tarant = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

```rust
use serde::{Deserialize, Serialize};
use tarant::{Client, Iter, Update};

#[derive(Serialize, Deserialize)]
struct User {
    id: u64,
    name: String,
    age: u32,
}

async fn example() -> tarant::Result<()> {
    let client = Client::connect("tarantool://app:secret@127.0.0.1:3301").await?;
    let users = client.space::<User>("users");

    users.insert(&User { id: 1, name: "ann".into(), age: 30 }).await?;

    let ann: Option<User> = users.get(1).await?;
    let adults: Vec<User> = users.index("age").select(18).iterator(Iter::Ge).limit(100).await?;

    users.update(1, Update::new().add(3, 1)).await?;
    Ok(())
}
```

## Why another client

The existing Rust connectors either target the C API from inside a Tarantool
process, or stopped at Tarantool 2.10 and never picked up streams, watchers or
name-addressed spaces. `tarant` is written against the 3.x protocol and the
`tokio` ecosystem, with the API surface a service actually needs.

- **One connection, many requests.** A `Client` is a cheap handle onto a
  background task that owns the socket. Requests are pipelined and matched to
  replies by their `sync`, so concurrent callers never queue behind one
  another. Clone the `Client` instead of opening more connections.
- **Types at the boundary.** A tuple is anything `serde` turns into a
  MessagePack array: a struct, a tuple, a `Vec`. Keys, call arguments and
  update operations are checked by the compiler before a byte reaches the wire.
- **Errors you can branch on.** A rejection arrives as `ServerError` carrying
  the numeric `ErrorCode` and the full error stack. Match on
  `ErrorCode::TUPLE_FOUND`, never on a message string.
- **Reconnects that keep their promises.** A dropped link is re-established
  with backoff, the handshake replayed and every watcher re-subscribed.
  Requests that were in flight fail with `Error::Closed`, because the client
  cannot know whether the server ran them.
- **Nothing unsafe, nothing hidden.** `#![forbid(unsafe_code)]`, every public
  item documented, zero `clippy::pedantic` and `clippy::nursery` warnings.

## What it covers

| Area | Support |
|---|---|
| CRUD | `insert`, `replace`, `upsert`, `get`, `select`, `update`, `delete`, typed through `serde` |
| Indexes | primary and secondary by name, every iterator type |
| Pagination | cursor-based (`IPROTO_POSITION`), correct under concurrent writes |
| Procedures | `call` and `eval` with typed arguments and returns |
| Transactions | streams, `begin`/`commit`/`rollback`, isolation levels, server-side timeout, rollback on drop |
| Watchers | `box.broadcast` subscriptions that survive reconnects, plus `watch_once` |
| Protocol | feature negotiation, `chap-sha1` auth, graceful-shutdown handling, `MP_ERROR` stacks |

## Cursor pagination

Walking a whole space with `offset` is wrong the moment anything else writes
to it. `page()` returns a cursor the server resolves against the index, so
rows are neither skipped nor repeated:

```rust
use serde::Deserialize;
use tarant::{Client, Iter};

#[derive(Deserialize)]
struct Event {
    id: u64,
    payload: String,
}

async fn drain(client: &Client) -> tarant::Result<()> {
    let events = client.space::<Event>("events");
    let mut cursor: Option<String> = None;
    loop {
        let mut select = events.select(()).iterator(Iter::All).limit(500);
        if let Some(position) = &cursor {
            select = select.after(position);
        }
        let page = select.page().await?;
        if page.rows.is_empty() {
            break;
        }
        // ... process page.rows ...
        cursor = page.position;
    }
    Ok(())
}
```

## Transactions

```rust
use std::time::Duration;
use tarant::{Client, Isolation, TxOptions};

async fn transfer(client: &Client) -> tarant::Result<()> {
    let mut tx = client.stream();
    tx.begin(TxOptions::new()
        .isolation(Isolation::ReadConfirmed)
        .timeout(Duration::from_secs(5)))
        .await?;

    // ... requests on `tx`, executed strictly in order ...

    tx.commit().await
}
```

Dropping the stream mid-transaction sends a rollback, so an abandoned
transaction does not hold locks until its timeout.

Interactive transactions need the server's MVCC transaction manager
(`database.use_mvcc_engine: true`). Without it, any DML inside a transaction
aborts it with `TRANSACTION_YIELD`.

## Watchers

A watcher holds the latest value broadcast for a key and wakes when it
changes. It survives reconnects: the client re-subscribes for you.

```rust
use tarant::Client;

async fn follow_config(client: &Client) -> tarant::Result<()> {
    let mut config = client.watch("app.config").await?;
    loop {
        let value: serde_json::Value = config.get()?;
        // ... apply it ...
        config.changed().await?;
    }
}
```

## Compatibility

**Tarantool 3.0 and later.** The client addresses spaces and indexes by name,
which the protocol gained in 3.0, so there is no schema fetch and no id cache
to go stale. Against 2.10–2.11, `call`, `eval`, transactions and watchers still
work; `Client::server_info()` reports what the handshake negotiated. CI runs
the integration suite against 3.0 and 3.8.

**Rust 1.85 and later** (edition 2024), on tokio 1.x.

## Feature flags

| Flag | Effect |
|---|---|
| `uuid` | accept `uuid::Uuid` directly as a key field |

## Testing

Unit tests cover the wire format against the byte sequences in the Tarantool
manual and need no server. The integration suite runs against a real instance,
and `compose.yaml` brings one up:

```sh
docker compose up --wait
TARANT_TEST_ADDR=tarantool://tarant:tarant@127.0.0.1:3301 cargo test
```

Without `TARANT_TEST_ADDR` the integration tests skip and the suite still
passes.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this crate by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
