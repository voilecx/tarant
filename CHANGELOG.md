# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] — 2026-11-22

### Fixed

- `Uuid::from_str` no longer panics or misparses on misplaced hyphens: they
  are accepted only at byte offsets 8, 13, 18 and 23 of a 36-byte input,
  and every other byte must be a hex digit.
- `Decimal` rejects values whose adjusted exponent exceeds the server's
  decNumber limits (±`Decimal::MAX_EXPONENT`, 999 999) instead of storing
  an exponent Tarantool would refuse — `1e1000000` now fails to parse.
- Connection-URL errors no longer echo the URL or the credentials it
  carried; messages name the host and which credential failed.
- `ConnectOptions::new` normalises IPv6 literals: `"::1"` and `"[::1]"`
  become `"[::1]:3301"`, while `"[::1]:3302"` is kept.
- Error text and event keys in replies are decoded lossily (U+FFFD for
  invalid UTF-8) instead of failing the whole packet; a malformed
  `IPROTO_ERROR` map falls back to the plain error message.
- A response claiming a huge length no longer reserves that much memory up
  front, and lengths over `max_packet_size` are rejected outright.
- Push chunks for streamed calls are zero-copy slices of the reply;
  a consumer falling more than 1024 chunks behind fails its request
  instead of growing memory without bound.

### Added

- `Sub` for `Interval` (unit-wise and saturating, like `Add`).
- `Decimal::MAX_EXPONENT`, the server's bound on the adjusted exponent.
- `usize` and `isize` as single-field keys.
- `ConnectOptions::keepalive(interval)`: sends `IPROTO_PING` after an
  interval of silence and drops the link after a second one.
- `ConnectOptions::max_packet_size(bytes)`: caps the largest accepted
  response frame; the default stays the 2 GiB protocol maximum.
- `tests/fake_server.rs`, a scripted iproto server that exercises
  reconnects, graceful shutdown, batching and malformed replies without a
  real Tarantool.

### Changed

- Breaking: `FieldRef`, `Adjust`, `Page` and `sql::Rows` are now
  `#[non_exhaustive]`, so later fields and variants are not a breaking
  change.
- Breaking: `Space` methods now ask only for what they use — `insert`,
  `replace` and `upsert` need `Serialize`, `get`, `delete` and `update`
  need `Deserialize`; a `Deserialize`-only tuple type can be read.
- A `Decimal` zero keeps a positive scale: `"0.00"` displays as `"0.00"`,
  still equal to `"0"`.
- `Key`'s documentation warns that `Vec<u8>`/`&[u8]` encodes as an array
  of integers, not a `varbinary` field.
- Breaking: request packets are built in a single buffer; `codec::Request`
  is crate-internal, so this only shows as `ArrayLike` now serialising
  without a temporary.
- The connection task reads and writes independently: commands are
  buffered and flushed in batches, so a stalled writer no longer blocks
  replies, and one encode failure costs only that request.
- Reconnect backoff now uses `saturating_mul`, a 1 ms floor and equal
  jitter; commands arriving during the whole wait are still accepted.
- `server_info()` is documented as the first-handshake snapshot; live
  feature checks go through the re-negotiated set.
- Requests for spaces or indexes by name fail fast with
  `Error::Unsupported` on pre-3.0 servers; selects using `after`/`page`
  require pagination support (2.11+).
- A request whose caller has already timed out or been cancelled is never
  sent — including ones queued during a reconnect.
- Watcher registration caches the latest value per key: a second
  `Client::watch` resolves immediately, and a subscribe nobody awaits is
  unwatched instead of leaking.
- Breaking: transaction requests — `begin`, `commit`, `rollback`, the
  drop-time rollback and everything issued on an open transaction — are
  pinned to their connection generation. After a reconnect they fail with
  `Error::Closed` instead of running outside the dead transaction.
- `Client::close` drains in-flight requests (up to the request timeout,
  else 5 s) before shutting the task down; `Handle::fire` no longer loses
  Drop-path sends to a full channel when a runtime is around.
- On `box.shutdown` the client stops taking work, lets pending replies
  land, closes the socket and — with `Reconnect::Backoff` — reconnects;
  `Reconnect::Never` still ends the task with `Error::ShuttingDown`.


## [0.3.0] — 2026-11-13

### Changed

- `Decimal` now supports Tarantool 3.5's 76-significant-digit values.
- `Decimal::mantissa()` is fallible because a 76-digit value cannot fit in `i128`.
- Session-push documentation now marks `box.session.push()` as legacy and
  recommends `box.broadcast()` with watchers for new server code.

## [0.2.1] — 2026-09-05

### Security

- The `time` feature now requires `time` 0.3.47 or later, which fixes
  RUSTSEC-2026-0009 (stack exhaustion on hostile input). That release needs
  Rust 1.88, so enabling the feature does too; the crate itself stays at 1.85.

## [0.2.0] — 2026-09-05

The release that finishes the protocol: everything a service talks to a
Tarantool 3.x instance for is now typed and tested against a live server.

### Added

- **SQL.** `Client::query`, `Client::execute`, and `Client::prepare` for
  prepared statements, each with a twin on `Stream` for use in a transaction.
  Parameters are an argument list in which a plain value fills a `?` and
  `sql::Named` fills a `:name`. Result columns arrive as `sql::Column`.
- **Field types.** `Decimal`, `Uuid`, `Datetime` and `Interval` encode as the
  MessagePack extensions Tarantool expects, usable in tuple structs, as keys,
  and via `TryFrom<&Value>`. Optional conversions behind the `uuid`,
  `rust_decimal`, `time`, `chrono` and `jiff` features.
- **Synchronous transactions.** `TxOptions::synchronous()` (Tarantool 3.3+).
- **Arrow insert.** `Space::insert_arrow` for `IPROTO_INSERT_ARROW` (3.3+).
- **Session push.** `Client::call_with_pushes` / `eval_with_pushes` return a
  `PushCall` that yields `box.session.push` values as they arrive.
- **Tuple-cursor pagination.** `Select::after_tuple`, alongside `after`.
- More `ErrorCode` constants (SQL, sync, field-type, unsupported).
- A live test of the full `tarantool/queue` lifecycle, and a README section
  on driving the queue, including the reconnect caveat.

### Removed

- Six transitive dependencies: `base64` (the greeting salt is decoded
  in-crate), `futures-util` (the socket is driven through `Framed`'s own
  `Sink`/`Stream`), and what they pulled in. `Response` is a third smaller
  and request encoding no longer allocates a scratch buffer per tuple.

### Changed

- The client now announces protocol version 10 and the `pagination`,
  `is_sync` and `insert_arrow` features. `Feature` gained the variants the
  protocol defines through version 9, and `Feature::ANNOUNCED` names the set
  the client implements.


## [0.1.1] — 2026-09-05

### Fixed

- The README's examples are complete programs rather than rustdoc fragments,
  so they read correctly on GitHub and crates.io.
- The compatibility statement is precise: space operations need Tarantool
  3.0, while `call`, `eval`, transactions and watchers also work on 2.10–2.11.
- `LICENSE-MIT` names the copyright holder.

### Changed

- docs.rs builds with every feature enabled, so `uuid` support is documented.
- The repository is a single crate at its root, and the integration suite
  starts its server from `compose.yaml`; CI covers Tarantool 3.0 and 3.8.

## [0.1.0] — 2026-09-05

First release.

### Added

- `Client`: connect over a URL or `ConnectOptions`, pipelined requests over one
  connection, `ping`, `call`, `eval`, feature negotiation and `chap-sha1`
  authentication.
- `Space<T>`: typed `insert`, `replace`, `upsert`, `get`, `delete`, `update`,
  addressed by name (Tarantool 3.0+) with no schema round-trip.
- `Select`: every iterator type, `limit`, `offset`, and cursor pagination via
  `page()` / `after()` that stays correct under concurrent writes.
- `Stream`: ordered request sequences and interactive transactions with
  isolation levels, a server-side timeout, and rollback on drop.
- `Watcher`: `box.broadcast` subscriptions that survive reconnects, plus
  `watch_once`.
- `ServerError` with the numeric `ErrorCode` and the full `MP_ERROR` stack.
- Automatic reconnection with backoff: the handshake is replayed and every
  watcher re-subscribed.

[Unreleased]: https://github.com/voilecx/tarant/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/voilecx/tarant/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/voilecx/tarant/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/voilecx/tarant/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/voilecx/tarant/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/voilecx/tarant/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/voilecx/tarant/releases/tag/v0.1.0
