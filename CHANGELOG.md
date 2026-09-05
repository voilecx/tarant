# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/voilecx/tarant/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/voilecx/tarant/releases/tag/v0.1.0
