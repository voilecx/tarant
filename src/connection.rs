//! The task that owns the socket.
//!
//! Every [`Client`](crate::Client) is a handle onto one of these. The task
//! owns the two halves of the socket independently: commands are serialised
//! into a write buffer and flushed in batches, while replies are read as
//! they arrive — a slow writer never blocks a reader. Requests are matched
//! to replies by `sync`, events are fanned out to watchers, and when the
//! socket breaks the task reconnects on its own — replaying the handshake
//! and every watch subscription — while callers wait.
//!
//! Nothing in here is public. The surface a user sees is in `client.rs`.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::poll_fn;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures_core::Stream;
use futures_sink::Sink;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, Sleep, sleep, timeout};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, info, warn};

use crate::auth;
use crate::codec::{Codec, Kind, Request, Response};
use crate::error::{Error, Result};
use crate::iproto::{Feature, PROTOCOL_VERSION, key, request};
use crate::options::{ConnectOptions, Reconnect};

/// Stop reading commands while this many bytes are buffered for the socket.
const HIGH_WATER: usize = 1024 * 1024;

/// Extra commands drained from the channel after the first, per select turn.
const BATCH: usize = 256;

/// How many `box.session.push()` chunks may queue before the request fails.
const PUSH_BACKLOG: usize = 1024;

/// Requests that arrived while the link was down, at most this many.
const MAX_DEFERRED: usize = 4096;

/// Grace window for in-flight requests when `Client::close` has no
/// configured request timeout to reuse.
const CLOSE_DRAIN: Duration = Duration::from_secs(5);

/// Commands a handle can send to its connection task.
pub(crate) enum Command {
    /// Send a packet and route the reply with the given sync back; chunks
    /// pushed by the server before the reply go to `pushes`, if given.
    /// `generation`, when set, pins the request to the current connection:
    /// a transaction request from a dead connection is refused, never sent.
    Request {
        sync: u64,
        packet: Bytes,
        reply: Reply,
        pushes: Option<Pushes>,
        generation: Option<u64>,
    },
    /// Subscribe a watcher to `key`; the first event arrives promptly.
    Watch { id: u64, key: String, sender: watch::Sender<Option<Bytes>> },
    /// Drop a watcher; the server is told when the last one for a key goes.
    Unwatch { id: u64, key: String },
    /// Send a packet that expects no reply (a rollback from `Drop`).
    Fire { packet: Bytes, generation: Option<u64> },
    /// Stop accepting commands, let in-flight requests finish, close the
    /// socket. `done` fires when the task is finished with the link.
    Close { done: oneshot::Sender<()> },
}

/// What the server told us about itself during the handshake.
#[derive(Debug, Clone)]
pub(crate) struct Negotiated {
    pub(crate) server_version: String,
    pub(crate) protocol_version: u64,
    pub(crate) features: HashSet<Feature>,
}

/// The feature set as a bitmask (`bit n` = `Feature::code() == n`).
fn feature_bits(features: &HashSet<Feature>) -> u64 {
    features.iter().fold(0, |bits, feature| bits | (1 << feature.code()))
}

/// Where a request's reply goes.
type Reply = oneshot::Sender<Result<Response>>;

/// Where a request's `box.session.push()` chunks go.
type Pushes = mpsc::Sender<Bytes>;

/// The reader side of a request that streams `box.session.push()` chunks.
pub(crate) struct PushStream {
    /// Chunks pushed before the reply, in order. At most
    /// [`PUSH_BACKLOG`] queue; a consumer that falls behind loses the
    /// request rather than growing memory without bound.
    pub(crate) pushes: mpsc::Receiver<Bytes>,
    response: oneshot::Receiver<Result<Response>>,
    request_timeout: Option<Duration>,
}

impl PushStream {
    /// Await the final reply, once the pushes are drained (or abandoned).
    ///
    /// Finishing early is fine: the receiver is closed first, so chunks
    /// still arriving are dropped instead of queued forever.
    pub(crate) async fn finish(mut self) -> Result<Response> {
        self.pushes.close();
        await_reply(self.response, self.request_timeout).await
    }
}

/// Await a request's reply honouring the optional timeout, mapping a
/// server-side rejection to [`Error::Server`].
async fn await_reply(
    response: oneshot::Receiver<Result<Response>>,
    request_timeout: Option<Duration>,
) -> Result<Response> {
    let response = match request_timeout {
        Some(limit) => timeout(limit, response).await.map_err(|_| Error::Timeout(limit))?,
        None => response.await,
    }
    .map_err(|_| Error::Closed)??;
    match response.kind {
        Kind::Error => Err(response.into_error().into()),
        _ => Ok(response),
    }
}

/// A request waiting for its reply.
struct Pending {
    reply: Reply,
    pushes: Option<Pushes>,
}

/// One subscriber to a watched key: its id, and where its events go.
type Subscriber = (u64, watch::Sender<Option<Bytes>>);

/// Everything the task knows about one broadcast key.
struct WatchState {
    /// The last value the server sent, for late subscribers.
    latest: Option<Bytes>,
    subscribers: Vec<Subscriber>,
    /// Kept regardless of subscribers: the client's own `box.shutdown`
    /// subscription.
    pinned: bool,
}

/// The `sync` counter is generated by this client, so the pending map needs
/// no `HashDoS` resistance — a multiplicative mixer is faster and enough.
#[derive(Default)]
struct SyncHasher(u64);

impl Hasher for SyncHasher {
    fn write_u64(&mut self, n: u64) {
        self.0 = n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = (self.0 ^ u64::from(byte)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

type PendingMap = HashMap<u64, Pending, BuildHasherDefault<SyncHasher>>;

/// What [`Handle::watch`] hands back: the subscription id and its receiver.
type Subscription = (u64, watch::Receiver<Option<Bytes>>);

/// State shared between the task and every handle.
pub(crate) struct Shared {
    next_sync: AtomicU64,
    next_stream_id: AtomicU64,
    next_watch_id: AtomicU64,
    /// Highest schema version seen in any response; lets caches invalidate.
    pub(crate) schema_version: AtomicU64,
    /// The connection's serial number: 0 for the first link, bumped on
    /// every reconnect. A transaction is scoped to one generation.
    generation: AtomicU64,
    /// The feature bitmask from the latest handshake.
    features: AtomicU64,
    /// Set once the server announced `box.shutdown` or the task ended.
    closed: AtomicBool,
}

/// The user-facing side of a connection task.
#[derive(Clone)]
pub(crate) struct Handle {
    tx: mpsc::Sender<Command>,
    shared: Arc<Shared>,
    negotiated: Arc<Negotiated>,
    request_timeout: Option<Duration>,
}

impl Handle {
    /// Connect, complete the handshake, and spawn the task.
    ///
    /// Fails if the first connection cannot be established: a client that
    /// never worked is a configuration error, not something to retry quietly.
    pub(crate) async fn connect(options: ConnectOptions) -> Result<Self> {
        let (link, negotiated) = handshake(&options).await?;
        let shared = Arc::new(Shared {
            next_sync: AtomicU64::new(1),
            next_stream_id: AtomicU64::new(1),
            next_watch_id: AtomicU64::new(1),
            schema_version: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            features: AtomicU64::new(feature_bits(&negotiated.features)),
            closed: AtomicBool::new(false),
        });
        let (tx, rx) = mpsc::channel(1024);
        let task = Task {
            options,
            rx,
            shared: Arc::clone(&shared),
            pending: PendingMap::default(),
            watchers: HashMap::new(),
            deferred: VecDeque::new(),
            shutting_down: false,
            generation: 0,
            shutdown_watch: negotiated.features.contains(&Feature::Watchers),
        };
        tokio::spawn(task.run(link));
        Ok(Self { tx, shared, negotiated: Arc::new(negotiated), request_timeout: None })
    }

    pub(crate) const fn with_request_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// The negotiated view from the first handshake; [`Handle::supports`]
    /// reads the live set, which may differ after a reconnect.
    pub(crate) fn negotiated(&self) -> &Negotiated {
        &self.negotiated
    }

    /// Whether the current connection negotiated `feature`.
    pub(crate) fn supports(&self, feature: Feature) -> bool {
        self.shared.features.load(Ordering::Relaxed) & (1 << feature.code()) != 0
    }

    /// The connection generation the task currently runs on.
    pub(crate) fn generation(&self) -> u64 {
        self.shared.generation.load(Ordering::Acquire)
    }

    pub(crate) fn next_sync(&self) -> u64 {
        self.shared.next_sync.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn next_stream_id(&self) -> u64 {
        self.shared.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn next_watch_id(&self) -> u64 {
        self.shared.next_watch_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn schema_version(&self) -> u64 {
        self.shared.schema_version.load(Ordering::Relaxed)
    }

    pub(crate) const fn request_timeout(&self) -> Option<Duration> {
        self.request_timeout
    }

    /// Send a request and wait for its reply. Server errors become [`Error::Server`].
    pub(crate) async fn request(&self, sync: u64, packet: Bytes) -> Result<Response> {
        self.request_in(sync, packet, None).await
    }

    /// Like [`request`](Self::request), but the request is refused unless the
    /// task still runs on `generation` — see [`Command::Request`].
    pub(crate) async fn request_in(
        &self,
        sync: u64,
        packet: Bytes,
        generation: Option<u64>,
    ) -> Result<Response> {
        self.request_inner(sync, packet, None, generation).await
    }

    /// Like [`request`](Self::request), but the packet is sent right away and
    /// the chunks the server pushes before the reply arrive on the returned
    /// [`PushStream`] as they come. Sending eagerly is what lets a caller
    /// read pushes before it awaits the reply.
    pub(crate) async fn request_with_pushes(&self, sync: u64, packet: Bytes) -> Result<PushStream> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let (reply, response) = oneshot::channel();
        let (pushes, receiver) = mpsc::channel(PUSH_BACKLOG);
        self.tx
            .send(Command::Request { sync, packet, reply, pushes: Some(pushes), generation: None })
            .await
            .map_err(|_| Error::Closed)?;
        Ok(PushStream { pushes: receiver, response, request_timeout: self.request_timeout })
    }

    async fn request_inner(
        &self,
        sync: u64,
        packet: Bytes,
        pushes: Option<Pushes>,
        generation: Option<u64>,
    ) -> Result<Response> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Command::Request { sync, packet, reply, pushes, generation })
            .await
            .map_err(|_| Error::Closed)?;
        await_reply(response, self.request_timeout).await
    }

    /// Send a packet without waiting. Used from `Drop`, where nothing can be awaited.
    pub(crate) fn fire(&self, packet: Bytes, generation: Option<u64>) {
        send_or_spawn(&self.tx, Command::Fire { packet, generation });
    }

    pub(crate) async fn watch(&self, key: String) -> Result<Subscription> {
        let id = self.next_watch_id();
        let (sender, receiver) = watch::channel(None);
        self.tx.send(Command::Watch { id, key, sender }).await.map_err(|_| Error::Closed)?;
        Ok((id, receiver))
    }

    pub(crate) fn unwatch(&self, id: u64, key: String) {
        send_or_spawn(&self.tx, Command::Unwatch { id, key });
    }

    /// Ask the task to drain and close; resolves when it has. A task that
    /// is already gone counts as closed.
    pub(crate) async fn close(&self) {
        let (done, wait) = oneshot::channel();
        if self.tx.send(Command::Close { done }).await.is_err() {
            return;
        }
        let _ = wait.await;
    }
}

/// `try_send`, falling back to a spawned `send` when the channel is full:
/// `Drop` callers cannot await, so rather than losing the message we let a
/// task wait for room. Off the runtime there is nowhere to spawn to.
fn send_or_spawn(tx: &mpsc::Sender<Command>, command: Command) {
    match tx.try_send(command) {
        Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
        Err(mpsc::error::TrySendError::Full(command)) => {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let tx = tx.clone();
                runtime.spawn(async move {
                    let _ = tx.send(command).await;
                });
            }
        }
    }
}

struct Task {
    options: ConnectOptions,
    rx: mpsc::Receiver<Command>,
    shared: Arc<Shared>,
    pending: PendingMap,
    watchers: HashMap<String, WatchState>,
    /// Commands that arrived while the link was down.
    deferred: VecDeque<Command>,
    shutting_down: bool,
    /// The connection generation this task runs; bumped on every reconnect.
    generation: u64,
    /// Whether to pin a `box.shutdown` subscription of our own.
    shutdown_watch: bool,
}

/// The two halves of one socket, each behind its own `Framed*` so writes
/// never have to wait on reads.
struct Link {
    reader: FramedRead<OwnedReadHalf, Codec>,
    writer: FramedWrite<OwnedWriteHalf, Codec>,
}

/// `Framed*` is a `Sink`/`Stream`, but the ergonomic `.send()`/`.next()` live
/// on `futures_util`'s extension traits. These helpers drive the inherent
/// `poll_*` methods through `poll_fn` instead, so the crate needs only the
/// two trait definitions and not the whole `futures_util` toolbox. `Link`'s
/// halves are `Unpin`, so `Pin::new` over a mutable borrow is sound.
///
/// Buffer `packet` into the writer; a flush turns the whole batch into one
/// write syscall.
fn enqueue(link: &mut Link, packet: Bytes) -> Result<()> {
    Pin::new(&mut link.writer).start_send(packet)
}

/// Send one packet and flush it, exactly as `SinkExt::send` would. Used on
/// the handshake path, where pipelining is not wanted.
async fn send(link: &mut Link, packet: Bytes) -> Result<()> {
    poll_fn(|cx| Pin::new(&mut link.writer).poll_ready(cx)).await?;
    enqueue(link, packet)?;
    poll_fn(|cx| Pin::new(&mut link.writer).poll_flush(cx)).await
}

/// Await the next packet, exactly as `StreamExt::next` would.
async fn recv(link: &mut Link) -> Option<Result<Response>> {
    poll_fn(|cx| Pin::new(&mut link.reader).poll_next(cx)).await
}

/// The keepalive clock: `interval` quiet sends a PING, another declares
/// the link dead. Any received packet resets it.
struct KeepAlive {
    interval: Duration,
    timer: Pin<Box<Sleep>>,
    pinged: bool,
}

impl KeepAlive {
    fn new(interval: Duration) -> Self {
        Self { interval, timer: Box::pin(sleep(interval)), pinged: false }
    }

    fn note_packet(&mut self) {
        self.pinged = false;
        self.timer.as_mut().reset(Instant::now() + self.interval);
    }
}

impl Task {
    async fn run(mut self, mut link: Link) {
        let mut needs_flush = false;
        if self.shutdown_watch {
            // Our own `box.shutdown` subscription: pinned so no user watcher
            // can unsubscribe it, and with no subscribers of its own.
            self.watchers.insert(
                String::from("box.shutdown"),
                WatchState { latest: None, subscribers: Vec::new(), pinned: true },
            );
            if let Ok(packet) = watch_packet(request::WATCH, "box.shutdown") {
                needs_flush = enqueue(&mut link, packet).is_ok();
            }
        }
        let mut keepalive = self.options.keepalive_interval().map(KeepAlive::new);
        loop {
            // Drain anything deferred during a reconnect before new commands;
            // while a shutdown drain is in progress they stay queued.
            if !self.shutting_down {
                while let Some(command) = self.deferred.pop_front() {
                    self.handle_command(&mut link, command, &mut needs_flush);
                }
            }

            let outcome = tokio::select! {
                command = self.rx.recv(), if link.writer.write_buffer().len() < HIGH_WATER => {
                    match command {
                        Some(Command::Close { done }) => Outcome::Close(done),
                        Some(command) => {
                            self.handle_command(&mut link, command, &mut needs_flush);
                            let mut outcome = Outcome::Continue;
                            for _ in 0..BATCH {
                                match self.rx.try_recv() {
                                    Ok(Command::Close { done }) => {
                                        outcome = Outcome::Close(done);
                                        break;
                                    }
                                    Ok(command) => {
                                        self.handle_command(&mut link, command, &mut needs_flush);
                                    }
                                    Err(mpsc::error::TryRecvError::Empty) => break,
                                    Err(mpsc::error::TryRecvError::Disconnected) => {
                                        outcome = Outcome::Dropped;
                                        break;
                                    }
                                }
                            }
                            outcome
                        }
                        None => Outcome::Dropped,
                    }
                }
                packet = poll_fn(|cx| Pin::new(&mut link.reader).poll_next(cx)) => match packet {
                    Some(Ok(response)) => {
                        if let Some(keepalive) = &mut keepalive {
                            keepalive.note_packet();
                        }
                        self.handle_response(&mut link, response, &mut needs_flush);
                        if self.shutting_down && self.pending.is_empty() {
                            Outcome::Shutdown
                        } else {
                            Outcome::Continue
                        }
                    }
                    Some(Err(err)) => Outcome::Lost(err),
                    None => Outcome::Lost(Error::Closed),
                },
                result = poll_fn(|cx| Pin::new(&mut link.writer).poll_flush(cx)), if needs_flush => {
                    match result {
                        Ok(()) => {
                            needs_flush = false;
                            Outcome::Continue
                        }
                        Err(err) => Outcome::Lost(err),
                    }
                }
                () = async { keepalive.as_mut().expect("enabled").timer.as_mut().await }, if keepalive.is_some() => {
                    let keepalive = keepalive.as_mut().expect("enabled");
                    if keepalive.pinged {
                        Outcome::Lost(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "keepalive: no packet for two intervals",
                        )))
                    } else {
                        keepalive.pinged = true;
                        keepalive.timer.as_mut().reset(Instant::now() + keepalive.interval);
                        let sync = self.shared.next_sync.fetch_add(1, Ordering::Relaxed);
                        if let Ok(packet) = Request::new(request::PING, sync, None).finish() {
                            if enqueue(&mut link, packet).is_ok() {
                                needs_flush = true;
                            }
                        }
                        Outcome::Continue
                    }
                }
            };

            if !self.handle_outcome(&mut link, outcome, &mut needs_flush).await {
                return;
            }
        }
    }

    /// Act on a select outcome. `false` means the task is done.
    async fn handle_outcome(
        &mut self,
        link: &mut Link,
        outcome: Outcome,
        needs_flush: &mut bool,
    ) -> bool {
        match outcome {
            Outcome::Continue => true,
            Outcome::Close(done) => {
                self.shared.closed.store(true, Ordering::Release);
                self.rx.close();
                // Let in-flight requests finish: keep reading (and
                // flushing) until the replies are in or time runs out.
                self.drain(link, needs_flush).await;
                let _ = poll_fn(|cx| Pin::new(&mut link.writer).poll_close(cx)).await;
                self.fail_pending();
                let _ = done.send(());
                debug!("connection task finished");
                false
            }
            Outcome::Dropped => {
                self.shared.closed.store(true, Ordering::Release);
                self.rx.close();
                let _ = poll_fn(|cx| Pin::new(&mut link.writer).poll_close(cx)).await;
                self.fail_pending();
                debug!("connection task finished");
                false
            }
            Outcome::Shutdown => {
                let _ = poll_fn(|cx| Pin::new(&mut link.writer).poll_close(cx)).await;
                match self.options.reconnect_policy() {
                    Reconnect::Never => {
                        self.shared.closed.store(true, Ordering::Release);
                        self.fail_pending();
                        self.fail_deferred();
                        debug!("connection task finished");
                        false
                    }
                    Reconnect::Backoff { .. } => {
                        info!("server shut down gracefully; reconnecting");
                        if let Some(new_link) = self.reconnect().await {
                            *link = new_link;
                            true
                        } else {
                            self.shared.closed.store(true, Ordering::Release);
                            self.fail_deferred();
                            false
                        }
                    }
                }
            }
            Outcome::Lost(err) => {
                self.fail_link(&err);
                if let Some(new_link) = self.reconnect().await {
                    *link = new_link;
                    true
                } else {
                    self.shared.closed.store(true, Ordering::Release);
                    self.fail_deferred();
                    false
                }
            }
        }
    }

    /// Keep reading replies until the pending map drains, the link fails or
    /// the deadline passes — the configured request timeout, else
    /// [`CLOSE_DRAIN`].
    async fn drain(&mut self, link: &mut Link, needs_flush: &mut bool) {
        let limit = self.options.request_timeout_value().unwrap_or(CLOSE_DRAIN);
        let timer = sleep(limit);
        tokio::pin!(timer);
        while !self.pending.is_empty() {
            tokio::select! {
                packet = poll_fn(|cx| Pin::new(&mut link.reader).poll_next(cx)) => match packet {
                    Some(Ok(response)) => self.handle_response(link, response, needs_flush),
                    Some(Err(_)) | None => break,
                },
                result = poll_fn(|cx| Pin::new(&mut link.writer).poll_flush(cx)), if *needs_flush => {
                    match result {
                        Ok(()) => *needs_flush = false,
                        Err(_) => break,
                    }
                }
                () = &mut timer => break,
            }
        }
    }

    /// Synchronous: an encode failure costs only that request's reply, and
    /// nothing here can lose the link. I/O errors surface in the flush arm.
    fn handle_command(&mut self, link: &mut Link, command: Command, needs_flush: &mut bool) {
        match command {
            Command::Request { sync, packet, reply, pushes, generation } => {
                // The caller timed out or was cancelled; do not spend a
                // round-trip on a reply nobody will read.
                if reply.is_closed() {
                    return;
                }
                if generation.is_some_and(|g| g != self.generation) {
                    let _ = reply.send(Err(Error::Closed));
                    return;
                }
                if self.shutting_down {
                    match self.options.reconnect_policy() {
                        Reconnect::Backoff { .. } => {
                            self.defer(Command::Request {
                                sync,
                                packet,
                                reply,
                                pushes,
                                generation,
                            });
                        }
                        Reconnect::Never => {
                            let _ = reply.send(Err(Error::ShuttingDown));
                        }
                    }
                    return;
                }
                self.pending.insert(sync, Pending { reply, pushes });
                match enqueue(link, packet) {
                    Ok(()) => *needs_flush = true,
                    // A packet that will not encode fails its request only.
                    Err(err) => {
                        let pending = self.pending.remove(&sync).expect("just inserted");
                        let _ = pending.reply.send(Err(err));
                    }
                }
            }
            Command::Fire { packet, generation } => {
                if generation.is_some_and(|g| g != self.generation) {
                    return;
                }
                if enqueue(link, packet).is_ok() {
                    *needs_flush = true;
                }
            }
            Command::Watch { id, key, sender } => {
                if sender.is_closed() {
                    return;
                }
                match self.watchers.entry(key.clone()) {
                    Entry::Vacant(entry) => {
                        entry.insert(WatchState {
                            latest: None,
                            subscribers: vec![(id, sender)],
                            pinned: false,
                        });
                        if let Ok(packet) = watch_packet(request::WATCH, &key) {
                            if enqueue(link, packet).is_ok() {
                                *needs_flush = true;
                            }
                        }
                    }
                    Entry::Occupied(mut entry) => {
                        let state = entry.get_mut();
                        // A late subscriber gets the current value at once.
                        if let Some(latest) = &state.latest {
                            let _ = sender.send(Some(latest.clone()));
                        }
                        state.subscribers.push((id, sender));
                    }
                }
            }
            Command::Unwatch { id, key } => {
                let mut unwatch = false;
                if let Some(state) = self.watchers.get_mut(&key) {
                    state.subscribers.retain(|(watcher, _)| *watcher != id);
                    if state.subscribers.is_empty() && !state.pinned {
                        self.watchers.remove(&key);
                        unwatch = true;
                    }
                }
                if unwatch {
                    if let Ok(packet) = watch_packet(request::UNWATCH, &key) {
                        if enqueue(link, packet).is_ok() {
                            *needs_flush = true;
                        }
                    }
                }
            }
            Command::Close { .. } => unreachable!("Close is handled by the caller"),
        }
    }

    fn handle_response(&mut self, link: &mut Link, response: Response, needs_flush: &mut bool) {
        if let Some(version) = response.schema_version {
            self.shared.schema_version.fetch_max(version, Ordering::Relaxed);
        }
        match response.kind {
            Kind::Ok | Kind::Error => {
                if let Some(pending) = self.pending.remove(&response.sync) {
                    // Dropping `pending.pushes` ends the caller's push stream.
                    let _ = pending.reply.send(Ok(response));
                } else {
                    debug!(sync = response.sync, "reply for a request nobody is waiting for");
                }
            }
            Kind::Chunk => {
                let Some(pushes) =
                    self.pending.get(&response.sync).and_then(|p| p.pushes.as_ref()).cloned()
                else {
                    debug!(sync = response.sync, "chunk for a request not listening");
                    return;
                };
                if let Some(chunk) = response.chunk_bytes() {
                    match pushes.try_send(chunk) {
                        Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            let pending = self.pending.remove(&response.sync).expect("checked");
                            let _ = pending.reply.send(Err(Error::protocol(
                                "push consumer fell more than 1024 chunks behind",
                            )));
                        }
                    }
                }
            }
            Kind::Event => {
                let Some((key, data)) = response.event() else { return };
                let key = key.to_owned();
                // Kept for late subscribers; a copy so it never pins the
                // read buffer it was sliced from.
                let data = Bytes::copy_from_slice(data);
                if key == "box.shutdown" && data.first() == Some(&0xc3) {
                    info!("server announced shutdown; draining in-flight requests");
                    self.shutting_down = true;
                }
                let Some(state) = self.watchers.get_mut(&key) else { return };
                state.latest = Some(data.clone());
                state.subscribers.retain(|(_, sender)| sender.send(Some(data.clone())).is_ok());
                // Acknowledge so the server may send the next change — or
                // drop a subscription nobody is listening to anymore.
                let ty = if state.subscribers.is_empty() && !state.pinned {
                    self.watchers.remove(&key);
                    request::UNWATCH
                } else {
                    request::WATCH
                };
                if let Ok(packet) = watch_packet(ty, &key) {
                    if enqueue(link, packet).is_ok() {
                        *needs_flush = true;
                    }
                }
            }
        }
    }

    fn fail_link(&mut self, err: &Error) {
        warn!(error = %err, "connection lost");
        self.fail_pending();
    }

    /// In-flight requests get `Error::Closed`: the server may or may not
    /// have executed them, and there is no way to know which.
    fn fail_pending(&mut self) {
        for (_, pending) in self.pending.drain() {
            let _ = pending.reply.send(Err(Error::Closed));
        }
    }

    fn fail_deferred(&mut self) {
        for command in self.deferred.drain(..) {
            if let Command::Request { reply, .. } = command {
                let _ = reply.send(Err(Error::Closed));
            }
        }
        // Handles still holding the sender will see the channel closed.
        self.rx.close();
    }

    /// Re-establish the link per the reconnect policy, buffering commands meanwhile.
    async fn reconnect(&mut self) -> Option<Link> {
        let Reconnect::Backoff { min, max } = self.options.reconnect_policy() else {
            return None;
        };
        let mut delay = min.max(Duration::from_millis(1));
        let mut attempts = 0u64;
        loop {
            // "Equal jitter": sleep delay/2 + a uniform share of the rest,
            // while commands keep arriving and are deferred.
            let half = delay / 2;
            let jitter = Duration::from_nanos(
                u64::try_from(
                    u128::from(std::collections::hash_map::RandomState::new().hash_one(attempts))
                        % (half.as_nanos() + 1),
                )
                .unwrap_or(u64::MAX),
            );
            attempts += 1;
            let timer = sleep(half + jitter);
            tokio::pin!(timer);
            loop {
                tokio::select! {
                    () = &mut timer => break,
                    command = self.rx.recv() => match command {
                        Some(Command::Close { done }) => {
                            drop(done);
                            return None;
                        }
                        Some(command) => self.defer(command),
                        None => return None,
                    },
                }
            }
            match handshake(&self.options).await {
                Ok((mut link, negotiated)) => {
                    info!(server = %negotiated.server_version, "reconnected");
                    // The generation and the live feature set move before any
                    // deferred command may be sent on this link.
                    self.generation += 1;
                    self.shared.generation.store(self.generation, Ordering::Release);
                    self.shared
                        .features
                        .store(feature_bits(&negotiated.features), Ordering::Release);
                    self.shutting_down = false;
                    for key in self.watchers.keys() {
                        if let Ok(packet) = watch_packet(request::WATCH, key) {
                            if send(&mut link, packet).await.is_err() {
                                break;
                            }
                        }
                    }
                    return Some(link);
                }
                Err(err) => {
                    warn!(error = %err, retry_in = ?delay, "reconnect failed");
                    delay = delay.saturating_mul(2).min(max);
                }
            }
        }
    }

    fn defer(&mut self, command: Command) {
        if self.deferred.len() >= MAX_DEFERRED {
            if let Command::Request { reply, .. } = command {
                let _ = reply.send(Err(Error::Closed));
            }
            return;
        }
        self.deferred.push_back(command);
    }
}

enum Outcome {
    Continue,
    /// `Client::close`: drain in-flight requests, then shut down.
    Close(oneshot::Sender<()>),
    /// Every handle is gone: shut down at once, nothing to drain for.
    Dropped,
    /// The server announced shutdown and the last reply just came in.
    Shutdown,
    Lost(Error),
}

fn watch_packet(ty: u64, key: &str) -> Result<Bytes> {
    Request::new(ty, 0, None).str(key::EVENT_KEY, key).finish()
}

/// Open the socket, read the greeting, negotiate features, authenticate.
async fn handshake(options: &ConnectOptions) -> Result<(Link, Negotiated)> {
    let limit = options.connect_timeout_value();
    timeout(limit, handshake_inner(options)).await.map_err(|_| Error::Timeout(limit))?
}

/// Length of a `chap-sha1` scramble, in bytes.
const SCRAMBLE_LEN: u32 = 20;

async fn handshake_inner(options: &ConnectOptions) -> Result<(Link, Negotiated)> {
    let mut stream = TcpStream::connect(options.addr()).await?;
    stream.set_nodelay(options.nodelay())?;

    let mut greeting = [0u8; 128];
    stream.read_exact(&mut greeting).await?;
    let banner = String::from_utf8_lossy(&greeting[..64]);
    let banner = banner.trim_end_matches(['\0', '\n']);
    let server_version = banner
        .strip_prefix("Tarantool ")
        .and_then(|rest| rest.split_whitespace().next())
        .ok_or_else(|| Error::protocol(format!("unexpected greeting `{banner}`")))?
        .to_owned();
    let salt_line = &greeting[64..128];

    let (read, write) = stream.into_split();
    let mut link = Link {
        reader: FramedRead::new(read, Codec::new(options.max_packet())),
        writer: FramedWrite::new(write, Codec::new(options.max_packet())),
    };

    // IPROTO_ID and IPROTO_AUTH are pipelined; replies come back in order.
    let features: Vec<u64> = Feature::ANNOUNCED.iter().map(|f| f.code()).collect();
    let id_packet = Request::new(request::ID, 0, None)
        .uint(key::VERSION, PROTOCOL_VERSION)
        .serialized(key::FEATURES, &features, true)?
        .str(key::AUTH_TYPE, auth::CHAP_SHA1)
        .finish()?;
    send(&mut link, id_packet).await?;

    let user = options.user_name().filter(|user| *user != "guest");
    if let Some(user) = user {
        let scramble = auth::chap_sha1(salt_line, options.password_str())?;
        let mut tuple = Vec::with_capacity(32);
        rmp::encode::write_array_len(&mut tuple, 2).expect("vec write");
        rmp::encode::write_str(&mut tuple, auth::CHAP_SHA1).expect("vec write");
        rmp::encode::write_str_len(&mut tuple, SCRAMBLE_LEN).expect("vec write");
        tuple.extend_from_slice(&scramble);
        let auth_packet = Request::new(request::AUTH, 1, None)
            .str(key::USER_NAME, user)
            .raw(key::TUPLE, &tuple)
            .finish()?;
        send(&mut link, auth_packet).await?;
    }

    let id_reply = next_reply(&mut link).await?;
    let negotiated = match id_reply.kind {
        Kind::Ok => parse_id(&id_reply, server_version)?,
        // Servers older than 2.10 do not know IPROTO_ID; treat as no features.
        Kind::Error => {
            debug!("server rejected IPROTO_ID; assuming a pre-2.10 feature set");
            Negotiated { server_version, protocol_version: 0, features: HashSet::new() }
        }
        _ => return Err(Error::protocol("unexpected reply to IPROTO_ID")),
    };

    if let Some(user) = user {
        let auth_reply = next_reply(&mut link).await?;
        if auth_reply.kind == Kind::Error {
            return Err(Error::Auth {
                user: user.to_owned(),
                source: Box::new(auth_reply.into_error()),
            });
        }
    }

    debug!(server = %negotiated.server_version, features = ?negotiated.features, "connected");
    Ok((link, negotiated))
}

async fn next_reply(link: &mut Link) -> Result<Response> {
    match recv(link).await {
        Some(Ok(response)) => Ok(response),
        Some(Err(err)) => Err(err),
        None => Err(Error::Closed),
    }
}

fn parse_id(reply: &Response, server_version: String) -> Result<Negotiated> {
    // Body: { VERSION: uint, FEATURES: [uint], AUTH_TYPE: str }.
    let body: HashMap<u64, rmpv::Value> = reply.body_map()?;
    let protocol_version =
        body.get(&u64::from(key::VERSION)).and_then(rmpv::Value::as_u64).unwrap_or(0);
    let features = body
        .get(&u64::from(key::FEATURES))
        .and_then(rmpv::Value::as_array)
        .map(|codes| {
            codes.iter().filter_map(rmpv::Value::as_u64).filter_map(Feature::from_code).collect()
        })
        .unwrap_or_default();
    Ok(Negotiated { server_version, protocol_version, features })
}
