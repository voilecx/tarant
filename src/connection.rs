//! The task that owns the socket.
//!
//! Every [`Client`](crate::Client) is a handle onto one of these. The task
//! runs a single loop: take a command off the channel, write its packet;
//! take a packet off the socket, route it to whoever is waiting. Requests
//! are matched to replies by `sync`, events are fanned out to watchers, and
//! when the socket breaks the task reconnects on its own — replaying the
//! handshake and every watch subscription — while callers wait.
//!
//! Nothing in here is public. The surface a user sees is in `client.rs`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures_core::Stream;
use futures_sink::Sink;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, sleep, timeout};
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::auth;
use crate::codec::{Codec, Kind, Request, Response};
use crate::error::{Error, Result};
use crate::iproto::{Feature, PROTOCOL_VERSION, key, request};
use crate::options::{ConnectOptions, Reconnect};

/// Commands a handle can send to its connection task.
pub(crate) enum Command {
    /// Send a packet and route the reply with the given sync back; chunks
    /// pushed by the server before the reply go to `pushes`, if given.
    Request { sync: u64, packet: Bytes, reply: Reply, pushes: Option<Pushes> },
    /// Subscribe a watcher to `key`; the first event arrives promptly.
    Watch { id: u64, key: String, sender: watch::Sender<Option<Bytes>> },
    /// Drop a watcher; the server is told when the last one for a key goes.
    Unwatch { id: u64, key: String },
    /// Send a packet that expects no reply (a rollback from `Drop`).
    Fire { packet: Bytes },
    /// Stop accepting commands, finish what is in flight, close the socket.
    Close,
}

/// What the server told us about itself during the handshake.
#[derive(Debug, Clone)]
pub(crate) struct Negotiated {
    pub(crate) server_version: String,
    pub(crate) protocol_version: u64,
    pub(crate) features: HashSet<Feature>,
}

/// Where a request's reply goes.
type Reply = oneshot::Sender<Result<Response>>;

/// Where a request's `box.session.push()` chunks go.
type Pushes = mpsc::UnboundedSender<Bytes>;

/// The reader side of a request that streams `box.session.push()` chunks.
pub(crate) struct PushStream {
    /// Chunks pushed before the reply, in order.
    pub(crate) pushes: mpsc::UnboundedReceiver<Bytes>,
    response: oneshot::Receiver<Result<Response>>,
    request_timeout: Option<Duration>,
}

impl PushStream {
    /// Await the final reply, once the pushes are drained (or abandoned).
    pub(crate) async fn finish(self) -> Result<Response> {
        let response = match self.request_timeout {
            Some(limit) => {
                timeout(limit, self.response).await.map_err(|_| Error::Timeout(limit))?
            }
            None => self.response.await,
        }
        .map_err(|_| Error::Closed)??;
        match response.kind {
            Kind::Error => Err(response.into_error().into()),
            _ => Ok(response),
        }
    }
}

/// A request waiting for its reply.
struct Pending {
    reply: Reply,
    pushes: Option<Pushes>,
}

/// One subscriber to a watched key: its id, and where its events go.
type Subscriber = (u64, watch::Sender<Option<Bytes>>);

/// What [`Handle::watch`] hands back: the subscription id and its receiver.
type Subscription = (u64, watch::Receiver<Option<Bytes>>);

/// State shared between the task and every handle.
pub(crate) struct Shared {
    next_sync: AtomicU64,
    next_stream_id: AtomicU64,
    next_watch_id: AtomicU64,
    /// Highest schema version seen in any response; lets caches invalidate.
    pub(crate) schema_version: AtomicU64,
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
        let (framed, negotiated) = handshake(&options).await?;
        let shared = Arc::new(Shared {
            next_sync: AtomicU64::new(1),
            next_stream_id: AtomicU64::new(1),
            next_watch_id: AtomicU64::new(1),
            schema_version: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        });
        let (tx, rx) = mpsc::channel(1024);
        let task = Task {
            options,
            rx,
            shared: Arc::clone(&shared),
            pending: HashMap::new(),
            watchers: HashMap::new(),
            deferred: VecDeque::new(),
            shutting_down: false,
        };
        tokio::spawn(task.run(framed));
        Ok(Self { tx, shared, negotiated: Arc::new(negotiated), request_timeout: None })
    }

    pub(crate) const fn with_request_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub(crate) fn negotiated(&self) -> &Negotiated {
        &self.negotiated
    }

    pub(crate) fn supports(&self, feature: Feature) -> bool {
        self.negotiated.features.contains(&feature)
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

    /// Send a request and wait for its reply. Server errors become [`Error::Server`].
    pub(crate) async fn request(&self, sync: u64, packet: Bytes) -> Result<Response> {
        self.request_inner(sync, packet, None).await
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
        let (pushes, receiver) = mpsc::unbounded_channel();
        self.tx
            .send(Command::Request { sync, packet, reply, pushes: Some(pushes) })
            .await
            .map_err(|_| Error::Closed)?;
        Ok(PushStream { pushes: receiver, response, request_timeout: self.request_timeout })
    }

    async fn request_inner(
        &self,
        sync: u64,
        packet: Bytes,
        pushes: Option<Pushes>,
    ) -> Result<Response> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Command::Request { sync, packet, reply, pushes })
            .await
            .map_err(|_| Error::Closed)?;
        let response = match self.request_timeout {
            Some(limit) => timeout(limit, response).await.map_err(|_| Error::Timeout(limit))?,
            None => response.await,
        }
        .map_err(|_| Error::Closed)??;
        match response.kind {
            Kind::Error => Err(response.into_error().into()),
            _ => Ok(response),
        }
    }

    /// Send a packet without waiting. Used from `Drop`, where nothing can be awaited.
    pub(crate) fn fire(&self, packet: Bytes) {
        let _ = self.tx.try_send(Command::Fire { packet });
    }

    pub(crate) async fn watch(&self, key: String) -> Result<Subscription> {
        let id = self.next_watch_id();
        let (sender, receiver) = watch::channel(None);
        self.tx.send(Command::Watch { id, key, sender }).await.map_err(|_| Error::Closed)?;
        Ok((id, receiver))
    }

    pub(crate) fn unwatch(&self, id: u64, key: String) {
        let _ = self.tx.try_send(Command::Unwatch { id, key });
    }

    pub(crate) async fn close(&self) {
        let _ = self.tx.send(Command::Close).await;
    }
}

struct Task {
    options: ConnectOptions,
    rx: mpsc::Receiver<Command>,
    shared: Arc<Shared>,
    pending: HashMap<u64, Pending>,
    watchers: HashMap<String, Vec<Subscriber>>,
    /// Commands that arrived while the link was down.
    deferred: VecDeque<Command>,
    shutting_down: bool,
}

type Link = Framed<TcpStream, Codec>;

// `Framed` is a `Sink`/`Stream`, but the ergonomic `.send()`/`.next()` live on
// `futures_util`'s extension traits. These three helpers drive the inherent
// `poll_*` methods through `poll_fn` instead, so the crate needs only the two
// trait definitions and not the whole `futures_util` toolbox. `Link` is
// `Unpin`, so `Pin::new` over a mutable borrow is sound without any `unsafe`.

/// Send one packet and flush it, exactly as `SinkExt::send` would.
async fn send(link: &mut Link, packet: Bytes) -> Result<()> {
    poll_fn(|cx| Pin::new(&mut *link).poll_ready(cx)).await?;
    Pin::new(&mut *link).start_send(packet)?;
    poll_fn(|cx| Pin::new(&mut *link).poll_flush(cx)).await
}

/// Await the next packet, exactly as `StreamExt::next` would.
async fn recv(link: &mut Link) -> Option<Result<Response>> {
    poll_fn(|cx| Pin::new(&mut *link).poll_next(cx)).await
}

/// Flush and close the sink, exactly as `SinkExt::close` would.
async fn close(link: &mut Link) -> Result<()> {
    poll_fn(|cx| Pin::new(&mut *link).poll_close(cx)).await
}

impl Task {
    async fn run(mut self, mut link: Link) {
        loop {
            // Drain anything deferred during a reconnect before reading new commands.
            while let Some(command) = self.deferred.pop_front() {
                if let Err(err) = self.handle_command(&mut link, command).await {
                    self.fail_link(&err);
                    break;
                }
            }

            let outcome = tokio::select! {
                command = self.rx.recv() => match command {
                    Some(Command::Close) | None => Outcome::Close,
                    Some(command) => match self.handle_command(&mut link, command).await {
                        Ok(()) => Outcome::Continue,
                        Err(err) => Outcome::Lost(err),
                    },
                },
                packet = recv(&mut link) => match packet {
                    Some(Ok(response)) => {
                        self.handle_response(&mut link, response).await;
                        if self.shutting_down && self.pending.is_empty() { Outcome::Close } else { Outcome::Continue }
                    }
                    Some(Err(err)) => Outcome::Lost(err),
                    None => Outcome::Lost(Error::Closed),
                },
            };

            match outcome {
                Outcome::Continue => {}
                Outcome::Close => {
                    self.shared.closed.store(true, Ordering::Release);
                    let _ = close(&mut link).await;
                    self.fail_pending(&Error::Closed);
                    debug!("connection task finished");
                    return;
                }
                Outcome::Lost(err) => {
                    self.fail_link(&err);
                    if let Some(new_link) = self.reconnect().await {
                        link = new_link;
                    } else {
                        self.shared.closed.store(true, Ordering::Release);
                        self.fail_deferred();
                        return;
                    }
                }
            }
        }
    }

    async fn handle_command(&mut self, link: &mut Link, command: Command) -> Result<()> {
        match command {
            Command::Request { sync, packet, reply, pushes } => {
                if self.shutting_down {
                    let _ = reply.send(Err(Error::ShuttingDown));
                    return Ok(());
                }
                self.pending.insert(sync, Pending { reply, pushes });
                // On failure the reply is settled by `fail_link` with the rest.
                send(link, packet).await?;
            }
            Command::Fire { packet } => send(link, packet).await?,
            Command::Watch { id, key, sender } => {
                let first = !self.watchers.contains_key(&key);
                self.watchers.entry(key.clone()).or_default().push((id, sender));
                if first {
                    send(link, watch_packet(request::WATCH, &key)).await?;
                }
            }
            Command::Unwatch { id, key } => {
                let gone = if let Some(list) = self.watchers.get_mut(&key) {
                    list.retain(|(watcher, _)| *watcher != id);
                    list.is_empty()
                } else {
                    false
                };
                if gone {
                    self.watchers.remove(&key);
                    send(link, watch_packet(request::UNWATCH, &key)).await?;
                }
            }
            Command::Close => unreachable!("Close is handled by the caller"),
        }
        Ok(())
    }

    async fn handle_response(&mut self, link: &mut Link, response: Response) {
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
                if let Some(pushes) =
                    self.pending.get(&response.sync).and_then(|p| p.pushes.as_ref())
                {
                    let _ = pushes.send(Bytes::copy_from_slice(response.data_bytes()));
                } else {
                    debug!(sync = response.sync, "chunk for a request not listening");
                }
            }
            Kind::Event => {
                let Some((key, data)) = response.event() else { return };
                let key = key.to_owned();
                let data = Bytes::copy_from_slice(data);
                if key == "box.shutdown" && data.first() == Some(&0xc3) {
                    info!("server announced shutdown; draining in-flight requests");
                    self.shutting_down = true;
                }
                if let Some(list) = self.watchers.get_mut(&key) {
                    list.retain(|(_, sender)| sender.send(Some(data.clone())).is_ok());
                    // Acknowledge so the server may send the next change.
                    let _ = send(link, watch_packet(request::WATCH, &key)).await;
                }
            }
        }
    }

    fn fail_link(&mut self, err: &Error) {
        warn!(error = %err, "connection lost");
        self.fail_pending(&Error::Closed);
    }

    fn fail_pending(&mut self, err: &Error) {
        for (_, pending) in self.pending.drain() {
            let _ = pending.reply.send(Err(clone_error(err)));
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
        let mut delay = min;
        loop {
            let wake = Instant::now() + delay;
            let attempt = tokio::select! {
                () = sleep(delay) => None,
                command = self.rx.recv() => Some(command),
            };
            match attempt {
                Some(None | Some(Command::Close)) => return None,
                Some(Some(command)) => {
                    self.defer(command);
                    // Keep waiting out the rest of the delay.
                    sleep(wake.saturating_duration_since(Instant::now())).await;
                }
                None => {}
            }
            match handshake(&self.options).await {
                Ok((mut link, negotiated)) => {
                    info!(server = %negotiated.server_version, "reconnected");
                    for key in self.watchers.keys() {
                        if send(&mut link, watch_packet(request::WATCH, key)).await.is_err() {
                            break;
                        }
                    }
                    return Some(link);
                }
                Err(err) => {
                    warn!(error = %err, retry_in = ?delay, "reconnect failed");
                    delay = (delay * 2).min(max);
                }
            }
        }
    }

    fn defer(&mut self, command: Command) {
        const MAX_DEFERRED: usize = 4096;
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
    Close,
    Lost(Error),
}

fn clone_error(err: &Error) -> Error {
    match err {
        Error::Closed => Error::Closed,
        Error::ShuttingDown => Error::ShuttingDown,
        other => Error::protocol(other.to_string()),
    }
}

fn watch_packet(ty: u64, key: &str) -> Bytes {
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

    let mut link = Framed::new(stream, Codec);

    // IPROTO_ID and IPROTO_AUTH are pipelined; replies come back in order.
    let features: Vec<u64> = Feature::ANNOUNCED.iter().map(|f| f.code()).collect();
    let id_packet = Request::new(request::ID, 0, None)
        .uint(key::VERSION, PROTOCOL_VERSION)
        .serialized(key::FEATURES, &features, true)?
        .str(key::AUTH_TYPE, auth::CHAP_SHA1)
        .finish();
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
            .finish();
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
