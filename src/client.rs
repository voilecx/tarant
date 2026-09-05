//! The public surface: [`Client`] and the handles you reach through it.
//!
//! A [`Client`] is a cheap, cloneable handle onto one connection. From it you
//! reach a [`Space`] (typed CRUD over one space), a [`Stream`] (an ordered,
//! optionally transactional sequence of requests), and the bare
//! [`call`](Client::call) / [`eval`](Client::eval) escape hatches. Cloning a
//! `Client` shares the connection; dropping the last clone closes it.

use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Request;
use crate::connection::{Handle, Negotiated};
use crate::error::{Error, Result};
use crate::iproto::{Feature, Isolation, Iter, key, request};
use crate::options::ConnectOptions;
use crate::tuple::{Args, Key, encode_array};
use crate::update::Update;
use crate::watcher::Watcher;

/// A connection to a Tarantool instance.
///
/// ```no_run
/// # use tarant::Client;
/// # async fn demo() -> tarant::Result<()> {
/// let client = Client::connect("tarantool://app:secret@127.0.0.1:3301").await?;
/// client.ping().await?;
/// # Ok(()) }
/// ```
///
/// The handle is `Clone` and `Send + Sync`; share one across tasks rather
/// than opening a connection per task. Requests on the same client are
/// pipelined over the single socket and answered concurrently.
#[derive(Clone)]
pub struct Client {
    handle: Handle,
}

impl Client {
    /// Connect using a `tarantool://[user[:password]@]host[:port]` URL.
    ///
    /// Resolves once the TCP connection, protocol negotiation and
    /// authentication have all succeeded. A failure here is a real
    /// misconfiguration — wrong address, wrong password — and is returned;
    /// only *later* drops are retried, per the reconnect policy.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::with_options(url.parse()?).await
    }

    /// Connect with explicit [`ConnectOptions`].
    pub async fn with_options(options: ConnectOptions) -> Result<Self> {
        let request_timeout = options.request_timeout_value();
        let handle = Handle::connect(options).await?.with_request_timeout(request_timeout);
        Ok(Self { handle })
    }

    /// A typed handle to the space named `name`.
    ///
    /// `T` is the tuple type: what [`insert`](Space::insert) takes and what
    /// [`select`](Space::select) returns. Nothing is sent here — the space is
    /// addressed by name on each request (Tarantool 3.0+), so there is no
    /// schema round-trip and no state to go stale.
    pub fn space<T>(&self, name: impl Into<String>) -> Space<T> {
        Space {
            handle: self.handle.clone(),
            name: Arc::new(name.into()),
            _tuple: std::marker::PhantomData,
        }
    }

    /// Call the stored function `name` with `args`, decoding its return as `R`.
    ///
    /// Arguments follow the [`Args`] rules: `()` for none, a tuple for
    /// several, a scalar for one. A Lua function returning multiple values
    /// yields a tuple `R`; one returning a single value yields it directly.
    ///
    /// ```no_run
    /// # use tarant::Client;
    /// # async fn demo(client: Client) -> tarant::Result<()> {
    /// let (sum,): (i64,) = client.call("add", (2, 3)).await?;
    /// let (min, max): (i64, i64) = client.call("bounds", ()).await?;
    /// # Ok(()) }
    /// ```
    pub async fn call<R: DeserializeOwned>(&self, name: &str, args: impl Args) -> Result<R> {
        let sync = self.handle.next_sync();
        let packet = Request::new(request::CALL, sync, None)
            .str(key::FUNCTION_NAME, name)
            .raw(key::TUPLE, &encode_array(&args)?)
            .finish();
        self.handle.request(sync, packet).await?.data()
    }

    /// Evaluate the Lua expression `expr` with `args`, decoding the result as `R`.
    ///
    /// Requires the `guest` user to have `execute` on `universe`, or an
    /// authenticated user with the same. Prefer [`call`](Self::call) for
    /// anything that has a home in a stored function.
    pub async fn eval<R: DeserializeOwned>(&self, expr: &str, args: impl Args) -> Result<R> {
        let sync = self.handle.next_sync();
        let packet = Request::new(request::EVAL, sync, None)
            .str(key::EXPR, expr)
            .raw(key::TUPLE, &encode_array(&args)?)
            .finish();
        self.handle.request(sync, packet).await?.data()
    }

    /// Round-trip a `ping`. Cheap; useful as a liveness probe.
    pub async fn ping(&self) -> Result<()> {
        let sync = self.handle.next_sync();
        let packet = Request::new(request::PING, sync, None).finish();
        self.handle.request(sync, packet).await.map(drop)
    }

    /// Open a stream: a sequence of requests the server runs strictly in order.
    ///
    /// A stream is the unit of interactive transactions — see
    /// [`Stream::begin`]. Even without a transaction it guarantees the server
    /// finishes one request before starting the next, which plain pipelining
    /// does not.
    pub fn stream(&self) -> Stream {
        Stream {
            handle: self.handle.clone(),
            id: self.handle.next_stream_id(),
            in_transaction: false,
        }
    }

    /// Subscribe to a broadcast key set with `box.broadcast()`.
    ///
    /// The returned [`Watcher`] yields the current value immediately and then
    /// every change, coalescing missed updates to the latest. Requires the
    /// server to support watchers (Tarantool 2.10+).
    pub async fn watch(&self, key: impl Into<String>) -> Result<Watcher> {
        if !self.handle.supports(Feature::Watchers) {
            return Err(Error::Unsupported("watchers (server is older than 2.10)"));
        }
        Watcher::subscribe(self.handle.clone(), key.into()).await
    }

    /// Read a broadcast key once, without subscribing (Tarantool 3.0+).
    pub async fn watch_once<R: DeserializeOwned>(&self, key: &str) -> Result<R> {
        if !self.handle.supports(Feature::WatchOnce) {
            return Err(Error::Unsupported("watch_once (server is older than 3.0)"));
        }
        let sync = self.handle.next_sync();
        let packet =
            Request::new(request::WATCH_ONCE, sync, None).str(key::EVENT_KEY, key).finish();
        self.handle.request(sync, packet).await?.data()
    }

    /// What the handshake negotiated: server version, protocol version, features.
    pub fn server_info(&self) -> ServerInfo<'_> {
        ServerInfo { negotiated: self.handle.negotiated() }
    }

    /// The highest schema version seen on this connection so far.
    ///
    /// Bumps whenever the server changes its schema. Compare across calls to
    /// know when a cached space or index layout may be stale.
    pub fn schema_version(&self) -> u64 {
        self.handle.schema_version()
    }

    /// Close the connection, letting in-flight requests finish first.
    ///
    /// Idempotent, and not required — dropping the last [`Client`] closes the
    /// connection too. Call it when you want to await a clean shutdown.
    pub async fn close(&self) {
        self.handle.close().await;
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("server", &self.handle.negotiated().server_version)
            .finish_non_exhaustive()
    }
}

/// What the server told us about itself during the handshake.
#[derive(Debug, Clone, Copy)]
pub struct ServerInfo<'a> {
    negotiated: &'a Negotiated,
}

impl ServerInfo<'_> {
    /// The server's version string, e.g. `"3.8.0"`.
    pub fn version(&self) -> &str {
        &self.negotiated.server_version
    }

    /// The iproto protocol version the server advertised (0 if pre-2.10).
    pub const fn protocol_version(&self) -> u64 {
        self.negotiated.protocol_version
    }

    /// Whether the server negotiated `feature`.
    pub fn supports(&self, feature: Feature) -> bool {
        self.negotiated.features.contains(&feature)
    }
}

/// Typed CRUD over one space, addressed by name.
///
/// `T` is the tuple type. It is used as the argument to writes and the
/// element type of reads, through `serde` — a struct with named fields, a
/// plain tuple, or [`Value`](crate::Value) when the shape is dynamic.
#[derive(Clone)]
pub struct Space<T> {
    handle: Handle,
    name: Arc<String>,
    _tuple: std::marker::PhantomData<fn() -> T>,
}

impl<T> Space<T> {
    /// Address a secondary index of this space by name for a [`select`](Index::select).
    ///
    /// The primary index is reached through the `Space` methods directly;
    /// this is for the others.
    pub fn index(&self, name: impl Into<String>) -> Index<T> {
        Index {
            handle: self.handle.clone(),
            space: Arc::clone(&self.name),
            reference: IndexRef::Name(name.into()),
            _tuple: std::marker::PhantomData,
        }
    }

    /// Start a select over the primary index. See [`Select`].
    ///
    /// ```no_run
    /// # use tarant::{Client, Iter};
    /// # #[derive(serde::Deserialize)] struct User;
    /// # async fn demo(client: Client) -> tarant::Result<()> {
    /// let users = client.space::<User>("users");
    /// let page: Vec<User> = users.select(()).iterator(Iter::All).limit(50).await?;
    /// # Ok(()) }
    /// ```
    pub fn select(&self, key: impl Key) -> Select<T> {
        Select::new(self.handle.clone(), Arc::clone(&self.name), IndexRef::Id(0), key)
    }
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> Space<T> {
    /// Insert `tuple`. Fails with [`ErrorCode::TUPLE_FOUND`] if the primary key exists.
    ///
    /// [`ErrorCode::TUPLE_FOUND`]: crate::ErrorCode::TUPLE_FOUND
    pub async fn insert(&self, tuple: &T) -> Result<()> {
        self.write(request::INSERT, tuple).await
    }

    /// Insert `tuple`, or overwrite the one with the same primary key.
    pub async fn replace(&self, tuple: &T) -> Result<()> {
        self.write(request::REPLACE, tuple).await
    }

    async fn write(&self, ty: u64, tuple: &T) -> Result<()> {
        let sync = self.handle.next_sync();
        let packet = Request::new(ty, sync, None)
            .str(key::SPACE_NAME, &self.name)
            .serialized(key::TUPLE, tuple, true)?
            .finish();
        self.handle.request(sync, packet).await.map(drop)
    }

    /// Fetch the tuple whose primary key equals `key`, if any.
    ///
    /// `key` follows the [`Key`] rules: a scalar for a one-field primary key,
    /// a tuple for a composite one.
    pub async fn get(&self, key: impl Key) -> Result<Option<T>> {
        let rows: Vec<T> = self.select(key).iterator(Iter::Eq).limit(1).await?;
        Ok(rows.into_iter().next())
    }

    /// Delete the tuple with primary key `key`, returning it if it existed.
    pub async fn delete(&self, key: impl Key) -> Result<Option<T>> {
        let sync = self.handle.next_sync();
        let packet = Request::new(request::DELETE, sync, None)
            .str(key::SPACE_NAME, &self.name)
            .uint(key::INDEX_ID, 0)
            .raw(key::KEY, &encode_array(&key)?)
            .finish();
        let rows: Vec<T> = self.handle.request(sync, packet).await?.data()?;
        Ok(rows.into_iter().next())
    }

    /// Apply `ops` to the tuple with primary key `key`, returning the result.
    ///
    /// Returns `Ok(None)` if no tuple matched. See [`Update`] for the operations.
    pub async fn update(&self, key: impl Key, ops: Update) -> Result<Option<T>> {
        let sync = self.handle.next_sync();
        let packet = Request::new(request::UPDATE, sync, None)
            .str(key::SPACE_NAME, &self.name)
            .uint(key::INDEX_ID, 0)
            .uint(key::INDEX_BASE, 1)
            .raw(key::KEY, &encode_array(&key)?)
            .raw_array(key::TUPLE, &ops.into_ops()?)
            .finish();
        let rows: Vec<T> = self.handle.request(sync, packet).await?.data()?;
        Ok(rows.into_iter().next())
    }

    /// Insert `tuple`, or apply `ops` to it if its primary key already exists.
    ///
    /// Upsert never returns the tuple and never reports a "not found": it is
    /// a blind write, exactly as Tarantool defines it.
    pub async fn upsert(&self, tuple: &T, ops: Update) -> Result<()> {
        let sync = self.handle.next_sync();
        let packet = Request::new(request::UPSERT, sync, None)
            .str(key::SPACE_NAME, &self.name)
            .uint(key::INDEX_BASE, 1)
            .serialized(key::TUPLE, tuple, true)?
            .raw_array(key::OPS, &ops.into_ops()?)
            .finish();
        self.handle.request(sync, packet).await.map(drop)
    }
}

impl<T> std::fmt::Debug for Space<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Space").field("name", &self.name).finish_non_exhaustive()
    }
}

/// A secondary index, reached with [`Space::index`].
#[derive(Clone)]
pub struct Index<T> {
    handle: Handle,
    space: Arc<String>,
    reference: IndexRef,
    _tuple: std::marker::PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned + Send + 'static> Index<T> {
    /// Start a select over this index. See [`Select`].
    pub fn select(&self, key: impl Key) -> Select<T> {
        Select::new(self.handle.clone(), Arc::clone(&self.space), self.reference.clone(), key)
    }
}

impl<T> std::fmt::Debug for Index<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Index")
            .field("space", &self.space)
            .field("index", &self.reference)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
enum IndexRef {
    Id(u64),
    Name(String),
}

/// A select in the making.
///
/// Set the iterator, limit and offset, then `.await` it — [`Select`]
/// implements [`IntoFuture`](std::future::IntoFuture), so there is no
/// `.execute()` to remember. The default is `Iter::Eq` with no limit.
///
/// ```no_run
/// # use tarant::{Client, Iter};
/// # #[derive(serde::Deserialize)] struct Event;
/// # async fn demo(client: Client) -> tarant::Result<()> {
/// let events = client.space::<Event>("events");
/// let recent: Vec<Event> = events
///     .index("ts")
///     .select(1_700_000_000u64)
///     .iterator(Iter::Ge)
///     .limit(100)
///     .await?;
/// # Ok(()) }
/// ```
#[must_use = "a Select does nothing until awaited"]
pub struct Select<T> {
    handle: Handle,
    space: Arc<String>,
    index: IndexRef,
    key: Result<Vec<u8>>,
    iter: Iter,
    limit: Option<u64>,
    offset: u64,
    after: Option<String>,
    fetch_position: bool,
    _tuple: std::marker::PhantomData<fn() -> T>,
}

impl<T> Select<T> {
    fn new(handle: Handle, space: Arc<String>, index: IndexRef, key: impl Key) -> Self {
        Self {
            handle,
            space,
            index,
            key: encode_array(&key),
            iter: Iter::Eq,
            limit: None,
            offset: 0,
            after: None,
            fetch_position: false,
            _tuple: std::marker::PhantomData,
        }
    }

    /// Choose how the index is walked. Default [`Iter::Eq`].
    pub const fn iterator(mut self, iter: Iter) -> Self {
        self.iter = iter;
        self
    }

    /// Return at most `limit` tuples. Default: no limit.
    pub const fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Skip the first `offset` matches. Default 0.
    pub const fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    /// Resume after the position returned by a previous [`page`](Self::page).
    ///
    /// Cursor pagination, and the only kind that is correct under concurrent
    /// writes: the server resumes from the tuple the token names, so inserts
    /// and deletes elsewhere in the index cannot make a row appear twice or
    /// go missing. Prefer it over [`offset`](Self::offset) for anything that
    /// walks a whole space.
    pub fn after(mut self, position: impl Into<String>) -> Self {
        self.after = Some(position.into());
        self
    }

    /// Run the select and return the rows together with a cursor.
    ///
    /// ```no_run
    /// # use tarant::{Client, Iter};
    /// # #[derive(serde::Deserialize)] struct Event;
    /// # async fn demo(client: Client) -> tarant::Result<()> {
    /// let events = client.space::<Event>("events");
    /// let mut cursor: Option<String> = None;
    /// loop {
    ///     let mut select = events.select(()).iterator(Iter::All).limit(500);
    ///     if let Some(position) = &cursor {
    ///         select = select.after(position);
    ///     }
    ///     let page = select.page().await?;
    ///     if page.rows.is_empty() {
    ///         break;
    ///     }
    ///     // ... process page.rows ...
    ///     cursor = page.position;
    /// }
    /// # Ok(()) }
    /// ```
    pub const fn page(mut self) -> Paged<T> {
        self.fetch_position = true;
        Paged(self)
    }

    fn build(self) -> Result<(Handle, u64, Bytes)> {
        let key = self.key?;
        let sync = self.handle.next_sync();
        let mut req = Request::new(request::SELECT, sync, None);
        req = match &self.index {
            IndexRef::Id(id) => req.str(key::SPACE_NAME, &self.space).uint(key::INDEX_ID, *id),
            IndexRef::Name(name) => {
                req.str(key::SPACE_NAME, &self.space).str(key::INDEX_NAME, name)
            }
        };
        req = req.uint(key::ITERATOR, self.iter.code());
        if self.offset > 0 {
            req = req.uint(key::OFFSET, self.offset);
        }
        req = req.uint(key::LIMIT, self.limit.unwrap_or_else(|| u64::from(u32::MAX)));
        req = req.raw(key::KEY, &key);
        if let Some(after) = &self.after {
            req = req.str(key::AFTER_POSITION, after);
        }
        if self.fetch_position {
            req = req.bool(key::FETCH_POSITION, true);
        }
        Ok((self.handle, sync, req.finish()))
    }
}

/// One page of a [`Select::page`] walk: the rows, and where to resume.
#[derive(Debug, Clone)]
pub struct Page<T> {
    /// The tuples this page returned, in index order.
    pub rows: Vec<T>,
    /// Cursor to hand to [`Select::after`] for the next page.
    ///
    /// `None` when the index has no more tuples after this page.
    pub position: Option<String>,
}

/// A [`Select`] that will return a [`Page`]. Await it.
#[must_use = "a Paged select does nothing until awaited"]
#[derive(Debug)]
pub struct Paged<T>(Select<T>);

impl<T: DeserializeOwned + Send + 'static> std::future::IntoFuture for Paged<T> {
    type Output = Result<Page<T>>;
    type IntoFuture = std::pin::Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (handle, sync, packet) = self.0.build()?;
            let response = handle.request(sync, packet).await?;
            let position = response.position().map(str::to_owned);
            Ok(Page { rows: response.data()?, position })
        })
    }
}

impl<T: DeserializeOwned + Send + 'static> std::future::IntoFuture for Select<T> {
    type Output = Result<Vec<T>>;
    type IntoFuture = std::pin::Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (handle, sync, packet) = self.build()?;
            handle.request(sync, packet).await?.data()
        })
    }
}

impl<T> std::fmt::Debug for Select<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Select")
            .field("space", &self.space)
            .field("index", &self.index)
            .field("iter", &self.iter)
            .field("limit", &self.limit)
            .field("offset", &self.offset)
            .field("after", &self.after)
            .finish_non_exhaustive()
    }
}

/// How a transaction behaves: isolation, and how long the server waits.
///
/// `Isolation` converts into this, so [`Stream::begin`] takes either.
#[derive(Debug, Clone, Copy, Default)]
#[must_use = "TxOptions does nothing until passed to `begin`"]
pub struct TxOptions {
    isolation: Isolation,
    timeout: Option<std::time::Duration>,
}

impl TxOptions {
    /// Server defaults: `box.cfg.txn_isolation`, and `box.cfg.txn_timeout`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the isolation level.
    pub const fn isolation(mut self, isolation: Isolation) -> Self {
        self.isolation = isolation;
        self
    }

    /// Roll the transaction back automatically after `timeout`.
    ///
    /// The server enforces this, so an abandoned transaction cannot hold
    /// locks forever even if the client disappears.
    pub const fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl From<Isolation> for TxOptions {
    fn from(isolation: Isolation) -> Self {
        Self::new().isolation(isolation)
    }
}

/// An ordered request sequence, and the scope of an interactive transaction.
///
/// A bare stream guarantees sequential execution. Wrap requests in
/// [`begin`](Self::begin) … [`commit`](Self::commit) to make them atomic; if
/// the [`Stream`] is dropped mid-transaction, a rollback is sent for you.
///
/// A stream carries no spaces of its own yet; issue DML through
/// [`call`](Self::call) or the low-level requests on the parent client while
/// the transaction is open. (Typed DML on a stream is a planned addition.)
pub struct Stream {
    handle: Handle,
    id: u64,
    in_transaction: bool,
}

impl Stream {
    /// Begin a transaction on this stream.
    ///
    /// Takes anything that becomes [`TxOptions`]: an [`Isolation`] on its own,
    /// or the builder when a timeout is wanted too.
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # use tarant::{Client, Isolation, TxOptions};
    /// # async fn demo(client: Client) -> tarant::Result<()> {
    /// let mut tx = client.stream();
    /// tx.begin(Isolation::ReadConfirmed).await?;
    /// // ... requests on `tx` ...
    /// tx.commit().await?;
    ///
    /// let mut bounded = client.stream();
    /// bounded.begin(TxOptions::new().timeout(Duration::from_secs(3))).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Requires the server to support transactions (Tarantool 2.10+).
    pub async fn begin(&mut self, options: impl Into<TxOptions>) -> Result<()> {
        if !self.handle.supports(Feature::Transactions) {
            return Err(Error::Unsupported("transactions (server is older than 2.10)"));
        }
        let options = options.into();
        let sync = self.handle.next_sync();
        let mut req = Request::new(request::BEGIN, sync, Some(self.id));
        if !matches!(options.isolation, Isolation::Default) {
            req = req.uint(key::TXN_ISOLATION, options.isolation.code());
        }
        if let Some(timeout) = options.timeout {
            req = req.f64(key::TIMEOUT, timeout.as_secs_f64());
        }
        self.handle.request(sync, req.finish()).await?;
        self.in_transaction = true;
        Ok(())
    }

    /// Commit the transaction on this stream.
    pub async fn commit(&mut self) -> Result<()> {
        let sync = self.handle.next_sync();
        let packet = Request::new(request::COMMIT, sync, Some(self.id)).finish();
        self.handle.request(sync, packet).await?;
        self.in_transaction = false;
        Ok(())
    }

    /// Roll the transaction on this stream back.
    pub async fn rollback(&mut self) -> Result<()> {
        let sync = self.handle.next_sync();
        let packet = Request::new(request::ROLLBACK, sync, Some(self.id)).finish();
        self.handle.request(sync, packet).await?;
        self.in_transaction = false;
        Ok(())
    }

    /// Call a stored function within this stream's ordering (and transaction, if open).
    pub async fn call<R: DeserializeOwned>(&self, name: &str, args: impl Args) -> Result<R> {
        let sync = self.handle.next_sync();
        let packet = Request::new(request::CALL, sync, Some(self.id))
            .str(key::FUNCTION_NAME, name)
            .raw(key::TUPLE, &encode_array(&args)?)
            .finish();
        self.handle.request(sync, packet).await?.data()
    }

    /// Evaluate Lua within this stream's ordering (and transaction, if open).
    ///
    /// The stream's guarantee is what makes this useful in a transaction: the
    /// server will not begin this expression until the previous request on
    /// the same stream has finished.
    pub async fn eval<R: DeserializeOwned>(&self, expr: &str, args: impl Args) -> Result<R> {
        let sync = self.handle.next_sync();
        let packet = Request::new(request::EVAL, sync, Some(self.id))
            .str(key::EXPR, expr)
            .raw(key::TUPLE, &encode_array(&args)?)
            .finish();
        self.handle.request(sync, packet).await?.data()
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        if self.in_transaction {
            // Best-effort: fire a rollback so an abandoned transaction does
            // not hold locks until its timeout. Nothing to await in Drop.
            let packet =
                Request::new(request::ROLLBACK, self.handle.next_sync(), Some(self.id)).finish();
            self.handle.fire(packet);
        }
    }
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stream")
            .field("id", &self.id)
            .field("in_transaction", &self.in_transaction)
            .finish_non_exhaustive()
    }
}
