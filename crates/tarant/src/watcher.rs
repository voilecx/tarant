//! Server-pushed notifications for a broadcast key.
//!
//! A [`Watcher`] is a subscription to one key set with `box.broadcast()` on
//! the server. It behaves like a `tokio::sync::watch` receiver: it holds the
//! latest value, coalesces updates the consumer was too slow to see, and
//! survives reconnects — the connection task re-subscribes for you, so a
//! dropped link is a gap in delivery, never a lost subscription.

use bytes::Bytes;
use serde::de::DeserializeOwned;
use tokio::sync::watch;

use crate::connection::Handle;

/// `MessagePack` `nil`: what a key broadcast with no value decodes from.
const NIL: [u8; 1] = [0xc0];
use crate::error::{Error, Result};

/// A live subscription to a broadcast key.
///
/// ```no_run
/// # use tarant::Client;
/// # async fn demo(client: Client) -> tarant::Result<()> {
/// let mut config = client.watch("app.config").await?;
/// loop {
///     let value: serde_json::Value = config.get()?;
///     // ... apply the new config ...
///     if config.changed().await.is_err() {
///         break; // the client was closed
///     }
/// }
/// # Ok(()) }
/// ```
///
/// Dropping the watcher unsubscribes; when the last watcher for a key goes,
/// the client tells the server to stop sending.
pub struct Watcher {
    handle: Handle,
    id: u64,
    key: String,
    receiver: watch::Receiver<Option<Bytes>>,
}

impl Watcher {
    pub(crate) async fn subscribe(handle: Handle, key: String) -> Result<Self> {
        let (id, mut receiver) = handle.watch(key.clone()).await?;
        // The first notification arrives right after registration; wait for
        // it so `get` has a value to return without blocking.
        if receiver.borrow().is_none() {
            receiver.changed().await.map_err(|_| Error::Closed)?;
        }
        Ok(Self { handle, id, key, receiver })
    }

    /// The current value, decoded as `R`.
    ///
    /// Returns whatever the most recent `box.broadcast()` carried. A key that
    /// was broadcast with no value decodes as it would from `nil` — often
    /// `Option::None` or unit.
    pub fn get<R: DeserializeOwned>(&self) -> Result<R> {
        // Clone out (a `Bytes` clone is a refcount bump) and release the
        // watch lock before decoding: deserialising is the caller's work and
        // must not block the connection task from publishing the next event.
        let latest = self.receiver.borrow().clone();
        let bytes = latest.as_deref().unwrap_or(&NIL);
        rmp_serde::from_slice(bytes).map_err(Error::decode)
    }

    /// Wait until the value changes from the one last observed.
    ///
    /// Returns `Ok(())` when a new value is ready, or [`Error::Closed`] if the
    /// client shut down. Mirrors [`tokio::sync::watch::Receiver::changed`]:
    /// call [`get`](Self::get) afterwards to read it.
    pub async fn changed(&mut self) -> Result<()> {
        self.receiver.changed().await.map_err(|_| Error::Closed)
    }

    /// The key this watches.
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.handle.unwatch(self.id, std::mem::take(&mut self.key));
    }
}

impl std::fmt::Debug for Watcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Watcher").field("key", &self.key).finish_non_exhaustive()
    }
}
