//! How to reach a server, and how patient to be about it.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crate::error::{Error, Result};

/// Default iproto port.
pub const DEFAULT_PORT: u16 = 3301;

/// Connection settings for a [`Client`](crate::Client).
///
/// Build one from a URL with [`ConnectOptions::parse`] (or `str::parse`),
/// or start from [`ConnectOptions::new`] and adjust with the setters. Every
/// setting has a default that suits a service talking to a nearby instance.
///
/// ```
/// use std::time::Duration;
/// use tarant::ConnectOptions;
///
/// let options = "tarantool://app:secret@db.internal:3301"
///     .parse::<ConnectOptions>()
///     .unwrap()
///     .request_timeout(Duration::from_secs(5));
/// assert_eq!(options.user_name(), Some("app"));
/// ```
#[derive(Clone)]
pub struct ConnectOptions {
    addr: String,
    user: Option<String>,
    password: Option<String>,
    connect_timeout: Duration,
    request_timeout: Option<Duration>,
    reconnect: Reconnect,
    tcp_nodelay: bool,
    max_packet: usize,
    keepalive: Option<Duration>,
}

impl ConnectOptions {
    /// Options for `addr`, as the `guest` user, with defaults everywhere else.
    ///
    /// The address may be `host`, `host:port`, or an IPv6 literal in any of
    /// its spellings: `"::1"`, `"[::1]"` and `"[::1]:3302"` all work. A bare
    /// host or a bare/bracketed literal gets the default port appended; a
    /// bracketed literal with a port is kept as is.
    pub fn new(addr: impl Into<String>) -> Self {
        let addr = addr.into();
        let addr = if addr.starts_with('[') {
            if addr.contains("]:") { addr } else { format!("{addr}:{DEFAULT_PORT}") }
        } else {
            match addr.matches(':').count() {
                0 => format!("{addr}:{DEFAULT_PORT}"),
                1 => addr,
                _ => format!("[{addr}]:{DEFAULT_PORT}"),
            }
        };
        Self {
            addr,
            user: None,
            password: None,
            connect_timeout: Duration::from_secs(5),
            request_timeout: None,
            reconnect: Reconnect::default(),
            tcp_nodelay: true,
            max_packet: crate::codec::DEFAULT_MAX_PACKET,
            keepalive: None,
        }
    }

    /// Parse a `tarantool://[user[:password]@]host[:port]` URL.
    ///
    /// The scheme is optional. User and password are percent-decoded, so a
    /// password containing `@` or `:` is written as `%40` / `%3A`.
    pub fn parse(url: &str) -> Result<Self> {
        let rest = url.strip_prefix("tarantool://").unwrap_or(url);
        let (credentials, host) = match rest.rsplit_once('@') {
            Some((credentials, host)) => (Some(credentials), host),
            None => (None, rest),
        };
        if host.is_empty() {
            return Err(Error::Url(String::from("no host")));
        }
        if host.contains('/') {
            return Err(Error::Url(format!("unexpected path after host `{host}`")));
        }
        let mut options = Self::new(host);
        if let Some(credentials) = credentials {
            let (user, password) = match credentials.split_once(':') {
                Some((user, password)) => (user, Some(password)),
                None => (credentials, None),
            };
            options.user = Some(percent_decode(user, "user name")?);
            options.password = password.map(|p| percent_decode(p, "password")).transpose()?;
        }
        Ok(options)
    }

    /// Authenticate as `user`. Without a password the user must have none.
    #[must_use]
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// The password for [`user`](Self::user).
    #[must_use]
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// How long to wait for the TCP connection and the handshake. Default 5 s.
    #[must_use]
    pub const fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Fail any request that has no reply within `timeout`.
    ///
    /// Off by default: a request waits as long as the connection lives.
    /// Timing out does not cancel the request on the server.
    #[must_use]
    pub const fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// The largest response frame accepted, in bytes.
    ///
    /// Defaults to 2 GiB, the protocol's own maximum; lower it when the
    /// server is not fully trusted so a hostile length prefix cannot make
    /// the client buffer more than it should.
    #[must_use]
    pub const fn max_packet_size(mut self, bytes: usize) -> Self {
        self.max_packet = bytes;
        self
    }

    /// Send a keepalive `IPROTO_PING` after `interval` without a reply, and
    /// treat the link as dead if another `interval` passes with still
    /// nothing received. Off by default.
    ///
    /// Any packet from the server resets the timer. Beware false positives:
    /// a server whose TX thread is blocked longer than the interval will
    /// look dead, and the client will drop a working connection.
    #[must_use]
    pub const fn keepalive(mut self, interval: Duration) -> Self {
        self.keepalive = Some(interval);
        self
    }

    /// What to do when the connection drops. Default: retry with backoff.
    #[must_use]
    pub const fn reconnect(mut self, policy: Reconnect) -> Self {
        self.reconnect = policy;
        self
    }

    /// Disable Nagle's algorithm on the socket. On by default; iproto is
    /// request/response and latency-bound.
    #[must_use]
    pub const fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.tcp_nodelay = enabled;
        self
    }

    pub(crate) fn addr(&self) -> &str {
        &self.addr
    }

    /// The user this connection authenticates as, if any.
    pub fn user_name(&self) -> Option<&str> {
        self.user.as_deref()
    }

    pub(crate) fn password_str(&self) -> &str {
        self.password.as_deref().unwrap_or_default()
    }

    pub(crate) const fn connect_timeout_value(&self) -> Duration {
        self.connect_timeout
    }

    pub(crate) const fn request_timeout_value(&self) -> Option<Duration> {
        self.request_timeout
    }

    pub(crate) const fn reconnect_policy(&self) -> Reconnect {
        self.reconnect
    }

    pub(crate) const fn nodelay(&self) -> bool {
        self.tcp_nodelay
    }

    pub(crate) const fn max_packet(&self) -> usize {
        self.max_packet
    }

    pub(crate) const fn keepalive_interval(&self) -> Option<Duration> {
        self.keepalive
    }
}

impl FromStr for ConnectOptions {
    type Err = Error;

    fn from_str(url: &str) -> Result<Self> {
        Self::parse(url)
    }
}

impl fmt::Debug for ConnectOptions {
    /// Never prints the password.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectOptions")
            .field("addr", &self.addr)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("reconnect", &self.reconnect)
            .field("tcp_nodelay", &self.tcp_nodelay)
            .field("max_packet", &self.max_packet)
            .field("keepalive", &self.keepalive)
            .finish()
    }
}

/// Reconnection policy after the connection is lost.
///
/// While reconnecting, new requests wait for the link to come back (or for
/// the request timeout); requests that were in flight when the link broke
/// fail with [`Error::Closed`], because the client cannot know whether the
/// server executed them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reconnect {
    /// Retry with exponential backoff between `min` and `max`, forever.
    Backoff {
        /// First delay; doubles on each failure.
        min: Duration,
        /// Ceiling for the delay.
        max: Duration,
    },
    /// Give up: the client becomes permanently closed.
    Never,
}

impl Default for Reconnect {
    fn default() -> Self {
        Self::Backoff { min: Duration::from_millis(100), max: Duration::from_secs(5) }
    }
}

fn percent_decode(input: &str, what: &'static str) -> Result<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = bytes
                    .get(i + 1..i + 3)
                    .and_then(|h| std::str::from_utf8(h).ok())
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                    .ok_or_else(|| Error::Url(format!("bad percent-escape in {what}")))?;
                out.push(hex);
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| Error::Url(format!("{what} is not valid UTF-8")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_url() {
        let o = ConnectOptions::parse("tarantool://app:s%40cret@db:3302").unwrap();
        assert_eq!(o.addr(), "db:3302");
        assert_eq!(o.user_name(), Some("app"));
        assert_eq!(o.password_str(), "s@cret");
    }

    #[test]
    fn defaults_port_and_guest() {
        let o: ConnectOptions = "localhost".parse().unwrap();
        assert_eq!(o.addr(), "localhost:3301");
        assert_eq!(o.user_name(), None);
        assert_eq!(o.password_str(), "");
    }

    #[test]
    fn rejects_paths_and_empty_hosts() {
        assert!(matches!(ConnectOptions::parse("tarantool://"), Err(Error::Url(_))));
        assert!(matches!(ConnectOptions::parse("tarantool://h:1/db"), Err(Error::Url(_))));
    }

    #[test]
    fn errors_never_echo_the_url_or_credentials() {
        let err = ConnectOptions::parse("tarantool://app:secret@host/db").unwrap_err();
        assert!(!err.to_string().contains("secret"), "{err}");
        let err = ConnectOptions::parse("tarantool://app:se%zzcret@host").unwrap_err();
        assert!(!err.to_string().contains("se%zz"), "{err}");
        assert_eq!(err.to_string(), "invalid connection url: bad percent-escape in password");
    }

    #[test]
    fn new_normalises_ipv6_literals() {
        assert_eq!(ConnectOptions::new("::1").addr(), "[::1]:3301");
        assert_eq!(ConnectOptions::new("[::1]").addr(), "[::1]:3301");
        assert_eq!(ConnectOptions::new("[::1]:3302").addr(), "[::1]:3302");
        assert_eq!(ConnectOptions::new("db").addr(), "db:3301");
        assert_eq!(ConnectOptions::new("db:1").addr(), "db:1");
    }

    #[test]
    fn debug_hides_the_password() {
        let o = ConnectOptions::new("h").user("u").password("hunter2");
        assert!(!format!("{o:?}").contains("hunter2"));
    }
}
