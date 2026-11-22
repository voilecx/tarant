//! Integration tests against a scripted in-process iproto server.
//!
//! A real Tarantool cannot be told to misbehave on cue — answer a request
//! with garbage UTF-8, announce a shutdown, stall a handshake — so these
//! tests speak the wire protocol themselves: a [`FakeServer`] accepts the
//! connection, writes the greeting, answers `IPROTO_ID` with a full feature
//! set, and then reads/writes frames exactly as the test scripts. Only the
//! public [`Client`] API is exercised; every test body is wrapped in a 10 s
//! timeout so a hung client becomes a failure, not a stall.

use std::io;
use std::time::Duration;

use rmpv::Value;
use tarant::{Client, ConnectOptions, Error, Isolation, Reconnect};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};

const LIMIT: Duration = Duration::from_secs(10);

mod ty {
    pub const EVAL: u64 = 0x08;
    pub const BEGIN: u64 = 0x0e;
    pub const COMMIT: u64 = 0x0f;
    pub const ROLLBACK: u64 = 0x10;
    pub const PING: u64 = 0x40;
    pub const ID: u64 = 0x49;
    pub const WATCH: u64 = 0x4a;
    pub const EVENT: u64 = 0x4c;
}

/// One request or reply, header keys already lifted out.
struct Frame {
    ty: u64,
    sync: u64,
    body: Value,
}

/// The server end of one accepted socket.
struct Conn {
    stream: TcpStream,
    shutdown_announced: bool,
}

/// Serialise a frame: `0xce` + u32 length, then header map, then body bytes.
fn encode_frame(ty: u64, sync: u64, body: &[u8]) -> Vec<u8> {
    let mut header = Vec::new();
    rmpv::encode::write_value(
        &mut header,
        &Value::Map(vec![
            (Value::from(0x00u64), Value::from(ty)),
            (Value::from(0x01u64), Value::from(sync)),
        ]),
    )
    .expect("header encodes");
    let mut frame = Vec::with_capacity(5 + header.len() + body.len());
    frame.push(0xce);
    frame.extend_from_slice(&u32::try_from(header.len() + body.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(body);
    frame
}

fn ok_body() -> Vec<u8> {
    let mut body = Vec::new();
    rmpv::encode::write_value(&mut body, &Value::Map(vec![])).expect("map encodes");
    body
}

fn event_body(key: &str, data: Value) -> Vec<u8> {
    let mut body = Vec::new();
    rmpv::encode::write_value(
        &mut body,
        &Value::Map(vec![(Value::from(0x57u64), Value::from(key)), (Value::from(0x58u64), data)]),
    )
    .expect("map encodes");
    body
}

impl Conn {
    /// The 128-byte greeting: version line padded to 64, salt line to 64.
    async fn write_greeting(&mut self) {
        let mut greeting = Vec::with_capacity(128);
        greeting
            .extend_from_slice(b"Tarantool 3.0.0 (Binary) aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        greeting.resize(63, b' ');
        greeting.push(b'\n');
        greeting.extend_from_slice(b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
        greeting.resize(128, b' ');
        self.stream.write_all(&greeting).await.expect("greeting write");
    }

    /// Read one raw frame; `None` on a clean EOF.
    async fn read_frame(&mut self) -> Option<Frame> {
        let mut prefix = [0u8; 5];
        match self.stream.read_exact(&mut prefix).await {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(err) => panic!("read failed: {err}"),
        }
        assert_eq!(prefix[0], 0xce, "frame length must be a msgpack u32");
        let len = u32::from_be_bytes(prefix[1..5].try_into().unwrap()) as usize;
        let mut buf = vec![0u8; len];
        self.stream.read_exact(&mut buf).await.expect("frame body");
        let mut slice = buf.as_slice();
        let header = rmpv::decode::read_value(&mut slice).expect("header map");
        let body = rmpv::decode::read_value(&mut slice).expect("body map");
        let mut ty = None;
        let mut sync = 0;
        for (k, v) in header.as_map().expect("header is a map") {
            match k.as_u64() {
                Some(0x00) => ty = v.as_u64(),
                Some(0x01) => sync = v.as_u64().unwrap_or(0),
                _ => {}
            }
        }
        Some(Frame { ty: ty.expect("request type"), sync, body })
    }

    /// Read the next frame a test cares about. The client's housekeeping
    /// `box.shutdown` subscription — a WATCH at connect, an ack-WATCH per
    /// event — is answered/acknowledged here and never surfaces.
    async fn next(&mut self) -> Option<Frame> {
        loop {
            let frame = self.read_frame().await?;
            if frame.ty == ty::WATCH && Self::is_shutdown_watch(&frame) {
                if !self.shutdown_announced {
                    self.shutdown_announced = true;
                    self.event("box.shutdown", Value::from(false)).await;
                }
                continue;
            }
            return Some(frame);
        }
    }

    /// Collect business frames until the socket goes quiet for `within`.
    async fn drain(&mut self, within: Duration) -> Vec<Frame> {
        let mut seen = Vec::new();
        let deadline = tokio::time::Instant::now() + within;
        while let Ok(frame) = timeout(deadline - tokio::time::Instant::now(), self.next()).await {
            match frame {
                Some(frame) => seen.push(frame),
                None => break,
            }
        }
        seen
    }

    async fn write_frame(&mut self, ty: u64, sync: u64, body: &[u8]) {
        self.stream.write_all(&encode_frame(ty, sync, body)).await.expect("frame write");
    }

    async fn ok(&mut self, sync: u64) {
        self.write_frame(0x00, sync, &ok_body()).await;
    }

    async fn event(&mut self, key: &str, data: Value) {
        self.write_frame(ty::EVENT, 0, &event_body(key, data)).await;
    }

    /// Answer the `IPROTO_ID` feature exchange as a 3.x server would.
    async fn handshake(&mut self) {
        let frame = self.read_frame().await.expect("IPROTO_ID");
        assert_eq!(frame.ty, ty::ID, "first request must be IPROTO_ID");
        let mut body = Vec::new();
        rmpv::encode::write_value(
            &mut body,
            &Value::Map(vec![
                (Value::from(0x54u64), Value::from(10u64)),
                (
                    Value::from(0x55u64),
                    Value::from(
                        [0u64, 1, 2, 3, 4, 5, 6, 11, 12]
                            .iter()
                            .map(|c| Value::from(*c))
                            .collect::<Vec<_>>(),
                    ),
                ),
            ]),
        )
        .expect("id body encodes");
        self.write_frame(0x00, frame.sync, &body).await;
    }

    fn is_shutdown_watch(frame: &Frame) -> bool {
        frame.body.as_map().and_then(|map| {
            map.iter().find(|(k, _)| k.as_u64() == Some(0x57)).map(|(_, v)| v.as_str())
        }) == Some(Some("box.shutdown"))
    }

    /// `true` once the peer closes the socket (read hits EOF).
    async fn closed(&mut self, within: Duration) -> bool {
        timeout(within, async {
            let mut byte = [0u8; 1];
            loop {
                match self.stream.read(&mut byte).await {
                    Ok(0) => return true,
                    Ok(_) => {} // late bytes; keep waiting for EOF
                    Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return true,
                    Err(err) => panic!("read failed: {err}"),
                }
            }
        })
        .await
        .unwrap_or(false)
    }
}

/// Bind a listener; `accept*` yields a [`Conn`].
struct FakeServer {
    listener: TcpListener,
    addr: String,
}

impl FakeServer {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = format!("127.0.0.1:{}", listener.local_addr().unwrap().port());
        Self { listener, addr }
    }

    /// Accept a socket without any greeting — the test drives the schedule.
    async fn accept_raw(&self) -> Conn {
        let (stream, _) = self.listener.accept().await.expect("accept");
        Conn { stream, shutdown_announced: false }
    }

    fn options(&self) -> ConnectOptions {
        ConnectOptions::new(self.addr.clone())
            .connect_timeout(Duration::from_secs(2))
            .request_timeout(Duration::from_secs(3))
    }
}

/// Greet then handshake, in the order `connect` expects.
async fn serve(conn: &mut Conn) {
    conn.write_greeting().await;
    conn.handshake().await;
}

/// Connect a client while accepting its socket: `Client::connect` waits on
/// the greeting, so the two must be driven concurrently.
async fn connect(server: &FakeServer, options: ConnectOptions) -> (Client, Conn) {
    let server = async {
        let mut conn = server.accept_raw().await;
        serve(&mut conn).await;
        conn
    };
    let (client, conn) = tokio::join!(Client::with_options(options), server);
    (client.expect("connect"), conn)
}

/// Accept a reconnection: greeting + handshake, already negotiated.
async fn accept_ready(server: &FakeServer) -> Conn {
    let mut conn = server.accept_raw().await;
    serve(&mut conn).await;
    conn
}

#[tokio::test]
async fn non_utf8_error_fails_only_its_request() {
    timeout(LIMIT, async {
        let server = FakeServer::new().await;
        let options = server.options();
        let (client, mut conn) = connect(&server, options).await;

        // Two concurrent pings; error out the first reply, answer the second.
        let (p1, p2) = {
            let (a, b) = (client.clone(), client.clone());
            (
                tokio::spawn(async move { a.ping().await }),
                tokio::spawn(async move { b.ping().await }),
            )
        };
        let f1 = conn.next().await.unwrap();
        let f2 = conn.next().await.unwrap();
        assert_eq!(f1.ty, ty::PING);
        assert_eq!(f2.ty, ty::PING);
        // {0x31: str "\xff\xfe"} — invalid UTF-8 on the wire, hand-built.
        conn.write_frame(0x8020, f1.sync, &[0x81, 0x31, 0xa2, 0xff, 0xfe]).await;
        conn.ok(f2.sync).await;

        let (r1, r2) = (p1.await.unwrap(), p2.await.unwrap());
        // Whichever request arrived first got the error reply.
        let (err, ok) = if matches!(r1, Err(Error::Server(_))) { (r1, r2) } else { (r2, r1) };
        match err {
            Err(Error::Server(err)) => {
                assert!(err.to_string().contains('\u{FFFD}'), "lossy decode: {err}");
            }
            other => panic!("expected Error::Server, got {other:?}"),
        }
        ok.expect("second ping ok");

        // The link survived: one more ping on the same connection.
        let p3 = tokio::spawn({
            let client = client.clone();
            async move { client.ping().await }
        });
        let f3 = conn.next().await.unwrap();
        assert_eq!(f3.ty, ty::PING);
        conn.ok(f3.sync).await;
        p3.await.unwrap().unwrap();
        assert!(conn.drain(Duration::from_millis(100)).await.is_empty());
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn second_watcher_gets_the_current_value() {
    timeout(LIMIT, async {
        let server = FakeServer::new().await;
        let options = server.options();
        let (client, mut conn) = connect(&server, options).await;

        let w1 = tokio::spawn({
            let client = client.clone();
            async move { client.watch("k").await }
        });
        let frame = conn.next().await.unwrap();
        assert_eq!(frame.ty, ty::WATCH);
        assert_eq!(frame.body.as_map().unwrap()[0].1.as_str(), Some("k"));
        conn.event("k", Value::from("v1")).await;
        let w1 = w1.await.unwrap().unwrap();
        assert_eq!(w1.get::<String>().unwrap(), "v1");

        // The event is acked with another WATCH for the key.
        let ack = conn.next().await.unwrap();
        assert_eq!(ack.ty, ty::WATCH);

        // A second subscribe resolves from the cached value — within a
        // second, and without a new subscription WATCH on the wire.
        let w2 = timeout(Duration::from_secs(1), client.watch("k"))
            .await
            .expect("cached watch must resolve")
            .unwrap();
        assert_eq!(w2.get::<String>().unwrap(), "v1");
        let rest = conn.drain(Duration::from_millis(200)).await;
        assert!(
            rest.iter().all(|f| f.ty != ty::WATCH),
            "no extra subscription expected: {:?}",
            rest.iter().map(|f| f.ty).collect::<Vec<_>>()
        );
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn transaction_does_not_survive_reconnect() {
    timeout(LIMIT, async {
        let server = FakeServer::new().await;
        let options = server.options().reconnect(Reconnect::Backoff {
            min: Duration::from_millis(20),
            max: Duration::from_millis(200),
        });
        let (client, conn) = connect(&server, options).await;

        // Answer the BEGIN, then kill the link once it has landed.
        let conn = tokio::spawn(async move {
            let mut conn = conn;
            let frame = conn.next().await.unwrap();
            assert_eq!(frame.ty, ty::BEGIN);
            conn.ok(frame.sync).await;
            conn
        });
        let mut stream = client.stream();
        stream.begin(Isolation::Default).await.expect("begin");
        drop(conn.await.unwrap());

        // The client reconnects and re-handshakes.
        let mut conn = accept_ready(&server).await;

        let err = stream.eval::<u64>("return 1", ()).await.unwrap_err();
        assert!(matches!(err, Error::Closed), "eval after reconnect: {err:?}");
        let err = stream.commit().await.unwrap_err();
        assert!(matches!(err, Error::Closed), "commit after reconnect: {err:?}");
        drop(stream); // its Drop rollback must also stay pinned to generation 0

        // Nothing transaction-shaped may have reached the new link.
        let seen = conn.drain(Duration::from_millis(300)).await;
        for frame in &seen {
            assert!(
                !matches!(frame.ty, ty::EVAL | ty::COMMIT | ty::ROLLBACK),
                "frame {:#x} leaked onto the new connection",
                frame.ty
            );
        }

        // …but the connection itself is healthy.
        let ping = tokio::spawn({
            let client = client.clone();
            async move { client.ping().await }
        });
        let frame = conn.next().await.unwrap();
        assert_eq!(frame.ty, ty::PING);
        conn.ok(frame.sync).await;
        ping.await.unwrap().unwrap();
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn timed_out_deferred_request_is_not_sent() {
    timeout(LIMIT, async {
        let server = FakeServer::new().await;
        let options = server.options().request_timeout(Duration::from_millis(100)).reconnect(
            Reconnect::Backoff { min: Duration::from_millis(50), max: Duration::from_millis(500) },
        );
        let (client, conn) = connect(&server, options).await;
        drop(conn);

        // Second accept stalls the greeting for 400 ms: a request issued in
        // the gap sits deferred through its whole timeout window.
        let mut conn = server.accept_raw().await;
        sleep(Duration::from_millis(150)).await;
        let err = client.ping().await.unwrap_err();
        assert!(matches!(err, Error::Timeout(_)), "expected Timeout, got {err:?}");
        sleep(Duration::from_millis(250)).await; // 400 ms of stalled greeting
        conn.write_greeting().await;
        conn.handshake().await;

        let seen = conn.drain(Duration::from_millis(300)).await;
        assert!(!seen.iter().any(|f| f.ty == ty::PING), "timed-out ping must not reach the server");
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn close_lets_in_flight_requests_finish() {
    timeout(LIMIT, async {
        let server = FakeServer::new().await;
        let options = server.options();
        let (client, mut conn) = connect(&server, options).await;

        let ping = tokio::spawn({
            let client = client.clone();
            async move { client.ping().await }
        });
        let frame = conn.next().await.unwrap();
        assert_eq!(frame.ty, ty::PING);

        let closer = tokio::spawn({
            let client = client.clone();
            async move { client.close().await }
        });
        sleep(Duration::from_millis(100)).await;
        conn.ok(frame.sync).await;

        ping.await.unwrap().expect("in-flight ping finishes");
        closer.await.unwrap();
        assert!(matches!(client.ping().await, Err(Error::Closed)));
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn graceful_shutdown_reconnects() {
    timeout(LIMIT, async {
        let server = FakeServer::new().await;
        let options = server.options().reconnect(Reconnect::Backoff {
            min: Duration::from_millis(20),
            max: Duration::from_millis(200),
        });
        let (client, mut conn) = connect(&server, options).await;

        conn.event("box.shutdown", Value::from(true)).await;
        assert!(conn.closed(Duration::from_secs(2)).await, "client must close the socket");

        let mut conn = accept_ready(&server).await;
        let ping = tokio::spawn({
            let client = client.clone();
            async move { client.ping().await }
        });
        let frame = conn.next().await.unwrap();
        assert_eq!(frame.ty, ty::PING);
        conn.ok(frame.sync).await;
        ping.await.unwrap().unwrap();
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn requests_are_batched() {
    timeout(LIMIT, async {
        let server = FakeServer::new().await;
        let options = server.options();
        let (client, mut conn) = connect(&server, options).await;

        let mut set = tokio::task::JoinSet::new();
        for _ in 0..200 {
            let client = client.clone();
            set.spawn(async move { client.ping().await });
        }
        for _ in 0..200 {
            let frame = conn.next().await.unwrap();
            assert_eq!(frame.ty, ty::PING);
            conn.ok(frame.sync).await;
        }
        while let Some(result) = set.join_next().await {
            result.unwrap().expect("ping");
        }
    })
    .await
    .expect("test timed out");
}
