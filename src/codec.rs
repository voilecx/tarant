//! Framing and packet layout.
//!
//! An iproto packet on the wire is `MP_UINT length` followed by `length`
//! bytes holding two `MessagePack` maps: the header and the body. This module
//! owns both directions of that boundary:
//!
//! * [`Codec`] turns a byte stream into whole packets and back, for use with
//!   `tokio_util::codec::Framed`;
//! * [`Request`] writes a header and lets the caller write a body;
//! * [`Response`] reads a header, locates the interesting parts of the body
//!   (`IPROTO_DATA`, the error, an event) and keeps the bytes around so a
//!   typed decode can happen later, exactly once, straight from the buffer.

use std::collections::{BTreeMap, HashMap};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rmpv::Value;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::{Error, ErrorCode, Result, ServerError};
use crate::iproto::{error as error_key, key, response};
use crate::msgpack::{MapCursor, read_str, read_uint};
use crate::tuple::ArrayLike;

/// Largest packet we are willing to buffer, matching the server's own limit.
const MAX_PACKET_LEN: usize = 2 * 1024 * 1024 * 1024;

/// Length-prefixed packet framing.
#[derive(Debug, Default)]
pub(crate) struct Codec;

impl Decoder for Codec {
    type Item = Response;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Response>> {
        let Some(&marker) = src.first() else { return Ok(None) };
        // The length is a MessagePack unsigned integer; the server always
        // sends the 5-byte `0xce` form, but any width is legal.
        let prefix = match marker {
            0x00..=0x7f => 1,
            0xcc => 2,
            0xcd => 3,
            0xce => 5,
            0xcf => 9,
            other => return Err(Error::protocol(format!("bad packet length marker {other:#04x}"))),
        };
        if src.len() < prefix {
            src.reserve(prefix - src.len());
            return Ok(None);
        }
        let len = match prefix {
            1 => usize::from(marker),
            2 => usize::from(src[1]),
            3 => usize::from(u16::from_be_bytes([src[1], src[2]])),
            5 => u32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize,
            _ => {
                let mut wide = [0u8; 8];
                wide.copy_from_slice(&src[1..9]);
                usize::try_from(u64::from_be_bytes(wide))
                    .map_err(|_| Error::protocol("packet length does not fit in memory"))?
            }
        };
        if len > MAX_PACKET_LEN {
            return Err(Error::protocol(format!("packet of {len} bytes exceeds the 2 GiB limit")));
        }
        if src.len() < prefix + len {
            src.reserve(prefix + len - src.len());
            return Ok(None);
        }
        src.advance(prefix);
        let packet = src.split_to(len).freeze();
        Response::parse(&packet).map(Some)
    }
}

impl Encoder<Bytes> for Codec {
    type Error = Error;

    fn encode(&mut self, packet: Bytes, dst: &mut BytesMut) -> Result<()> {
        let len = u32::try_from(packet.len())
            .map_err(|_| Error::protocol("request exceeds the 4 GiB packet limit"))?;
        dst.reserve(5 + packet.len());
        dst.put_u8(0xce);
        dst.put_u32(len);
        dst.extend_from_slice(&packet);
        Ok(())
    }
}

/// A request under construction: the header is fixed at creation, body
/// entries are appended one `(key, value)` at a time, and `finish` joins the
/// two with the entry count the body map needs.
#[derive(Debug)]
pub(crate) struct Request {
    header: Vec<u8>,
    body: Vec<u8>,
    entries: u32,
}

impl Request {
    /// Begin a request of type `ty`, optionally inside a stream.
    pub(crate) fn new(ty: u64, sync: u64, stream_id: Option<u64>) -> Self {
        let mut header = Vec::with_capacity(24);
        let header_len = 2 + u32::from(stream_id.is_some());
        rmp::encode::write_map_len(&mut header, header_len).expect("vec write");
        put_uint(&mut header, key::REQUEST_TYPE, ty);
        put_uint(&mut header, key::SYNC, sync);
        if let Some(stream_id) = stream_id {
            put_uint(&mut header, key::STREAM_ID, stream_id);
        }
        Self { header, body: Vec::with_capacity(64), entries: 0 }
    }

    fn key(&mut self, k: u8) {
        self.entries += 1;
        rmp::encode::write_uint(&mut self.body, u64::from(k)).expect("vec write");
    }

    pub(crate) fn uint(mut self, k: u8, v: u64) -> Self {
        self.key(k);
        rmp::encode::write_uint(&mut self.body, v).expect("vec write");
        self
    }

    pub(crate) fn str(mut self, k: u8, v: &str) -> Self {
        self.key(k);
        rmp::encode::write_str(&mut self.body, v).expect("vec write");
        self
    }

    pub(crate) fn bool(mut self, k: u8, v: bool) -> Self {
        self.key(k);
        rmp::encode::write_bool(&mut self.body, v).expect("vec write");
        self
    }

    pub(crate) fn f64(mut self, k: u8, v: f64) -> Self {
        self.key(k);
        rmp::encode::write_f64(&mut self.body, v).expect("vec write");
        self
    }

    /// Write `value` with serde. `expect_array` demands that it serialised to
    /// an `MP_ARRAY`, which is what every tuple-, key- and argument-shaped
    /// field must be; a scalar there is a mistake worth catching client-side.
    pub(crate) fn serialized<T: Serialize + ?Sized>(
        mut self,
        k: u8,
        value: &T,
        expect_array: bool,
    ) -> Result<Self> {
        self.key(k);
        let start = self.body.len();
        rmp_serde::encode::write(&mut self.body, value).map_err(Error::encode)?;
        if expect_array && !is_array_marker(self.body.get(start).copied()) {
            return Err(Error::encode(NotAnArray));
        }
        Ok(self)
    }

    /// Write `payload` as the `MessagePack` extension `tag`.
    pub(crate) fn ext(mut self, k: u8, tag: i8, payload: &[u8]) -> Result<Self> {
        self.key(k);
        let len = u32::try_from(payload.len())
            .map_err(|_| Error::protocol("extension payload exceeds 4 GiB"))?;
        rmp::encode::write_ext_meta(&mut self.body, len, tag).expect("vec write");
        self.body.extend_from_slice(payload);
        Ok(self)
    }

    /// Write a tuple-, key- or argument-shaped value straight into the body,
    /// with no intermediate buffer.
    pub(crate) fn array<T: ArrayLike + ?Sized>(mut self, k: u8, value: &T) -> Result<Self> {
        self.key(k);
        value.encode(&mut self.body)?;
        Ok(self)
    }

    /// Write an already-encoded `MessagePack` value verbatim.
    pub(crate) fn raw(mut self, k: u8, msgpack: &[u8]) -> Self {
        self.key(k);
        self.body.extend_from_slice(msgpack);
        self
    }

    /// Write an array made of pre-encoded elements.
    pub(crate) fn raw_array(mut self, k: u8, elements: &[Vec<u8>]) -> Self {
        self.key(k);
        let len = u32::try_from(elements.len()).expect("an array of < 4 billion elements");
        rmp::encode::write_array_len(&mut self.body, len).expect("vec write");
        for element in elements {
            self.body.extend_from_slice(element);
        }
        self
    }

    pub(crate) fn finish(mut self) -> Bytes {
        let mut packet = self.header;
        rmp::encode::write_map_len(&mut packet, self.entries).expect("vec write");
        packet.append(&mut self.body);
        Bytes::from(packet)
    }
}

fn put_uint(buf: &mut Vec<u8>, k: u8, v: u64) {
    rmp::encode::write_uint(buf, u64::from(k)).expect("vec write");
    rmp::encode::write_uint(buf, v).expect("vec write");
}

const fn is_array_marker(marker: Option<u8>) -> bool {
    matches!(marker, Some(0x90..=0x9f | 0xdc | 0xdd))
}

#[derive(Debug)]
struct NotAnArray;

impl std::fmt::Display for NotAnArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "value must serialize to a MessagePack array (a tuple, a struct, a Vec or a slice); \
             a bare scalar is not a tuple — wrap it: `(value,)`",
        )
    }
}

impl std::error::Error for NotAnArray {}

/// What kind of packet a [`Response`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A reply to a request, carrying `IPROTO_DATA`.
    Ok,
    /// An out-of-band `box.session.push()` chunk for a request still running.
    Chunk,
    /// A reply that failed; the error is in [`Response::error`].
    Error,
    /// A server-initiated event for a watched key; has no sync.
    Event,
}

/// A parsed packet: header fields plus located body slices.
#[derive(Debug, Clone)]
pub(crate) struct Response {
    pub(crate) kind: Kind,
    pub(crate) sync: u64,
    pub(crate) schema_version: Option<u64>,
    body: Bytes,
    data: Option<(usize, usize)>,
    position: Option<String>,
    // Boxed so an OK response — the common case — does not carry a 150-byte
    // `ServerError` inline; only the error path pays for the allocation.
    error: Option<Box<ServerError>>,
    event_key: Option<String>,
    event_data: Option<(usize, usize)>,
    metadata: Option<(usize, usize)>,
    bind_metadata: Option<(usize, usize)>,
    sql_info: Option<(usize, usize)>,
    bind_count: Option<u64>,
    stmt_id: Option<u64>,
}

impl Response {
    fn parse(packet: &Bytes) -> Result<Self> {
        let (mut header, header_len) = MapCursor::new(packet)?;
        let mut request_type = None;
        let mut sync = 0;
        let mut schema_version = None;
        while let Some((k, _, v)) = header.next()? {
            match u8::try_from(k).unwrap_or(u8::MAX) {
                key::REQUEST_TYPE => request_type = Some(read_uint(v)?),
                key::SYNC => sync = read_uint(v)?,
                key::SCHEMA_VERSION => schema_version = Some(read_uint(v)?),
                _ => {}
            }
        }
        let request_type =
            request_type.ok_or_else(|| Error::protocol("response header has no request type"))?;

        let kind = match request_type {
            response::OK => Kind::Ok,
            response::CHUNK => Kind::Chunk,
            response::EVENT => Kind::Event,
            t if t & response::ERROR_FLAG != 0 => Kind::Error,
            other => return Err(Error::protocol(format!("unknown response type {other:#x}"))),
        };

        let mut this = Self {
            kind,
            sync,
            schema_version,
            body: packet.slice(header_len..),
            data: None,
            position: None,
            error: None,
            event_key: None,
            event_data: None,
            metadata: None,
            bind_metadata: None,
            sql_info: None,
            bind_count: None,
            stmt_id: None,
        };

        if this.body.is_empty() {
            return Ok(this);
        }
        let (mut body, _) = MapCursor::new(&this.body)?;
        let mut error_24 = None;
        let mut error_ext = None;
        while let Some((k, start, v)) = body.next()? {
            let range = (start, start + v.len());
            match u8::try_from(k).unwrap_or(u8::MAX) {
                key::DATA => this.data = Some(range),
                key::POSITION => this.position = Some(read_str(v)?.to_owned()),
                key::ERROR_24 => error_24 = Some(read_str(v)?.to_owned()),
                key::ERROR => error_ext = Some(v.to_vec()),
                key::EVENT_KEY => this.event_key = Some(read_str(v)?.to_owned()),
                key::EVENT_DATA => this.event_data = Some(range),
                key::METADATA => this.metadata = Some(range),
                key::BIND_METADATA => this.bind_metadata = Some(range),
                key::SQL_INFO => this.sql_info = Some(range),
                key::BIND_COUNT => this.bind_count = Some(read_uint(v)?),
                key::STMT_ID => this.stmt_id = Some(read_uint(v)?),
                _ => {}
            }
        }

        if kind == Kind::Error {
            let code = u32::try_from(request_type & !response::ERROR_FLAG).unwrap_or(0);
            this.error = Some(Box::new(match error_ext {
                Some(bytes) => decode_error_map(&bytes, code)?,
                None => ServerError::from_message(code, error_24.unwrap_or_default()),
            }));
        }
        Ok(this)
    }

    /// The `IPROTO_DATA` payload, decoded as `T`.
    pub(crate) fn data<T: DeserializeOwned>(&self) -> Result<T> {
        let bytes = self.data_bytes();
        rmp_serde::from_slice(bytes).map_err(Error::decode)
    }

    /// The whole body as a map of `(key, value)`, materialised. For the rare
    /// packets whose body is small and irregular (`IPROTO_ID`), not for data.
    pub(crate) fn body_map(&self) -> Result<HashMap<u64, Value>> {
        let mut map = HashMap::new();
        if self.body.is_empty() {
            return Ok(map);
        }
        let (mut cursor, _) = MapCursor::new(&self.body)?;
        while let Some((k, _, v)) = cursor.next()? {
            let value = rmpv::decode::read_value(&mut &v[..])
                .map_err(|e| Error::protocol(format!("malformed body value: {e}")))?;
            map.insert(k, value);
        }
        Ok(map)
    }

    /// The `IPROTO_DATA` payload as raw `MessagePack` (`nil` if absent).
    pub(crate) fn data_bytes(&self) -> &[u8] {
        self.slice(self.data).unwrap_or(&[0xc0])
    }

    /// Whether the body carries `IPROTO_DATA` at all.
    pub(crate) const fn has_data(&self) -> bool {
        self.data.is_some()
    }

    /// SQL: the `IPROTO_METADATA` array describing result columns.
    pub(crate) fn metadata_bytes(&self) -> Option<&[u8]> {
        self.slice(self.metadata)
    }

    /// SQL: the `IPROTO_BIND_METADATA` array describing parameters.
    pub(crate) fn bind_metadata_bytes(&self) -> Option<&[u8]> {
        self.slice(self.bind_metadata)
    }

    /// SQL: the `IPROTO_SQL_INFO` map of a data-changing statement.
    pub(crate) fn sql_info_bytes(&self) -> Option<&[u8]> {
        self.slice(self.sql_info)
    }

    /// SQL: how many parameters a prepared statement takes.
    pub(crate) const fn bind_count(&self) -> Option<u64> {
        self.bind_count
    }

    /// SQL: the id of a freshly prepared statement.
    pub(crate) const fn stmt_id(&self) -> Option<u64> {
        self.stmt_id
    }

    fn slice(&self, range: Option<(usize, usize)>) -> Option<&[u8]> {
        let (start, end) = range?;
        Some(&self.body[start..end])
    }

    /// The pagination position returned when `fetch_position` was set.
    pub(crate) fn position(&self) -> Option<&str> {
        self.position.as_deref()
    }

    /// The watched key and its data for an event packet.
    pub(crate) fn event(&self) -> Option<(&str, &[u8])> {
        let key = self.event_key.as_deref()?;
        Some((key, self.slice(self.event_data).unwrap_or(&[0xc0])))
    }

    /// Turn an error packet into its [`ServerError`].
    pub(crate) fn into_error(self) -> ServerError {
        self.error.map_or_else(|| ServerError::from_message(0, "unknown server error"), |err| *err)
    }
}

/// Decode the map under `IPROTO_ERROR` (or the payload of an `MP_ERROR` ext).
///
/// Layout: `{ MP_ERROR_STACK: [ frame, frame, ... ] }`, first frame outermost.
fn decode_error_map(bytes: &[u8], code: u32) -> Result<ServerError> {
    let mut cursor = bytes;
    let value = rmpv::decode::read_value(&mut cursor)
        .map_err(|e| Error::protocol(format!("malformed MP_ERROR: {e}")))?;
    let stack = value
        .as_map()
        .and_then(|entries| {
            entries
                .iter()
                .find(|(k, _)| k.as_u64() == Some(u64::from(error_key::STACK)))
                .map(|(_, v)| v)
        })
        .and_then(Value::as_array)
        .ok_or_else(|| Error::protocol("MP_ERROR without a stack"))?;

    let mut frames = stack.iter().map(decode_error_frame).collect::<Result<Vec<_>>>()?;
    if frames.is_empty() {
        return Ok(ServerError::from_message(code, "empty error stack"));
    }
    let mut outer = frames.remove(0);
    outer.cause = frames;
    Ok(outer)
}

fn decode_error_frame(frame: &Value) -> Result<ServerError> {
    let entries = frame.as_map().ok_or_else(|| Error::protocol("MP_ERROR frame is not a map"))?;
    let mut err = ServerError::from_message(0, "");
    for (k, v) in entries {
        match k.as_u64().and_then(|k| u8::try_from(k).ok()) {
            Some(error_key::TYPE) => v.as_str().unwrap_or_default().clone_into(&mut err.kind),
            Some(error_key::FILE) => err.file = v.as_str().map(str::to_owned),
            Some(error_key::LINE) => err.line = v.as_u64(),
            Some(error_key::MESSAGE) => v.as_str().unwrap_or_default().clone_into(&mut err.message),
            Some(error_key::ERRNO) => err.errno = v.as_u64().unwrap_or(0),
            Some(error_key::ERRCODE) => {
                err.code = ErrorCode(v.as_u64().and_then(|c| u32::try_from(c).ok()).unwrap_or(0));
            }
            Some(error_key::FIELDS) => {
                err.fields = v
                    .as_map()
                    .map(|fields| {
                        fields
                            .iter()
                            .filter_map(|(k, v)| Some((k.as_str()?.to_owned(), v.clone())))
                            .collect::<BTreeMap<_, _>>()
                    })
                    .unwrap_or_default();
            }
            _ => {}
        }
    }
    Ok(err)
}

/// Serialise `value` to a standalone `MessagePack` buffer.
pub(crate) fn to_msgpack<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    rmp_serde::encode::write(&mut buf, value).map_err(Error::encode)?;
    Ok(buf)
}

/// Like [`to_msgpack`], but the value must be tuple-shaped (an `MP_ARRAY`).
pub(crate) fn to_msgpack_array<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let buf = to_msgpack(value)?;
    if is_array_marker(buf.first().copied()) { Ok(buf) } else { Err(Error::encode(NotAnArray)) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iproto::request;

    fn packet(header: &Value, body: &Value) -> Bytes {
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, header).unwrap();
        rmpv::encode::write_value(&mut buf, body).unwrap();
        Bytes::from(buf)
    }

    fn map(entries: Vec<(u8, Value)>) -> Value {
        Value::Map(entries.into_iter().map(|(k, v)| (Value::from(k), v)).collect())
    }

    #[test]
    fn request_layout_matches_the_reference_select() {
        // The documented `select` packet: header {0: 1, 1: 4}, body of six keys.
        let bytes = Request::new(request::SELECT, 4, None)
            .uint(key::SPACE_ID, 512)
            .uint(key::INDEX_ID, 0)
            .uint(key::ITERATOR, 0)
            .uint(key::OFFSET, 0)
            .uint(key::LIMIT, u64::from(u32::MAX))
            .serialized(key::KEY, &(280u32,), true)
            .unwrap()
            .finish();
        let mut cursor = &bytes[..];
        let header = rmpv::decode::read_value(&mut cursor).unwrap();
        let body = rmpv::decode::read_value(&mut cursor).unwrap();
        assert_eq!(header, map(vec![(0, Value::from(1)), (1, Value::from(4))]));
        assert_eq!(
            body,
            map(vec![
                (0x10, Value::from(512)),
                (0x11, Value::from(0)),
                (0x14, Value::from(0)),
                (0x13, Value::from(0)),
                (0x12, Value::from(u32::MAX)),
                (0x20, Value::Array(vec![Value::from(280)])),
            ])
        );
    }

    #[test]
    fn scalar_where_a_tuple_is_expected_is_rejected_client_side() {
        let err = Request::new(request::INSERT, 1, None)
            .uint(key::SPACE_ID, 512)
            .serialized(key::TUPLE, &5u32, true)
            .unwrap_err();
        assert!(matches!(err, Error::Encode(_)));
        assert!(err.to_string().contains("wrap it"));
    }

    #[test]
    fn stream_id_lands_in_the_header() {
        let bytes = Request::new(request::PING, 9, Some(3)).finish();
        let mut cursor = &bytes[..];
        let header = rmpv::decode::read_value(&mut cursor).unwrap();
        assert_eq!(
            header,
            map(vec![(0, Value::from(0x40)), (1, Value::from(9)), (0x0a, Value::from(3))])
        );
    }

    #[test]
    fn decodes_ok_response_and_data_lazily() {
        let packet = packet(
            &map(vec![(0x00, Value::from(0)), (0x01, Value::from(7)), (0x05, Value::from(100))]),
            &map(vec![(
                0x30,
                Value::Array(vec![Value::Array(vec![Value::from(1), Value::from("AAA")])]),
            )]),
        );
        let mut framed = BytesMut::new();
        Codec.encode(packet, &mut framed).unwrap();
        let response = Codec.decode(&mut framed).unwrap().expect("a whole packet");
        assert_eq!(response.kind, Kind::Ok);
        assert_eq!(response.sync, 7);
        assert_eq!(response.schema_version, Some(100));
        let rows: Vec<(u64, String)> = response.data().unwrap();
        assert_eq!(rows, vec![(1, "AAA".to_owned())]);
        assert!(framed.is_empty());
    }

    #[test]
    fn partial_frames_wait_for_more_bytes() {
        let packet =
            packet(&map(vec![(0x00, Value::from(0)), (0x01, Value::from(1))]), &map(vec![]));
        let mut framed = BytesMut::new();
        Codec.encode(packet, &mut framed).unwrap();
        let cut = framed.split_off(4);
        assert!(Codec.decode(&mut framed).unwrap().is_none());
        framed.unsplit(cut);
        assert!(Codec.decode(&mut framed).unwrap().is_some());
    }

    #[test]
    fn decodes_the_documented_error_stack() {
        let frame = map(vec![
            (0x00, Value::from("ClientError")),
            (0x01, Value::from("builtin/box/schema.lua")),
            (0x02, Value::from(1234)),
            (0x03, Value::from("Space '_space' already exists")),
            (0x04, Value::from(0)),
            (0x05, Value::from(10)),
        ]);
        let packet = packet(
            &map(vec![(0x00, Value::from(0x800a)), (0x01, Value::from(5))]),
            &map(vec![
                (0x31, Value::from("Space '_space' already exists")),
                (0x52, map(vec![(0x00, Value::Array(vec![frame]))])),
            ]),
        );
        let mut framed = BytesMut::new();
        Codec.encode(packet, &mut framed).unwrap();
        let response = Codec.decode(&mut framed).unwrap().unwrap();
        assert_eq!(response.kind, Kind::Error);
        let err = response.into_error();
        assert_eq!(err.code, ErrorCode::SPACE_EXISTS);
        assert_eq!(err.kind, "ClientError");
        assert_eq!(err.line, Some(1234));
        assert_eq!(err.to_string(), "Space '_space' already exists");
    }

    #[test]
    fn events_carry_key_and_data() {
        let packet = packet(
            &map(vec![(0x00, Value::from(0x4c))]),
            &map(vec![(0x57, Value::from("box.shutdown")), (0x58, Value::from(true))]),
        );
        let mut framed = BytesMut::new();
        Codec.encode(packet, &mut framed).unwrap();
        let response = Codec.decode(&mut framed).unwrap().unwrap();
        assert_eq!(response.kind, Kind::Event);
        let (k, data) = response.event().unwrap();
        assert_eq!(k, "box.shutdown");
        assert_eq!(data, &[0xc3]);
    }
}
