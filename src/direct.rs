// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Display, io, str::FromStr, sync::Arc};

use ciborium::cbor;
use futures_util::{
  AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, FutureExt,
};
use libp2p::{PeerId, request_response::Codec};

#[derive(Debug)]
#[derive(Clone)]
#[derive(Default)]
pub struct QalamDirect;
impl Codec for QalamDirect {
  type Protocol = QalamDirectProtocol;
  type Request = QalamDirectRequest;
  type Response = QalamDirectResponse;

  fn read_request<'life0, 'life1, 'life2, 'async_trait, T>(
    &'life0 mut self,
    _: &'life1 Self::Protocol,
    io: &'life2 mut T,
  ) -> ::core::pin::Pin<
    Box<
      dyn ::core::future::Future<Output = io::Result<Self::Request>>
        + ::core::marker::Send
        + 'async_trait,
    >,
  >
  where
    T: AsyncRead + Unpin + Send,
    T: 'async_trait,
    'life0: 'async_trait,
    'life1: 'async_trait,
    'life2: 'async_trait,
    Self: 'async_trait,
  {
    tracing::debug!("read_request");
    async {
      let data = read_with_len(io).await?;
      let value = ciborium::from_reader::<ciborium::Value, _>(data.as_slice())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
      value.try_into()
    }
    .boxed()
  }
  fn read_response<'life0, 'life1, 'life2, 'async_trait, T>(
    &'life0 mut self,
    _: &'life1 Self::Protocol,
    io: &'life2 mut T,
  ) -> ::core::pin::Pin<
    Box<
      dyn ::core::future::Future<Output = io::Result<Self::Response>>
        + ::core::marker::Send
        + 'async_trait,
    >,
  >
  where
    T: AsyncRead + Unpin + Send,
    T: 'async_trait,
    'life0: 'async_trait,
    'life1: 'async_trait,
    'life2: 'async_trait,
    Self: 'async_trait,
  {
    tracing::debug!("read_response");
    async {
      let data = read_with_len(io).await?;
      let value = ciborium::from_reader::<ciborium::Value, _>(data.as_slice())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
      tracing::debug!("response data: {:?}", value);
      let a = value.try_into();
      tracing::debug!("try: {:?}", a);

      a
    }
    .boxed()
  }

  fn write_request<'life0, 'life1, 'life2, 'async_trait, T>(
    &'life0 mut self,
    _: &'life1 Self::Protocol,
    io: &'life2 mut T,
    req: Self::Request,
  ) -> ::core::pin::Pin<
    Box<
      dyn ::core::future::Future<Output = io::Result<()>>
        + ::core::marker::Send
        + 'async_trait,
    >,
  >
  where
    T: AsyncWrite + Unpin + Send,
    T: 'async_trait,
    'life0: 'async_trait,
    'life1: 'async_trait,
    'life2: 'async_trait,
    Self: 'async_trait,
  {
    tracing::debug!("write_request");
    async move {
      let data = to_cbor_bytes(&ciborium::Value::from(&req))?;
      write_with_len(io, data).await
    }
    .boxed()
  }
  fn write_response<'life0, 'life1, 'life2, 'async_trait, T>(
    &'life0 mut self,
    _: &'life1 Self::Protocol,
    io: &'life2 mut T,
    res: Self::Response,
  ) -> ::core::pin::Pin<
    Box<
      dyn ::core::future::Future<Output = io::Result<()>>
        + ::core::marker::Send
        + 'async_trait,
    >,
  >
  where
    T: AsyncWrite + Unpin + Send,
    T: 'async_trait,
    'life0: 'async_trait,
    'life1: 'async_trait,
    'life2: 'async_trait,
    Self: 'async_trait,
  {
    tracing::debug!("write_response");
    async move {
      let value = ciborium::Value::from(res);
      let data = to_cbor_bytes(&value)?;
      write_with_len(io, data).await?;
      Ok(())
    }
    .boxed()
  }
}

async fn read_with_len<R>(io: &mut R) -> io::Result<Vec<u8>>
where
  R: AsyncRead + Unpin,
{
  let mut len_buf = [0u8; 4];
  io.read_exact(&mut len_buf).await?;
  let len = u32::from_be_bytes(len_buf) as usize;

  let mut data_buf = vec![0; len];
  io.read_exact(&mut data_buf).await?;
  Ok(data_buf)
}
async fn write_with_len<W, T>(io: &mut W, data: T) -> io::Result<()>
where
  W: AsyncWrite + Unpin,
  T: AsRef<[u8]>,
{
  let data = data.as_ref();
  let len = u32::try_from(data.len()).map_err(|_| {
    io::Error::new(io::ErrorKind::InvalidInput, "too big payload")
  })?;
  io.write_all(&len.to_be_bytes()).await?;
  io.write_all(data).await?;
  io.close().await
}

#[derive(Clone)]
pub struct QalamDirectProtocol;
impl AsRef<str> for QalamDirectProtocol {
  fn as_ref(&self) -> &str {
    "/hat/direct/0.1.0"
  }
}

#[derive(Debug)]
#[derive(Clone)]
pub enum QalamDirectRequest {
  Persona,
  Ping,
}
impl TryFrom<ciborium::Value> for QalamDirectRequest {
  type Error = io::Error;

  fn try_from(value: ciborium::Value) -> Result<Self, Self::Error> {
    let tag = cbor_get_str(&value, &["type"])?;
    match tag {
      "ping" => Ok(Self::Ping),
      "persona" => Ok(Self::Persona),

      unknown => Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unknown type provided: {}", unknown),
      )),
    }
  }
}
impl From<&QalamDirectRequest> for ciborium::Value {
  fn from(req: &QalamDirectRequest) -> Self {
    match req {
      QalamDirectRequest::Ping => cbor!({ "type" => "ping" }).unwrap(),
      QalamDirectRequest::Persona => cbor!({ "type" => "persona" }).unwrap(),
    }
  }
}

#[derive(Debug)]
#[derive(Clone)]
pub enum QalamDirectResponse {
  Persona { peer: PeerId, ident: Arc<str> },
  Pong,
}
impl Display for QalamDirectResponse {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_fmt(format_args!("{:?}", self))
  }
}
impl std::error::Error for QalamDirectResponse {}
impl From<QalamDirectResponse> for ciborium::Value {
  fn from(value: QalamDirectResponse) -> Self {
    match value {
      QalamDirectResponse::Pong => cbor!({ "type" => "pong"}),
      QalamDirectResponse::Persona {
        peer,
        ident: persona,
      } => {
        cbor!({ "type" => "persona", "data" => { "peer" => peer.to_string(), "ident" => &*persona } })
      }
    }
    .unwrap()
  }
}
impl TryFrom<ciborium::Value> for QalamDirectResponse {
  type Error = io::Error;

  fn try_from(value: ciborium::Value) -> Result<Self, Self::Error> {
    let tag = cbor_get_str(&value, &["type"])?;
    match tag {
      "pong" => Ok(Self::Pong),
      "persona" => {
        let peer = cbor_get_str(&value, &["data", "peer"])?;
        let ident = cbor_get_str(&value, &["data", "ident"])?;
        Ok(Self::Persona {
          peer: PeerId::from_str(peer)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?,
          ident: Arc::from(ident),
        })
      }

      unknown => Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unknown response type: {}", unknown),
      )),
    }
  }
}

#[inline]
fn cbor_get_str<'a>(
  value: &'a ciborium::Value,
  path: &[&str],
) -> io::Result<&'a str> {
  let (last, parents) = path
    .split_last()
    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "empty path"))?;

  let mut current = value;
  for key in parents {
    current = cbor_get_value(current, key)?;
  }

  cbor_get_value(current, last)?.as_text().ok_or_else(|| {
    io::Error::new(
      io::ErrorKind::InvalidData,
      format!("field '{}' is not a string", last),
    )
  })
}

#[inline]
fn cbor_get_value<'a>(
  value: &'a ciborium::Value,
  key: &str,
) -> io::Result<&'a ciborium::Value> {
  match value {
    ciborium::Value::Map(m) => m
      .iter()
      .find(|(k, _)| *k == ciborium::Value::Text(key.into()))
      .map(|(_, v)| v)
      .ok_or_else(|| {
        io::Error::new(
          io::ErrorKind::InvalidData,
          format!("missing field: '{}'", key),
        )
      }),
    _ => Err(io::Error::new(
      io::ErrorKind::InvalidData,
      format!("expected map at '{}'", key),
    )),
  }
}
#[inline]
fn to_cbor_bytes(value: &ciborium::Value) -> io::Result<Vec<u8>> {
  let mut buf = Vec::new();
  ciborium::into_writer(value, &mut buf)
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
  Ok(buf)
}
