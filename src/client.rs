use crate::backend::{BoxStream, Datagram};
use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::frame::{
    Capabilities, Frame, ServiceStatus, MAX_DATAGRAM_SIZE, MAX_FRAME_SIZE, PROTOCOL_VERSION,
};
use async_trait::async_trait;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

#[cfg(unix)]
use tokio::net::UnixStream;

#[async_trait]
pub trait Connector: Send + Sync + 'static {
    async fn open(&self) -> io::Result<BoxStream>;
}

#[derive(Clone)]
pub struct TcpConnector {
    address: SocketAddr,
}

impl TcpConnector {
    pub fn new(address: SocketAddr) -> Self {
        Self { address }
    }
}

#[async_trait]
impl Connector for TcpConnector {
    async fn open(&self) -> io::Result<BoxStream> {
        Ok(Box::new(TcpStream::connect(self.address).await?))
    }
}

#[cfg(unix)]
#[derive(Clone)]
pub struct UnixConnector {
    path: PathBuf,
}

#[cfg(unix)]
impl UnixConnector {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

#[cfg(unix)]
#[async_trait]
impl Connector for UnixConnector {
    async fn open(&self) -> io::Result<BoxStream> {
        Ok(Box::new(UnixStream::connect(&self.path).await?))
    }
}

#[derive(Clone)]
pub struct Client {
    connector: Arc<dyn Connector>,
}

impl Client {
    pub fn from_connector<C>(connector: C) -> Self
    where
        C: Connector,
    {
        Self {
            connector: Arc::new(connector),
        }
    }

    pub async fn connect(address: SocketAddr) -> Result<Self> {
        let client = Self::from_connector(TcpConnector::new(address));
        client.query_capabilities().await?;
        Ok(client)
    }

    #[cfg(unix)]
    pub async fn connect_unix(path: impl Into<PathBuf>) -> Result<Self> {
        let client = Self::from_connector(UnixConnector::new(path));
        client.query_capabilities().await?;
        Ok(client)
    }

    pub async fn query_capabilities(&self) -> Result<Capabilities> {
        let (_, capabilities, _) = self.open_session().await?;
        Ok(capabilities)
    }

    pub async fn query_status(&self) -> Result<ServiceStatus> {
        let (mut stream, _, max_frame_size) = self.open_session().await?;
        Frame::StatusRequest.write_to(&mut stream).await?;
        match Frame::read_from_with_limit(&mut stream, max_frame_size).await? {
            Frame::Status(status) => Ok(status),
            Frame::Error { code, message } => Err(Error::Remote { code, message }),
            _ => Err(Error::Protocol(
                "expected a Status response from the server".to_owned(),
            )),
        }
    }

    pub async fn ping(&self) -> Result<()> {
        let (mut stream, _, max_frame_size) = self.open_session().await?;
        Frame::Ping.write_to(&mut stream).await?;
        match Frame::read_from_with_limit(&mut stream, max_frame_size).await? {
            Frame::Pong => Ok(()),
            Frame::Error { code, message } => Err(Error::Remote { code, message }),
            _ => Err(Error::Protocol("expected Pong from the server".to_owned())),
        }
    }

    pub async fn connect_tcp(&self, target: Endpoint) -> Result<TcpConnection> {
        let (mut stream, _, max_frame_size) = self.open_session().await?;
        Frame::OpenTcp {
            target: target.clone(),
        }
        .write_to(&mut stream)
        .await?;
        match Frame::read_from_with_limit(&mut stream, max_frame_size).await? {
            Frame::TcpReady { source } => Ok(TcpConnection {
                stream,
                target,
                source,
            }),
            Frame::Error { code, message } => Err(Error::Remote { code, message }),
            _ => Err(Error::Protocol(
                "expected TcpReady from the server".to_owned(),
            )),
        }
    }

    pub async fn open_udp(&self, bind: Option<Endpoint>) -> Result<UdpAssociation> {
        let (mut stream, capabilities, max_frame_size) = self.open_session().await?;
        if !capabilities.udp_associate {
            return Err(Error::Remote {
                code: crate::frame::ErrorCode::Unsupported,
                message: "server does not support UDP associations".to_owned(),
            });
        }
        Frame::OpenUdp { bind }.write_to(&mut stream).await?;
        let bound_endpoint = match Frame::read_from_with_limit(&mut stream, max_frame_size).await? {
            Frame::UdpReady { bound } => bound,
            Frame::Error { code, message } => return Err(Error::Remote { code, message }),
            _ => {
                return Err(Error::Protocol(
                    "expected UdpReady from the server".to_owned(),
                ));
            }
        };
        let datagram_limit = (capabilities.max_datagram_size as usize).min(MAX_DATAGRAM_SIZE);
        let (reader, writer) = tokio::io::split(stream);
        Ok(UdpAssociation {
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
            bound_endpoint,
            max_frame_size,
            datagram_limit,
        })
    }

    async fn open_session(&self) -> Result<(BoxStream, Capabilities, usize)> {
        let mut stream = self.connector.open().await?;
        Frame::Hello {
            version: PROTOCOL_VERSION,
            max_frame_size: MAX_FRAME_SIZE as u32,
        }
        .write_to(&mut stream)
        .await?;
        match Frame::read_from(&mut stream).await? {
            Frame::Welcome {
                version,
                max_frame_size,
                capabilities,
            } if version == PROTOCOL_VERSION => {
                let negotiated = (max_frame_size as usize).min(MAX_FRAME_SIZE);
                if negotiated < 128 {
                    return Err(Error::Protocol(
                        "server negotiated an invalid frame limit".to_owned(),
                    ));
                }
                Ok((stream, capabilities, negotiated))
            }
            Frame::Welcome { version, .. } => Err(Error::Protocol(format!(
                "server selected unsupported protocol version {version}"
            ))),
            Frame::Error { code, message } => Err(Error::Remote { code, message }),
            _ => Err(Error::Protocol(
                "expected Welcome from the server".to_owned(),
            )),
        }
    }
}

pub struct TcpConnection {
    stream: BoxStream,
    target: Endpoint,
    source: Option<Endpoint>,
}

impl TcpConnection {
    pub fn target(&self) -> &Endpoint {
        &self.target
    }

    pub fn source(&self) -> Option<&Endpoint> {
        self.source.as_ref()
    }

    pub fn info(&self) -> String {
        format!("SOCKS6 TCP {}", self.target)
    }
}

impl AsyncRead for TcpConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.stream).poll_shutdown(cx)
    }
}

pub struct UdpAssociation {
    reader: Mutex<ReadHalf<BoxStream>>,
    writer: Mutex<WriteHalf<BoxStream>>,
    bound_endpoint: Option<Endpoint>,
    max_frame_size: usize,
    datagram_limit: usize,
}

impl UdpAssociation {
    pub fn bound_endpoint(&self) -> Option<&Endpoint> {
        self.bound_endpoint.as_ref()
    }

    pub async fn send_to(&self, endpoint: Endpoint, payload: Vec<u8>) -> Result<()> {
        if payload.len() > self.datagram_limit {
            return Err(Error::FrameTooLarge {
                size: payload.len(),
                limit: self.datagram_limit,
            });
        }
        Frame::Datagram { endpoint, payload }
            .write_to(&mut *self.writer.lock().await)
            .await
    }

    pub async fn recv_from(&self) -> Result<Datagram> {
        match Frame::read_from_with_limit(&mut *self.reader.lock().await, self.max_frame_size)
            .await?
        {
            Frame::Datagram { endpoint, payload } => Ok(Datagram { endpoint, payload }),
            Frame::Error { code, message } => Err(Error::Remote { code, message }),
            _ => Err(Error::Protocol(
                "expected a UDP datagram from the server".to_owned(),
            )),
        }
    }

    pub async fn close(&self) -> Result<()> {
        Frame::Close
            .write_to(&mut *self.writer.lock().await)
            .await?;
        self.writer.lock().await.shutdown().await?;
        Ok(())
    }
}
