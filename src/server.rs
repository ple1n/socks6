use crate::backend::{AsyncStream, Backend, BackendError, DatagramAssociation};
use crate::error::{Error, Result};
use crate::frame::{ErrorCode, Frame, MAX_FRAME_SIZE, PROTOCOL_VERSION};
use async_trait::async_trait;
use std::io;
use std::sync::Arc;
use tokio::io::{copy_bidirectional, split};
use tokio::net::{TcpListener, TcpStream};
use tracing::warn;

#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

#[async_trait]
pub trait Listener: Send + 'static {
    type Stream: AsyncStream + 'static;

    async fn accept(&mut self) -> io::Result<Self::Stream>;
}

#[async_trait]
impl Listener for TcpListener {
    type Stream = TcpStream;

    async fn accept(&mut self) -> io::Result<Self::Stream> {
        Ok(TcpListener::accept(self).await?.0)
    }
}

#[cfg(unix)]
#[async_trait]
impl Listener for UnixListener {
    type Stream = UnixStream;

    async fn accept(&mut self) -> io::Result<Self::Stream> {
        Ok(UnixListener::accept(self).await?.0)
    }
}

pub struct Server {
    backend: Arc<dyn Backend>,
    max_frame_size: usize,
}

impl Server {
    pub fn new<B>(backend: B) -> Self
    where
        B: Backend,
    {
        Self {
            backend: Arc::new(backend),
            max_frame_size: MAX_FRAME_SIZE,
        }
    }

    pub fn from_shared_backend(backend: Arc<dyn Backend>) -> Self {
        Self {
            backend,
            max_frame_size: MAX_FRAME_SIZE,
        }
    }

    pub fn with_max_frame_size(mut self, max_frame_size: usize) -> Result<Self> {
        if !(128..=MAX_FRAME_SIZE).contains(&max_frame_size) {
            return Err(Error::Protocol(format!(
                "frame limit must be between 128 and {MAX_FRAME_SIZE}"
            )));
        }
        self.max_frame_size = max_frame_size;
        Ok(self)
    }

    pub async fn run<L>(self: Arc<Self>, mut listener: L) -> Result<()>
    where
        L: Listener,
    {
        loop {
            let stream = listener.accept().await?;
            let server = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(error) = server.serve_connection(stream).await {
                    warn!(%error, "SOCKS6 client connection ended with an error");
                }
            });
        }
    }

    pub async fn serve_connection<S>(&self, mut stream: S) -> Result<()>
    where
        S: AsyncStream + 'static,
    {
        let hello = Frame::read_from_with_limit(&mut stream, self.max_frame_size).await?;
        let (version, requested_limit) = match hello {
            Frame::Hello {
                version,
                max_frame_size,
            } => (version, max_frame_size as usize),
            _ => {
                Frame::Error {
                    code: ErrorCode::InvalidRequest,
                    message: "first frame must be Hello".to_owned(),
                }
                .write_to(&mut stream)
                .await?;
                return Err(Error::Protocol("first frame must be Hello".to_owned()));
            }
        };
        if version != PROTOCOL_VERSION {
            Frame::Error {
                code: ErrorCode::VersionMismatch,
                message: format!(
                    "unsupported version {version}; this server supports {PROTOCOL_VERSION}"
                ),
            }
            .write_to(&mut stream)
            .await?;
            return Ok(());
        }
        if requested_limit < 128 {
            Frame::Error {
                code: ErrorCode::InvalidRequest,
                message: "maximum frame size must be at least 128 bytes".to_owned(),
            }
            .write_to(&mut stream)
            .await?;
            return Ok(());
        }
        let max_frame_size = self.max_frame_size.min(requested_limit);
        Frame::Welcome {
            version: PROTOCOL_VERSION,
            max_frame_size: max_frame_size as u32,
            capabilities: self.backend.capabilities(),
        }
        .write_to(&mut stream)
        .await?;

        match Frame::read_from_with_limit(&mut stream, max_frame_size).await? {
            Frame::OpenTcp { target } => {
                let mut upstream = match self.backend.connect_tcp(target).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        write_backend_error(&mut stream, error).await?;
                        return Ok(());
                    }
                };
                Frame::TcpReady { source: None }
                    .write_to(&mut stream)
                    .await?;
                copy_bidirectional(&mut stream, &mut upstream).await?;
            }
            Frame::OpenUdp { bind } => {
                let association = match self.backend.open_udp(bind).await {
                    Ok(association) => association,
                    Err(error) => {
                        write_backend_error(&mut stream, error).await?;
                        return Ok(());
                    }
                };
                let bound = association.bound_endpoint();
                Frame::UdpReady { bound }.write_to(&mut stream).await?;
                self.serve_udp(stream, association, max_frame_size).await?;
            }
            Frame::StatusRequest => {
                Frame::Status(self.backend.status())
                    .write_to(&mut stream)
                    .await?;
            }
            Frame::Ping => Frame::Pong.write_to(&mut stream).await?,
            Frame::Close => {}
            _ => {
                Frame::Error {
                    code: ErrorCode::InvalidRequest,
                    message: "expected OpenTcp, OpenUdp, StatusRequest, Ping, or Close".to_owned(),
                }
                .write_to(&mut stream)
                .await?;
            }
        }
        Ok(())
    }

    async fn serve_udp<S>(
        &self,
        stream: S,
        association: Box<dyn DatagramAssociation>,
        max_frame_size: usize,
    ) -> Result<()>
    where
        S: AsyncStream + 'static,
    {
        let (mut reader, mut writer) = split(stream);
        loop {
            tokio::select! {
                incoming = Frame::read_from_with_limit(&mut reader, max_frame_size) => {
                    match incoming? {
                        Frame::Datagram { endpoint, payload } => {
                            if let Err(error) = association.send_to(endpoint, payload).await {
                                write_backend_error(&mut writer, error).await?;
                                return Ok(());
                            }
                        }
                        Frame::Close => return Ok(()),
                        Frame::Ping => Frame::Pong.write_to(&mut writer).await?,
                        _ => {
                            Frame::Error {
                                code: ErrorCode::InvalidRequest,
                                message: "expected Datagram, Ping, or Close".to_owned(),
                            }
                            .write_to(&mut writer)
                            .await?;
                            return Ok(());
                        }
                    }
                }
                outgoing = association.recv_from() => {
                    let datagram = match outgoing {
                        Ok(datagram) => datagram,
                        Err(error) => {
                            write_backend_error(&mut writer, error).await?;
                            return Ok(());
                        }
                    };
                    Frame::Datagram {
                        endpoint: datagram.endpoint,
                        payload: datagram.payload,
                    }
                    .write_to(&mut writer)
                    .await?;
                }
            }
        }
    }
}

async fn write_backend_error<W>(writer: &mut W, error: BackendError) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    Frame::Error {
        code: error.code(),
        message: error.to_string(),
    }
    .write_to(writer)
    .await
}
