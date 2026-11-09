use crate::backend::{BoxStream, Datagram};
use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::frame::{
    Capabilities, Frame, ServiceStatus, MAX_DATAGRAM_SIZE, MAX_FRAME_SIZE, PROTOCOL_VERSION,
};
use crate::identity::{IdentityLease, IdentityOffer, IdentitySelection, IdentityUse};
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
        let (mut stream, capabilities, _, max_frame_size) = self.open_session().await?;
        Frame::Close
            .write_to_with_limit(&mut stream, max_frame_size)
            .await?;
        stream.shutdown().await?;
        Ok(capabilities)
    }

    pub async fn identity_session(&self) -> Result<IdentitySession> {
        let (stream, capabilities, identities, max_frame_size) = self.open_session().await?;
        Ok(IdentitySession {
            client: self.clone(),
            stream,
            capabilities,
            identities,
            max_frame_size,
        })
    }

    pub async fn query_identities(&self) -> Result<Vec<IdentityOffer>> {
        let (mut stream, _, _, max_frame_size) = self.open_session().await?;
        Frame::IdentityOffersRequest
            .write_to_with_limit(&mut stream, max_frame_size)
            .await?;
        let response = Frame::read_from_with_limit(&mut stream, max_frame_size).await?;
        close_control_session(&mut stream, max_frame_size).await?;
        let identities = match response {
            Frame::IdentityOffers { identities } => identities,
            Frame::Error { code, message } => return Err(Error::Remote { code, message }),
            _ => {
                return Err(Error::Protocol(
                    "expected identity offers from the server".to_owned(),
                ));
            }
        };
        Ok(identities)
    }

    pub async fn lease_identity(&self, identity_id: Option<String>) -> Result<IdentityLease> {
        let (mut stream, _, _, max_frame_size) = self.open_session().await?;
        Frame::LeaseIdentity { identity_id }
            .write_to_with_limit(&mut stream, max_frame_size)
            .await?;
        let response = Frame::read_from_with_limit(&mut stream, max_frame_size).await?;
        close_control_session(&mut stream, max_frame_size).await?;
        let lease = match response {
            Frame::IdentityLeased { lease } => lease,
            Frame::Error { code, message } => return Err(Error::Remote { code, message }),
            _ => {
                return Err(Error::Protocol(
                    "expected an identity lease from the server".to_owned(),
                ));
            }
        };
        Ok(lease)
    }

    pub async fn release_identity(&self, lease: &IdentityLease) -> Result<()> {
        let (mut stream, _, _, max_frame_size) = self.open_session().await?;
        Frame::ReleaseIdentity {
            lease_id: lease.id.clone(),
        }
        .write_to_with_limit(&mut stream, max_frame_size)
        .await?;
        let response = Frame::read_from_with_limit(&mut stream, max_frame_size).await?;
        close_control_session(&mut stream, max_frame_size).await?;
        match response {
            Frame::IdentityReleased => {}
            Frame::Error { code, message } => return Err(Error::Remote { code, message }),
            _ => {
                return Err(Error::Protocol(
                    "expected identity release confirmation from the server".to_owned(),
                ));
            }
        }
        Ok(())
    }

    pub async fn query_status(&self) -> Result<ServiceStatus> {
        let (mut stream, _, _, max_frame_size) = self.open_session().await?;
        Frame::StatusRequest.write_to(&mut stream).await?;
        let response = Frame::read_from_with_limit(&mut stream, max_frame_size).await?;
        close_control_session(&mut stream, max_frame_size).await?;
        match response {
            Frame::Status(status) => Ok(status),
            Frame::Error { code, message } => Err(Error::Remote { code, message }),
            _ => Err(Error::Protocol(
                "expected a Status response from the server".to_owned(),
            )),
        }
    }

    pub async fn ping(&self) -> Result<()> {
        let (mut stream, _, _, max_frame_size) = self.open_session().await?;
        Frame::Ping.write_to(&mut stream).await?;
        let response = Frame::read_from_with_limit(&mut stream, max_frame_size).await?;
        close_control_session(&mut stream, max_frame_size).await?;
        match response {
            Frame::Pong => Ok(()),
            Frame::Error { code, message } => Err(Error::Remote { code, message }),
            _ => Err(Error::Protocol("expected Pong from the server".to_owned())),
        }
    }

    pub async fn connect_stream(
        &self,
        target: Endpoint,
        identity: IdentitySelection,
    ) -> Result<StreamConnection> {
        let (mut stream, _, _, max_frame_size) = self.open_session().await?;
        Frame::OpenStream {
            identity,
            target: target.clone(),
        }
        .write_to(&mut stream)
        .await?;
        match Frame::read_from_with_limit(&mut stream, max_frame_size).await? {
            Frame::StreamReady { identity, source } => Ok(StreamConnection {
                stream,
                target,
                source,
                identity,
            }),
            Frame::Error { code, message } => Err(Error::Remote { code, message }),
            _ => Err(Error::Protocol(
                "expected StreamReady from the server".to_owned(),
            )),
        }
    }

    pub async fn accept_stream(
        &self,
        identity: IdentitySelection,
        port: u16,
    ) -> Result<StreamConnection> {
        let (mut stream, _, _, max_frame_size) = self.open_session().await?;
        Frame::AcceptStream { identity, port }
            .write_to_with_limit(&mut stream, max_frame_size)
            .await?;
        match Frame::read_from_with_limit(&mut stream, max_frame_size).await? {
            Frame::StreamAccepted { identity, peer } => Ok(StreamConnection {
                stream,
                target: peer,
                source: None,
                identity,
            }),
            Frame::Error { code, message } => Err(Error::Remote { code, message }),
            _ => Err(Error::Protocol(
                "expected an incoming stream from the server".to_owned(),
            )),
        }
    }

    pub async fn open_datagrams(
        &self,
        identity: IdentitySelection,
    ) -> Result<DatagramChannelConnection> {
        let (mut stream, capabilities, _, max_frame_size) = self.open_session().await?;
        Frame::OpenDatagrams { identity }
            .write_to(&mut stream)
            .await?;
        let (identity, bound_endpoint) =
            match Frame::read_from_with_limit(&mut stream, max_frame_size).await? {
                Frame::DatagramsReady { identity, bound } => (identity, bound),
                Frame::Error { code, message } => return Err(Error::Remote { code, message }),
                _ => {
                    return Err(Error::Protocol(
                        "expected DatagramsReady from the server".to_owned(),
                    ));
                }
            };
        let datagram_limit = (capabilities.max_datagram_size as usize).min(MAX_DATAGRAM_SIZE);
        let (reader, writer) = tokio::io::split(stream);
        Ok(DatagramChannelConnection {
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
            bound_endpoint,
            identity,
            max_frame_size,
            datagram_limit,
        })
    }

    async fn open_session(&self) -> Result<(BoxStream, Capabilities, Vec<IdentityOffer>, usize)> {
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
                identities,
            } if version == PROTOCOL_VERSION => {
                let negotiated = (max_frame_size as usize).min(MAX_FRAME_SIZE);
                if negotiated < 128 {
                    return Err(Error::Protocol(
                        "server negotiated an invalid frame limit".to_owned(),
                    ));
                }
                Ok((stream, capabilities, identities, negotiated))
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

async fn close_control_session(stream: &mut BoxStream, max_frame_size: usize) -> Result<()> {
    Frame::Close
        .write_to_with_limit(stream, max_frame_size)
        .await?;
    stream.shutdown().await?;
    Ok(())
}

pub struct IdentitySession {
    client: Client,
    stream: BoxStream,
    capabilities: Capabilities,
    identities: Vec<IdentityOffer>,
    max_frame_size: usize,
}

impl IdentitySession {
    pub fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    pub fn identities(&self) -> &[IdentityOffer] {
        &self.identities
    }

    pub async fn refresh_identities(&mut self) -> Result<&[IdentityOffer]> {
        Frame::IdentityOffersRequest
            .write_to_with_limit(&mut self.stream, self.max_frame_size)
            .await?;
        self.identities =
            match Frame::read_from_with_limit(&mut self.stream, self.max_frame_size).await? {
                Frame::IdentityOffers { identities } => identities,
                Frame::Error { code, message } => return Err(Error::Remote { code, message }),
                _ => {
                    return Err(Error::Protocol(
                        "expected identity offers from the server".to_owned(),
                    ));
                }
            };
        Ok(&self.identities)
    }

    pub async fn lease_identity(&mut self, identity_id: Option<String>) -> Result<IdentityLease> {
        Frame::LeaseIdentity { identity_id }
            .write_to_with_limit(&mut self.stream, self.max_frame_size)
            .await?;
        match Frame::read_from_with_limit(&mut self.stream, self.max_frame_size).await? {
            Frame::IdentityLeased { lease } => Ok(lease),
            Frame::Error { code, message } => Err(Error::Remote { code, message }),
            _ => Err(Error::Protocol(
                "expected an identity lease from the server".to_owned(),
            )),
        }
    }

    pub async fn release_identity(&mut self, lease: &IdentityLease) -> Result<()> {
        Frame::ReleaseIdentity {
            lease_id: lease.id.clone(),
        }
        .write_to_with_limit(&mut self.stream, self.max_frame_size)
        .await?;
        match Frame::read_from_with_limit(&mut self.stream, self.max_frame_size).await? {
            Frame::IdentityReleased => Ok(()),
            Frame::Error { code, message } => Err(Error::Remote { code, message }),
            _ => Err(Error::Protocol(
                "expected identity release confirmation from the server".to_owned(),
            )),
        }
    }

    pub async fn connect_stream(
        &self,
        target: Endpoint,
        identity: IdentitySelection,
    ) -> Result<StreamConnection> {
        self.client.connect_stream(target, identity).await
    }

    pub async fn accept_stream(
        &self,
        identity: IdentitySelection,
        port: u16,
    ) -> Result<StreamConnection> {
        self.client.accept_stream(identity, port).await
    }

    pub async fn open_datagrams(
        &self,
        identity: IdentitySelection,
    ) -> Result<DatagramChannelConnection> {
        self.client.open_datagrams(identity).await
    }

    pub async fn close(mut self) -> Result<()> {
        Frame::Close
            .write_to_with_limit(&mut self.stream, self.max_frame_size)
            .await?;
        self.stream.shutdown().await?;
        Ok(())
    }
}

pub struct StreamConnection {
    stream: BoxStream,
    target: Endpoint,
    source: Option<Endpoint>,
    identity: IdentityUse,
}

impl StreamConnection {
    pub fn target(&self) -> &Endpoint {
        &self.target
    }

    pub fn source(&self) -> Option<&Endpoint> {
        self.source.as_ref()
    }

    pub fn identity(&self) -> &IdentityUse {
        &self.identity
    }

    pub fn info(&self) -> String {
        format!("SOCKS6 stream {}", self.target)
    }
}

impl AsyncRead for StreamConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for StreamConnection {
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

pub struct DatagramChannelConnection {
    reader: Mutex<ReadHalf<BoxStream>>,
    writer: Mutex<WriteHalf<BoxStream>>,
    bound_endpoint: Option<Endpoint>,
    identity: IdentityUse,
    max_frame_size: usize,
    datagram_limit: usize,
}

impl DatagramChannelConnection {
    pub fn bound_endpoint(&self) -> Option<&Endpoint> {
        self.bound_endpoint.as_ref()
    }

    pub fn identity(&self) -> &IdentityUse {
        &self.identity
    }

    pub async fn send_to(&self, endpoint: Endpoint, payload: Vec<u8>) -> Result<()> {
        if payload.len() > self.datagram_limit {
            return Err(Error::FrameTooLarge {
                size: payload.len(),
                limit: self.datagram_limit,
            });
        }
        Frame::Datagram { endpoint, payload }
            .write_to_with_limit(&mut *self.writer.lock().await, self.max_frame_size)
            .await
    }

    pub async fn recv_from(&self) -> Result<Datagram> {
        match Frame::read_from_with_limit(&mut *self.reader.lock().await, self.max_frame_size)
            .await?
        {
            Frame::Datagram { endpoint, payload } => Ok(Datagram { endpoint, payload }),
            Frame::Error { code, message } => Err(Error::Remote { code, message }),
            _ => Err(Error::Protocol(
                "expected a datagram from the server".to_owned(),
            )),
        }
    }

    pub async fn close(&self) -> Result<()> {
        let mut writer = self.writer.lock().await;
        Frame::Close
            .write_to_with_limit(&mut *writer, self.max_frame_size)
            .await?;
        writer.shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::MAX_FRAME_SIZE;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn querying_capabilities_closes_the_protocol_session() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let hello = Frame::read_from(&mut stream).await.unwrap();
            let Frame::Hello { max_frame_size, .. } = hello else {
                panic!("expected Hello");
            };
            let capabilities = Capabilities {
                server_name: "test-server".to_owned(),
                ..Capabilities::default()
            };
            Frame::Welcome {
                version: PROTOCOL_VERSION,
                max_frame_size: max_frame_size.min(MAX_FRAME_SIZE as u32),
                capabilities,
                identities: vec![],
            }
            .write_to(&mut stream)
            .await
            .unwrap();
            assert_eq!(Frame::read_from(&mut stream).await.unwrap(), Frame::Close);
        });

        let client = Client::from_connector(TcpConnector::new(address));
        assert_eq!(
            client.query_capabilities().await.unwrap().server_name,
            "test-server"
        );
        server.await.unwrap();
    }
}
