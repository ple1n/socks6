use crate::endpoint::Endpoint;
use crate::frame::{Capabilities, ErrorCode, MAX_DATAGRAM_SIZE};
use crate::identity::{IdentityCapabilities, IdentityOffer};
use async_trait::async_trait;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, UdpSocket};

pub trait AsyncStream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

pub type BoxStream = Box<dyn AsyncStream>;

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("unsupported endpoint: {0}")]
    UnsupportedEndpoint(String),
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("backend unavailable: {0}")]
    Unavailable(String),
    #[error("backend failure: {0}")]
    Internal(String),
}

impl BackendError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::UnsupportedEndpoint(_) => ErrorCode::Unsupported,
            Self::InvalidEndpoint(_) => ErrorCode::InvalidRequest,
            Self::Unavailable(_) => ErrorCode::Unavailable,
            Self::Internal(_) => ErrorCode::Internal,
        }
    }
}

#[async_trait]
pub trait DatagramChannel: Send + Sync {
    fn bound_endpoint(&self) -> Option<Endpoint>;
    async fn send_to(&self, endpoint: Endpoint, payload: Vec<u8>) -> Result<(), BackendError>;
    async fn recv_from(&self) -> Result<Datagram, BackendError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Datagram {
    pub endpoint: Endpoint,
    pub payload: Vec<u8>,
}

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    fn capabilities(&self) -> Capabilities;

    fn identity_offers(&self) -> Result<Vec<IdentityOffer>, BackendError>;

    fn status(&self) -> crate::frame::ServiceStatus {
        crate::frame::ServiceStatus {
            connected: true,
            detail: None,
        }
    }

    async fn connect_stream(
        &self,
        identity: &IdentityOffer,
        target: Endpoint,
    ) -> Result<BoxStream, BackendError>;

    async fn accept_stream(
        &self,
        _identity: &IdentityOffer,
    ) -> Result<(BoxStream, Endpoint), BackendError> {
        Err(BackendError::UnsupportedEndpoint(
            "identity cannot receive reliable streams".to_owned(),
        ))
    }

    async fn open_datagrams(
        &self,
        identity: &IdentityOffer,
    ) -> Result<Box<dyn DatagramChannel>, BackendError>;
}

#[derive(Default)]
pub struct SocketBackend;

#[async_trait]
impl Backend for SocketBackend {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            max_datagram_size: MAX_DATAGRAM_SIZE as u32,
            server_name: "socks6-socket-backend".to_owned(),
        }
    }

    fn identity_offers(&self) -> Result<Vec<IdentityOffer>, BackendError> {
        Ok(vec![IdentityOffer {
            id: "default-egress".to_owned(),
            address: None,
            capabilities: IdentityCapabilities {
                send_streams: true,
                send_datagrams: true,
                receive_streams: false,
                receive_datagrams: false,
            },
        }])
    }

    async fn connect_stream(
        &self,
        identity: &IdentityOffer,
        target: Endpoint,
    ) -> Result<BoxStream, BackendError> {
        if !identity.capabilities.send_streams {
            return Err(BackendError::UnsupportedEndpoint(format!(
                "identity {} cannot send reliable streams",
                identity.id
            )));
        }
        let stream = match target {
            Endpoint::Socket(address) => TcpStream::connect(address).await,
            Endpoint::Domain(host, port) => TcpStream::connect((host.as_str(), port)).await,
            Endpoint::Ip(ip) => {
                return Err(BackendError::InvalidEndpoint(format!(
                    "TCP endpoint {ip} has no port"
                )));
            }
            Endpoint::Service { namespace, .. } => {
                return Err(BackendError::UnsupportedEndpoint(namespace));
            }
        }
        .map_err(|error| BackendError::Unavailable(error.to_string()))?;
        Ok(Box::new(stream))
    }

    async fn open_datagrams(
        &self,
        identity: &IdentityOffer,
    ) -> Result<Box<dyn DatagramChannel>, BackendError> {
        if !identity.capabilities.send_datagrams {
            return Err(BackendError::UnsupportedEndpoint(format!(
                "identity {} cannot send unreliable datagrams",
                identity.id
            )));
        }
        let socket = UdpSocket::bind(
            "0.0.0.0:0"
                .parse::<SocketAddr>()
                .expect("static socket address"),
        )
        .await
        .map_err(|error| BackendError::Unavailable(error.to_string()))?;
        let bound = socket
            .local_addr()
            .map_err(|error| BackendError::Internal(error.to_string()))?;
        Ok(Box::new(SocketDatagrams {
            socket,
            bound: Endpoint::Socket(bound),
        }))
    }
}

struct SocketDatagrams {
    socket: UdpSocket,
    bound: Endpoint,
}

#[async_trait]
impl DatagramChannel for SocketDatagrams {
    fn bound_endpoint(&self) -> Option<Endpoint> {
        Some(self.bound.clone())
    }

    async fn send_to(&self, endpoint: Endpoint, payload: Vec<u8>) -> Result<(), BackendError> {
        let sent = match endpoint {
            Endpoint::Socket(address) => self.socket.send_to(&payload, address).await,
            Endpoint::Domain(host, port) => {
                self.socket.send_to(&payload, (host.as_str(), port)).await
            }
            Endpoint::Ip(ip) => {
                return Err(BackendError::InvalidEndpoint(format!(
                    "UDP endpoint {ip} has no port"
                )));
            }
            Endpoint::Service { namespace, .. } => {
                return Err(BackendError::UnsupportedEndpoint(namespace));
            }
        }
        .map_err(|error| BackendError::Unavailable(error.to_string()))?;
        if sent != payload.len() {
            return Err(BackendError::Internal(format!(
                "UDP socket accepted {sent} of {} bytes",
                payload.len()
            )));
        }
        Ok(())
    }

    async fn recv_from(&self) -> Result<Datagram, BackendError> {
        let mut buffer = vec![0; MAX_DATAGRAM_SIZE];
        let (size, from) = self
            .socket
            .recv_from(&mut buffer)
            .await
            .map_err(|error| BackendError::Unavailable(error.to_string()))?;
        buffer.truncate(size);
        Ok(Datagram {
            endpoint: Endpoint::Socket(from),
            payload: buffer,
        })
    }
}
