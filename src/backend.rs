use crate::endpoint::{Endpoint, EndpointKind};
use crate::frame::{Capabilities, ErrorCode, MAX_DATAGRAM_SIZE};
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
pub trait DatagramAssociation: Send + Sync {
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

    fn status(&self) -> crate::frame::ServiceStatus {
        crate::frame::ServiceStatus {
            connected: true,
            detail: None,
        }
    }

    async fn connect_tcp(&self, target: Endpoint) -> Result<BoxStream, BackendError>;

    async fn open_udp(
        &self,
        bind: Option<Endpoint>,
    ) -> Result<Box<dyn DatagramAssociation>, BackendError>;
}

#[derive(Default)]
pub struct SocketBackend;

#[async_trait]
impl Backend for SocketBackend {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            tcp_connect: true,
            udp_associate: true,
            udp_bind: true,
            endpoint_kinds: vec![EndpointKind::Socket, EndpointKind::Ip, EndpointKind::Domain],
            max_datagram_size: MAX_DATAGRAM_SIZE as u32,
            server_name: "socks6-socket-backend".to_owned(),
        }
    }

    async fn connect_tcp(&self, target: Endpoint) -> Result<BoxStream, BackendError> {
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

    async fn open_udp(
        &self,
        bind: Option<Endpoint>,
    ) -> Result<Box<dyn DatagramAssociation>, BackendError> {
        let bind_addr = match bind {
            None => "0.0.0.0:0"
                .parse::<SocketAddr>()
                .expect("static socket address"),
            Some(Endpoint::Socket(address)) => address,
            Some(Endpoint::Ip(ip)) => SocketAddr::new(ip, 0),
            Some(Endpoint::Domain(host, _)) => {
                return Err(BackendError::InvalidEndpoint(format!(
                    "cannot bind UDP to domain {host}"
                )));
            }
            Some(Endpoint::Service { namespace, .. }) => {
                return Err(BackendError::UnsupportedEndpoint(namespace));
            }
        };
        let socket = UdpSocket::bind(bind_addr)
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
impl DatagramAssociation for SocketDatagrams {
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
