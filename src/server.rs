use crate::backend::{AsyncStream, Backend, BackendError, DatagramChannel};
use crate::error::{Error, Result};
use crate::frame::{ErrorCode, Frame, MAX_FRAME_SIZE, PROTOCOL_VERSION};
use crate::identity::IdentityUse;
use crate::identity::{IdentityCapabilities, IdentityLease, IdentityOffer, IdentitySelection};
use async_trait::async_trait;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::io::{copy_bidirectional, split};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::warn;

const MAX_ACTIVE_IDENTITY_LEASES: usize = 4096;

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
    connection_slots: Arc<Semaphore>,
    identity_leases: Arc<Mutex<IdentityLeaseState>>,
}

#[derive(Default)]
struct IdentityLeaseState {
    next_id: u64,
    leases: HashMap<String, IdentityOffer>,
}

impl Server {
    pub fn new<B>(backend: B) -> Self
    where
        B: Backend,
    {
        Self {
            backend: Arc::new(backend),
            max_frame_size: MAX_FRAME_SIZE,
            connection_slots: Arc::new(Semaphore::new(1024)),
            identity_leases: Arc::new(Mutex::new(IdentityLeaseState::default())),
        }
    }

    pub fn from_shared_backend(backend: Arc<dyn Backend>) -> Self {
        Self {
            backend,
            max_frame_size: MAX_FRAME_SIZE,
            connection_slots: Arc::new(Semaphore::new(1024)),
            identity_leases: Arc::new(Mutex::new(IdentityLeaseState::default())),
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

    pub fn with_max_connections(mut self, max_connections: usize) -> Result<Self> {
        if max_connections == 0 {
            return Err(Error::Protocol(
                "maximum connection count must be nonzero".to_owned(),
            ));
        }
        self.connection_slots = Arc::new(Semaphore::new(max_connections));
        Ok(self)
    }

    pub async fn run<L>(self: Arc<Self>, mut listener: L) -> Result<()>
    where
        L: Listener,
    {
        loop {
            let stream = listener.accept().await?;
            let permit = match Arc::clone(&self.connection_slots).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    warn!("SOCKS6 connection limit reached; rejecting client");
                    drop(stream);
                    continue;
                }
            };
            let server = Arc::clone(&self);
            tokio::spawn(async move {
                let _permit = permit;
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
        let identities = match self.backend.identity_offers() {
            Ok(identities) => identities,
            Err(error) => {
                write_backend_error(&mut stream, error, max_frame_size).await?;
                return Ok(());
            }
        };
        Frame::Welcome {
            version: PROTOCOL_VERSION,
            max_frame_size: max_frame_size as u32,
            capabilities: self.backend.capabilities(),
            identities,
        }
        .write_to_with_limit(&mut stream, max_frame_size)
        .await?;

        loop {
            match Frame::read_from_with_limit(&mut stream, max_frame_size).await? {
                Frame::IdentityOffersRequest => {
                    let identities = match self.backend.identity_offers() {
                        Ok(identities) => identities,
                        Err(error) => {
                            write_backend_error(&mut stream, error, max_frame_size).await?;
                            continue;
                        }
                    };
                    Frame::IdentityOffers { identities }
                        .write_to_with_limit(&mut stream, max_frame_size)
                        .await?;
                }
                Frame::LeaseIdentity { identity_id } => {
                    match self.grant_identity(identity_id.as_deref(), None) {
                        Ok(lease) => {
                            Frame::IdentityLeased { lease }
                                .write_to_with_limit(&mut stream, max_frame_size)
                                .await?
                        }
                        Err(error) => {
                            write_backend_error(&mut stream, error, max_frame_size).await?
                        }
                    }
                }
                Frame::ReleaseIdentity { lease_id } => match self.release_identity(&lease_id) {
                    Ok(()) => {
                        Frame::IdentityReleased
                            .write_to_with_limit(&mut stream, max_frame_size)
                            .await?
                    }
                    Err(error) => write_backend_error(&mut stream, error, max_frame_size).await?,
                },
                Frame::OpenStream { identity, target } => {
                    let identity = match self.resolve_identity(identity, |caps| caps.send_streams) {
                        Ok(identity) => identity,
                        Err(error) => {
                            write_backend_error(&mut stream, error, max_frame_size).await?;
                            return Ok(());
                        }
                    };
                    let mut upstream = match self
                        .backend
                        .connect_stream(&identity.identity, target)
                        .await
                    {
                        Ok(stream) => stream,
                        Err(error) => {
                            write_backend_error(&mut stream, error, max_frame_size).await?;
                            return Ok(());
                        }
                    };
                    Frame::StreamReady {
                        identity,
                        source: None,
                    }
                    .write_to_with_limit(&mut stream, max_frame_size)
                    .await?;
                    copy_bidirectional(&mut stream, &mut upstream).await?;
                    return Ok(());
                }
                Frame::AcceptStream { identity } => {
                    let identity =
                        match self.resolve_identity(identity, |caps| caps.receive_streams) {
                            Ok(identity) => identity,
                            Err(error) => {
                                write_backend_error(&mut stream, error, max_frame_size).await?;
                                return Ok(());
                            }
                        };
                    let (mut incoming, peer) =
                        match self.backend.accept_stream(&identity.identity).await {
                            Ok(stream) => stream,
                            Err(error) => {
                                write_backend_error(&mut stream, error, max_frame_size).await?;
                                return Ok(());
                            }
                        };
                    Frame::StreamAccepted { identity, peer }
                        .write_to_with_limit(&mut stream, max_frame_size)
                        .await?;
                    copy_bidirectional(&mut stream, &mut incoming).await?;
                    return Ok(());
                }
                Frame::OpenDatagrams { identity } => {
                    let identity = match self.resolve_identity(identity, |caps| caps.send_datagrams)
                    {
                        Ok(identity) => identity,
                        Err(error) => {
                            write_backend_error(&mut stream, error, max_frame_size).await?;
                            return Ok(());
                        }
                    };
                    let association = match self.backend.open_datagrams(&identity.identity).await {
                        Ok(association) => association,
                        Err(error) => {
                            write_backend_error(&mut stream, error, max_frame_size).await?;
                            return Ok(());
                        }
                    };
                    let bound = association.bound_endpoint();
                    Frame::DatagramsReady { identity, bound }
                        .write_to_with_limit(&mut stream, max_frame_size)
                        .await?;
                    self.serve_datagrams(stream, association, max_frame_size)
                        .await?;
                    return Ok(());
                }
                Frame::StatusRequest => {
                    Frame::Status(self.backend.status())
                        .write_to_with_limit(&mut stream, max_frame_size)
                        .await?;
                }
                Frame::Ping => {
                    Frame::Pong
                        .write_to_with_limit(&mut stream, max_frame_size)
                        .await?;
                }
                Frame::Close => return Ok(()),
                _ => {
                    Frame::Error {
                        code: ErrorCode::InvalidRequest,
                        message: "expected an identity or stream/datagram request, StatusRequest, Ping, or Close".to_owned(),
                    }
                    .write_to_with_limit(&mut stream, max_frame_size)
                    .await?;
                }
            }
        }
    }

    fn grant_identity(
        &self,
        identity_id: Option<&str>,
        required: Option<fn(&IdentityCapabilities) -> bool>,
    ) -> std::result::Result<IdentityLease, BackendError> {
        let identity = self.select_identity(identity_id, required)?;

        let mut state = self
            .identity_leases
            .lock()
            .map_err(|error| BackendError::Internal(format!("identity lease map: {error}")))?;
        if state.leases.len() >= MAX_ACTIVE_IDENTITY_LEASES {
            return Err(BackendError::Unavailable(format!(
                "identity lease limit reached ({MAX_ACTIVE_IDENTITY_LEASES})"
            )));
        }
        state.next_id = state.next_id.wrapping_add(1).max(1);
        let lease_id = format!("{}-{:x}", std::process::id(), state.next_id);
        state.leases.insert(lease_id.clone(), identity.clone());
        Ok(IdentityLease {
            id: lease_id,
            identity,
        })
    }

    fn resolve_identity(
        &self,
        selection: IdentitySelection,
        required: fn(&IdentityCapabilities) -> bool,
    ) -> std::result::Result<IdentityUse, BackendError> {
        match selection {
            IdentitySelection::Lease(id) => {
                let state = self.identity_leases.lock().map_err(|error| {
                    BackendError::Internal(format!("identity lease map: {error}"))
                })?;
                let identity = state.leases.get(&id).cloned().ok_or_else(|| {
                    BackendError::InvalidEndpoint(format!(
                        "unknown or released identity lease {id}"
                    ))
                })?;
                if !required(&identity.capabilities) {
                    return Err(BackendError::UnsupportedEndpoint(format!(
                        "identity {} cannot perform this operation",
                        identity.id
                    )));
                }
                Ok(IdentityUse {
                    identity,
                    lease_id: Some(id),
                })
            }
            IdentitySelection::Offer(id) => {
                self.select_identity(Some(&id), Some(required))
                    .map(|identity| IdentityUse {
                        identity,
                        lease_id: None,
                    })
            }
            IdentitySelection::Any => {
                self.select_identity(None, Some(required))
                    .map(|identity| IdentityUse {
                        identity,
                        lease_id: None,
                    })
            }
        }
    }

    fn select_identity(
        &self,
        identity_id: Option<&str>,
        required: Option<fn(&IdentityCapabilities) -> bool>,
    ) -> std::result::Result<IdentityOffer, BackendError> {
        self.backend
            .identity_offers()?
            .into_iter()
            .find(|identity| {
                identity_id.map_or(true, |id| identity.id == id)
                    && required.map_or(true, |supports| supports(&identity.capabilities))
            })
            .ok_or_else(|| {
                BackendError::UnsupportedEndpoint(match identity_id {
                    Some(id) => format!("identity {id} is not available for this operation"),
                    None => "no identity is available for this operation".to_owned(),
                })
            })
    }

    fn release_identity(&self, lease_id: &str) -> std::result::Result<(), BackendError> {
        let removed = self
            .identity_leases
            .lock()
            .map_err(|error| BackendError::Internal(format!("identity lease map: {error}")))?
            .leases
            .remove(lease_id);
        if removed.is_some() {
            Ok(())
        } else {
            Err(BackendError::InvalidEndpoint(format!(
                "unknown or released identity lease {lease_id}"
            )))
        }
    }

    async fn serve_datagrams<S>(
        &self,
        stream: S,
        association: Box<dyn DatagramChannel>,
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
                                write_backend_error(&mut writer, error, max_frame_size).await?;
                                return Ok(());
                            }
                        }
                        Frame::Close => return Ok(()),
                        Frame::Ping => {
                            Frame::Pong
                                .write_to_with_limit(&mut writer, max_frame_size)
                                .await?
                        }
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
                            write_backend_error(&mut writer, error, max_frame_size).await?;
                            return Ok(());
                        }
                    };
                    Frame::Datagram {
                        endpoint: datagram.endpoint,
                        payload: datagram.payload,
                    }
                    .write_to_with_limit(&mut writer, max_frame_size)
                    .await?;
                }
            }
        }
    }
}

async fn write_backend_error<W>(
    writer: &mut W,
    error: BackendError,
    max_frame_size: usize,
) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    Frame::Error {
        code: error.code(),
        message: error.to_string(),
    }
    .write_to_with_limit(writer, max_frame_size)
    .await
}
