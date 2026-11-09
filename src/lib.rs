pub mod backend;
pub mod client;
pub mod endpoint;
pub mod error;
pub mod frame;
pub mod identity;
pub mod server;

pub use backend::{Backend, BackendError, BoxStream, Datagram, DatagramChannel, SocketBackend};
#[cfg(unix)]
pub use client::UnixConnector;
pub use client::{
    Client, Connector, DatagramChannelConnection, IdentitySession, StreamConnection, TcpConnector,
};
pub use endpoint::{Endpoint, EndpointKind};
pub use error::{Error, Result};
pub use frame::{
    Capabilities, ErrorCode, Frame, ServiceStatus, MAX_DATAGRAM_SIZE, MAX_FRAME_SIZE,
    PROTOCOL_VERSION,
};
pub use identity::{
    IdentityAddress, IdentityCapabilities, IdentityLease, IdentityOffer, IdentitySelection,
    IdentityUse, MAX_IDENTITY_ID_LENGTH,
};
pub use server::{Listener, Server};
