use crate::error::{Error, Result};
use std::fmt;
use std::net::{IpAddr, SocketAddr};

pub const MAX_IDENTITY_ID_LENGTH: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityAddress {
    Socket(SocketAddr),
    Ip(IpAddr),
    Domain(String),
    Service { namespace: String, address: Vec<u8> },
}

impl IdentityAddress {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Domain(host) if host.is_empty() || host.len() > 253 => Err(Error::Protocol(
                "identity domain must contain 1 to 253 bytes".to_owned(),
            )),
            Self::Service { namespace, address }
                if namespace.is_empty() || namespace.len() > 128 || address.len() > 4096 =>
            {
                Err(Error::Protocol(
                    "invalid service identity namespace or address length".to_owned(),
                ))
            }
            _ => Ok(()),
        }
    }
}

impl fmt::Display for IdentityAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Socket(address) => write!(f, "{address}"),
            Self::Ip(address) => write!(f, "{address}"),
            Self::Domain(address) => write!(f, "{address}"),
            Self::Service { namespace, address } => {
                write!(f, "{namespace}:")?;
                for byte in address {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Describes which SOCKS6 data abstractions an identity can use.
///
/// Streams mean reliable byte streams and datagrams mean unreliable messages;
/// these semantics do not constrain the underlying network transport.
pub struct IdentityCapabilities {
    pub send_streams: bool,
    pub send_datagrams: bool,
    pub receive_streams: bool,
    pub receive_datagrams: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityOffer {
    pub id: String,
    pub address: Option<IdentityAddress>,
    pub capabilities: IdentityCapabilities,
}

impl IdentityOffer {
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.id.len() > MAX_IDENTITY_ID_LENGTH {
            return Err(Error::Protocol(format!(
                "identity id must contain 1 to {MAX_IDENTITY_ID_LENGTH} bytes"
            )));
        }
        if let Some(address) = &self.address {
            address.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityLease {
    pub id: String,
    pub identity: IdentityOffer,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityUse {
    pub identity: IdentityOffer,
    pub lease_id: Option<String>,
}

impl IdentityUse {
    pub fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        if let Some(lease_id) = &self.lease_id {
            if lease_id.is_empty() || lease_id.len() > MAX_IDENTITY_ID_LENGTH {
                return Err(Error::Protocol(format!(
                    "lease id must contain 1 to {MAX_IDENTITY_ID_LENGTH} bytes"
                )));
            }
        }
        Ok(())
    }
}

impl IdentityLease {
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.id.len() > MAX_IDENTITY_ID_LENGTH {
            return Err(Error::Protocol(format!(
                "lease id must contain 1 to {MAX_IDENTITY_ID_LENGTH} bytes"
            )));
        }
        self.identity.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentitySelection {
    Lease(String),
    Offer(String),
    Any,
}

impl IdentitySelection {
    pub fn validate(&self) -> Result<()> {
        let id = match self {
            Self::Lease(id) | Self::Offer(id) => id,
            Self::Any => return Ok(()),
        };
        if id.is_empty() || id.len() > MAX_IDENTITY_ID_LENGTH {
            return Err(Error::Protocol(format!(
                "identity reference must contain 1 to {MAX_IDENTITY_ID_LENGTH} bytes"
            )));
        }
        Ok(())
    }
}
