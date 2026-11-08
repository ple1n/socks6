use std::fmt;
use std::net::{IpAddr, SocketAddr};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Endpoint {
    Socket(SocketAddr),
    Ip(IpAddr),
    Domain(String, u16),
    Service {
        namespace: String,
        address: Vec<u8>,
        port: Option<u16>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EndpointKind {
    Socket,
    Ip,
    Domain,
    Service,
}

impl Endpoint {
    pub fn kind(&self) -> EndpointKind {
        match self {
            Self::Socket(_) => EndpointKind::Socket,
            Self::Ip(_) => EndpointKind::Ip,
            Self::Domain(_, _) => EndpointKind::Domain,
            Self::Service { .. } => EndpointKind::Service,
        }
    }

    pub fn validate(&self) -> std::result::Result<(), &'static str> {
        match self {
            Self::Domain(host, _) if host.is_empty() || host.len() > 253 => {
                Err("domain endpoint must contain 1 to 253 bytes")
            }
            Self::Service {
                namespace, address, ..
            } if namespace.is_empty() || namespace.len() > 128 || address.len() > 4096 => {
                Err("invalid service endpoint namespace or address length")
            }
            _ => Ok(()),
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Socket(addr) => write!(f, "{addr}"),
            Self::Ip(ip) => write!(f, "{ip}"),
            Self::Domain(host, port) => write!(f, "{host}:{port}"),
            Self::Service {
                namespace,
                address,
                port,
            } => {
                write!(f, "{namespace}:")?;
                for byte in address {
                    write!(f, "{byte:02x}")?;
                }
                if let Some(port) = port {
                    write!(f, ":{port}")?;
                }
                Ok(())
            }
        }
    }
}
