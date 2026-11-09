use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::identity::{
    IdentityAddress, IdentityCapabilities, IdentityLease, IdentityOffer, IdentitySelection,
    IdentityUse, MAX_IDENTITY_ID_LENGTH,
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAGIC: [u8; 2] = *b"S6";
pub const PROTOCOL_VERSION: u16 = 2;
pub const MAX_FRAME_SIZE: usize = 1024 * 1024;
pub const MAX_DATAGRAM_SIZE: usize = 65_507;
pub const MAX_IDENTITY_OFFERS: usize = 256;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Capabilities {
    pub max_datagram_size: u32,
    pub server_name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    InvalidRequest,
    Unsupported,
    Unavailable,
    LimitExceeded,
    Internal,
    VersionMismatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceStatus {
    pub connected: bool,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Hello {
        version: u16,
        max_frame_size: u32,
    },
    Welcome {
        version: u16,
        max_frame_size: u32,
        capabilities: Capabilities,
        identities: Vec<IdentityOffer>,
    },
    IdentityOffersRequest,
    IdentityOffers {
        identities: Vec<IdentityOffer>,
    },
    LeaseIdentity {
        identity_id: Option<String>,
    },
    IdentityLeased {
        lease: IdentityLease,
    },
    ReleaseIdentity {
        lease_id: String,
    },
    IdentityReleased,
    OpenStream {
        identity: IdentitySelection,
        target: Endpoint,
    },
    AcceptStream {
        identity: IdentitySelection,
    },
    StreamReady {
        identity: IdentityUse,
        source: Option<Endpoint>,
    },
    StreamAccepted {
        identity: IdentityUse,
        peer: Endpoint,
    },
    OpenDatagrams {
        identity: IdentitySelection,
    },
    DatagramsReady {
        identity: IdentityUse,
        bound: Option<Endpoint>,
    },
    Datagram {
        endpoint: Endpoint,
        payload: Vec<u8>,
    },
    StatusRequest,
    Status(ServiceStatus),
    Ping,
    Pong,
    Close,
    Error {
        code: ErrorCode,
        message: String,
    },
}

impl Frame {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut encoder = Encoder::default();
        encoder.raw(&MAGIC);
        match self {
            Self::Hello {
                version,
                max_frame_size,
            } => {
                encoder.u16(*version);
                encoder.u8(1);
                encoder.u32(*max_frame_size);
            }
            Self::Welcome {
                version,
                max_frame_size,
                capabilities,
                identities,
            } => {
                encoder.u16(*version);
                encoder.u8(2);
                encoder.u32(*max_frame_size);
                encoder.capabilities(capabilities)?;
                encoder.identities(identities)?;
            }
            Self::IdentityOffersRequest => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(3);
            }
            Self::IdentityOffers { identities } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(4);
                encoder.identities(identities)?;
            }
            Self::LeaseIdentity { identity_id } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(5);
                encoder.optional_string(identity_id.as_deref())?;
            }
            Self::IdentityLeased { lease } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(6);
                encoder.identity_lease(lease)?;
            }
            Self::ReleaseIdentity { lease_id } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(7);
                encoder.string(lease_id)?;
            }
            Self::IdentityReleased => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(8);
            }
            Self::OpenStream { identity, target } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(9);
                encoder.identity_selection(identity)?;
                encoder.endpoint(target)?;
            }
            Self::StreamReady { identity, source } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(10);
                encoder.identity_use(identity)?;
                encoder.optional_endpoint(source.as_ref())?;
            }
            Self::AcceptStream { identity } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(20);
                encoder.identity_selection(identity)?;
            }
            Self::StreamAccepted { identity, peer } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(21);
                encoder.identity_use(identity)?;
                encoder.endpoint(peer)?;
            }
            Self::OpenDatagrams { identity } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(11);
                encoder.identity_selection(identity)?;
            }
            Self::DatagramsReady { identity, bound } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(12);
                encoder.identity_use(identity)?;
                encoder.optional_endpoint(bound.as_ref())?;
            }
            Self::Datagram { endpoint, payload } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(13);
                encoder.endpoint(endpoint)?;
                encoder.bytes(payload)?;
            }
            Self::StatusRequest => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(14);
            }
            Self::Status(status) => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(15);
                encoder.u8(u8::from(status.connected));
                encoder.optional_string(status.detail.as_deref())?;
            }
            Self::Ping => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(16);
            }
            Self::Pong => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(17);
            }
            Self::Close => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(18);
            }
            Self::Error { code, message } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(19);
                encoder.u8(error_code_to_wire(*code));
                encoder.string(message)?;
            }
        }
        if encoder.bytes.len() > MAX_FRAME_SIZE {
            return Err(Error::FrameTooLarge {
                size: encoder.bytes.len(),
                limit: MAX_FRAME_SIZE,
            });
        }
        Ok(encoder.bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_FRAME_SIZE {
            return Err(Error::FrameTooLarge {
                size: bytes.len(),
                limit: MAX_FRAME_SIZE,
            });
        }
        let mut decoder = Decoder::new(bytes);
        if decoder.take(2)? != MAGIC {
            return Err(Error::Protocol("invalid SOCKS6 frame magic".to_owned()));
        }
        let version = decoder.u16()?;
        let opcode = decoder.u8()?;
        let frame = match opcode {
            1 => Self::Hello {
                version,
                max_frame_size: decoder.u32()?,
            },
            2 => Self::Welcome {
                version,
                max_frame_size: decoder.u32()?,
                capabilities: decoder.capabilities()?,
                identities: decoder.identities()?,
            },
            3 => {
                ensure_version(version)?;
                Self::IdentityOffersRequest
            }
            4 => {
                ensure_version(version)?;
                Self::IdentityOffers {
                    identities: decoder.identities()?,
                }
            }
            5 => {
                ensure_version(version)?;
                Self::LeaseIdentity {
                    identity_id: decoder.optional_string()?,
                }
            }
            6 => {
                ensure_version(version)?;
                Self::IdentityLeased {
                    lease: decoder.identity_lease()?,
                }
            }
            7 => {
                ensure_version(version)?;
                Self::ReleaseIdentity {
                    lease_id: decoder.string()?,
                }
            }
            8 => {
                ensure_version(version)?;
                Self::IdentityReleased
            }
            9 => {
                ensure_version(version)?;
                Self::OpenStream {
                    identity: decoder.identity_selection()?,
                    target: decoder.endpoint()?,
                }
            }
            10 => {
                ensure_version(version)?;
                Self::StreamReady {
                    identity: decoder.identity_use()?,
                    source: decoder.optional_endpoint()?,
                }
            }
            20 => {
                ensure_version(version)?;
                Self::AcceptStream {
                    identity: decoder.identity_selection()?,
                }
            }
            21 => {
                ensure_version(version)?;
                Self::StreamAccepted {
                    identity: decoder.identity_use()?,
                    peer: decoder.endpoint()?,
                }
            }
            11 => {
                ensure_version(version)?;
                Self::OpenDatagrams {
                    identity: decoder.identity_selection()?,
                }
            }
            12 => {
                ensure_version(version)?;
                Self::DatagramsReady {
                    identity: decoder.identity_use()?,
                    bound: decoder.optional_endpoint()?,
                }
            }
            13 => {
                ensure_version(version)?;
                Self::Datagram {
                    endpoint: decoder.endpoint()?,
                    payload: decoder.bytes()?,
                }
            }
            14 => {
                ensure_version(version)?;
                Self::StatusRequest
            }
            15 => {
                ensure_version(version)?;
                Self::Status(ServiceStatus {
                    connected: decoder.boolean()?,
                    detail: decoder.optional_string()?,
                })
            }
            16 => {
                ensure_version(version)?;
                Self::Ping
            }
            17 => {
                ensure_version(version)?;
                Self::Pong
            }
            18 => {
                ensure_version(version)?;
                Self::Close
            }
            19 => {
                ensure_version(version)?;
                Self::Error {
                    code: error_code_from_wire(decoder.u8()?)?,
                    message: decoder.string()?,
                }
            }
            _ => {
                return Err(Error::Protocol(format!(
                    "unknown SOCKS6 frame opcode {opcode}"
                )));
            }
        };
        decoder.finish()?;
        frame.validate()?;
        Ok(frame)
    }

    pub async fn write_to<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<()> {
        self.write_to_with_limit(writer, MAX_FRAME_SIZE).await
    }

    pub async fn write_to_with_limit<W: AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
        max_frame_size: usize,
    ) -> Result<()> {
        let body = self.to_bytes()?;
        if body.len() > max_frame_size.min(MAX_FRAME_SIZE) {
            return Err(Error::FrameTooLarge {
                size: body.len(),
                limit: max_frame_size.min(MAX_FRAME_SIZE),
            });
        }
        let len = u32::try_from(body.len())
            .map_err(|_| Error::FrameTooLarge {
                size: body.len(),
                limit: MAX_FRAME_SIZE,
            })?
            .to_be_bytes();
        writer.write_all(&len).await?;
        writer.write_all(&body).await?;
        Ok(())
    }

    pub async fn read_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        Self::read_from_with_limit(reader, MAX_FRAME_SIZE).await
    }

    pub async fn read_from_with_limit<R: AsyncRead + Unpin>(
        reader: &mut R,
        max_frame_size: usize,
    ) -> Result<Self> {
        let mut len_buf = [0; 4];
        reader.read_exact(&mut len_buf).await?;
        let size = u32::from_be_bytes(len_buf) as usize;
        let limit = max_frame_size.min(MAX_FRAME_SIZE);
        if size == 0 || size > limit {
            return Err(Error::FrameTooLarge { size, limit });
        }
        let mut body = vec![0; size];
        reader.read_exact(&mut body).await?;
        Self::from_bytes(&body)
    }

    fn validate(&self) -> Result<()> {
        let check_endpoint = |endpoint: &Endpoint| {
            endpoint
                .validate()
                .map_err(|message| Error::Protocol(message.to_owned()))
        };
        match self {
            Self::Hello {
                version,
                max_frame_size,
            } if *version == 0 || *max_frame_size < 128 => {
                Err(Error::Protocol("invalid hello parameters".to_owned()))
            }
            Self::Welcome {
                version,
                max_frame_size,
                capabilities,
                identities,
            } => {
                if *version == 0 || *max_frame_size < 128 {
                    return Err(Error::Protocol("invalid welcome parameters".to_owned()));
                }
                if capabilities.max_datagram_size as usize > MAX_DATAGRAM_SIZE {
                    return Err(Error::Protocol(
                        "server advertised an unsupported datagram size".to_owned(),
                    ));
                }
                validate_identities(identities)
            }
            Self::IdentityOffers { identities } => validate_identities(identities),
            Self::LeaseIdentity {
                identity_id: Some(id),
            }
            | Self::ReleaseIdentity { lease_id: id } => check_identity_id(id),
            Self::IdentityLeased { lease } => lease.validate(),
            Self::OpenStream { identity, target } => {
                identity.validate()?;
                check_endpoint(target)
            }
            Self::StreamReady { identity, source } => {
                identity.validate()?;
                source.as_ref().map_or(Ok(()), check_endpoint)
            }
            Self::AcceptStream { identity } => identity.validate(),
            Self::StreamAccepted { identity, peer } => {
                identity.validate()?;
                check_endpoint(peer)
            }
            Self::OpenDatagrams { identity } => identity.validate(),
            Self::DatagramsReady { identity, bound } => {
                identity.validate()?;
                bound.as_ref().map_or(Ok(()), check_endpoint)
            }
            Self::Datagram { endpoint, payload } => {
                check_endpoint(endpoint)?;
                if payload.len() > MAX_DATAGRAM_SIZE {
                    return Err(Error::FrameTooLarge {
                        size: payload.len(),
                        limit: MAX_DATAGRAM_SIZE,
                    });
                }
                Ok(())
            }
            Self::Error { message, .. } if message.len() > 4096 => {
                Err(Error::Protocol("error message is too long".to_owned()))
            }
            Self::Status(ServiceStatus {
                detail: Some(detail),
                ..
            }) if detail.len() > 4096 => {
                Err(Error::Protocol("status detail is too long".to_owned()))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn raw(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u16(&mut self, value: u16) {
        self.raw(&value.to_be_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.raw(&value.to_be_bytes());
    }

    fn string(&mut self, value: &str) -> Result<()> {
        let len = u16::try_from(value.len())
            .map_err(|_| Error::Protocol("string exceeds 65535 bytes".to_owned()))?;
        self.u16(len);
        self.raw(value.as_bytes());
        Ok(())
    }

    fn optional_string(&mut self, value: Option<&str>) -> Result<()> {
        match value {
            Some(value) => {
                self.u8(1);
                self.string(value)
            }
            None => {
                self.u8(0);
                Ok(())
            }
        }
    }

    fn bytes(&mut self, value: &[u8]) -> Result<()> {
        let len = u32::try_from(value.len())
            .map_err(|_| Error::Protocol("byte string exceeds 4 GiB".to_owned()))?;
        self.u32(len);
        self.raw(value);
        Ok(())
    }

    fn endpoint(&mut self, endpoint: &Endpoint) -> Result<()> {
        match endpoint {
            Endpoint::Socket(SocketAddr::V4(address)) => {
                self.u8(1);
                self.raw(&address.ip().octets());
                self.u16(address.port());
            }
            Endpoint::Socket(SocketAddr::V6(address)) => {
                self.u8(2);
                self.raw(&address.ip().octets());
                self.u16(address.port());
                self.u32(address.flowinfo());
                self.u32(address.scope_id());
            }
            Endpoint::Ip(IpAddr::V4(address)) => {
                self.u8(3);
                self.raw(&address.octets());
            }
            Endpoint::Ip(IpAddr::V6(address)) => {
                self.u8(4);
                self.raw(&address.octets());
            }
            Endpoint::Domain(host, port) => {
                self.u8(5);
                self.string(host)?;
                self.u16(*port);
            }
            Endpoint::Service {
                namespace,
                address,
                port,
            } => {
                self.u8(6);
                self.string(namespace)?;
                self.bytes(address)?;
                match port {
                    Some(port) => {
                        self.u8(1);
                        self.u16(*port);
                    }
                    None => self.u8(0),
                }
            }
        }
        Ok(())
    }

    fn optional_endpoint(&mut self, endpoint: Option<&Endpoint>) -> Result<()> {
        match endpoint {
            Some(endpoint) => {
                self.u8(1);
                self.endpoint(endpoint)
            }
            None => {
                self.u8(0);
                Ok(())
            }
        }
    }

    fn capabilities(&mut self, capabilities: &Capabilities) -> Result<()> {
        self.u32(capabilities.max_datagram_size);
        self.string(&capabilities.server_name)?;
        Ok(())
    }

    fn identity_capabilities(&mut self, capabilities: &IdentityCapabilities) {
        self.u8(u8::from(capabilities.send_streams));
        self.u8(u8::from(capabilities.send_datagrams));
        self.u8(u8::from(capabilities.receive_streams));
        self.u8(u8::from(capabilities.receive_datagrams));
    }

    fn identity_offer(&mut self, identity: &IdentityOffer) -> Result<()> {
        self.string(&identity.id)?;
        self.optional_identity_address(identity.address.as_ref())?;
        self.identity_capabilities(&identity.capabilities);
        Ok(())
    }

    fn optional_identity_address(&mut self, address: Option<&IdentityAddress>) -> Result<()> {
        match address {
            Some(address) => {
                self.u8(1);
                match address {
                    IdentityAddress::Socket(SocketAddr::V4(address)) => {
                        self.u8(1);
                        self.raw(&address.ip().octets());
                        self.u16(address.port());
                    }
                    IdentityAddress::Socket(SocketAddr::V6(address)) => {
                        self.u8(2);
                        self.raw(&address.ip().octets());
                        self.u16(address.port());
                        self.u32(address.flowinfo());
                        self.u32(address.scope_id());
                    }
                    IdentityAddress::Ip(IpAddr::V4(address)) => {
                        self.u8(3);
                        self.raw(&address.octets());
                    }
                    IdentityAddress::Ip(IpAddr::V6(address)) => {
                        self.u8(4);
                        self.raw(&address.octets());
                    }
                    IdentityAddress::Domain(host) => {
                        self.u8(5);
                        self.string(host)?;
                    }
                    IdentityAddress::Service { namespace, address } => {
                        self.u8(6);
                        self.string(namespace)?;
                        self.bytes(address)?;
                    }
                }
                Ok(())
            }
            None => {
                self.u8(0);
                Ok(())
            }
        }
    }

    fn identities(&mut self, identities: &[IdentityOffer]) -> Result<()> {
        let count = u16::try_from(identities.len())
            .map_err(|_| Error::Protocol("too many identity offers".to_owned()))?;
        self.u16(count);
        for identity in identities {
            self.identity_offer(identity)?;
        }
        Ok(())
    }

    fn identity_lease(&mut self, lease: &IdentityLease) -> Result<()> {
        self.string(&lease.id)?;
        self.identity_offer(&lease.identity)
    }

    fn identity_use(&mut self, identity: &IdentityUse) -> Result<()> {
        self.identity_offer(&identity.identity)?;
        self.optional_string(identity.lease_id.as_deref())
    }

    fn identity_selection(&mut self, identity: &IdentitySelection) -> Result<()> {
        match identity {
            IdentitySelection::Any => self.u8(0),
            IdentitySelection::Offer(id) => {
                self.u8(1);
                self.string(id)?;
            }
            IdentitySelection::Lease(id) => {
                self.u8(2);
                self.string(id)?;
            }
        }
        Ok(())
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(size)
            .ok_or_else(|| Error::Protocol("frame offset overflow".to_owned()))?;
        if end > self.bytes.len() {
            return Err(Error::Protocol("truncated SOCKS6 frame".to_owned()));
        }
        let result = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(result)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn boolean(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(Error::Protocol(format!("invalid boolean value {value}"))),
        }
    }

    fn string(&mut self) -> Result<String> {
        let len = usize::from(self.u16()?);
        let value = std::str::from_utf8(self.take(len)?)
            .map_err(|error| Error::Protocol(format!("invalid UTF-8 string: {error}")))?;
        Ok(value.to_owned())
    }

    fn optional_string(&mut self) -> Result<Option<String>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.string().map(Some),
            value => Err(Error::Protocol(format!("invalid optional flag {value}"))),
        }
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let len = usize::try_from(self.u32()?)
            .map_err(|_| Error::Protocol("byte string length overflow".to_owned()))?;
        Ok(self.take(len)?.to_vec())
    }

    fn endpoint(&mut self) -> Result<Endpoint> {
        match self.u8()? {
            1 => {
                let ip = Ipv4Addr::from(<[u8; 4]>::try_from(self.take(4)?).unwrap());
                Ok(Endpoint::Socket(SocketAddr::new(
                    IpAddr::V4(ip),
                    self.u16()?,
                )))
            }
            2 => {
                let ip = Ipv6Addr::from(<[u8; 16]>::try_from(self.take(16)?).unwrap());
                let port = self.u16()?;
                let flowinfo = self.u32()?;
                let scope_id = self.u32()?;
                Ok(Endpoint::Socket(SocketAddr::V6(SocketAddrV6::new(
                    ip, port, flowinfo, scope_id,
                ))))
            }
            3 => Ok(Endpoint::Ip(IpAddr::V4(Ipv4Addr::from(
                <[u8; 4]>::try_from(self.take(4)?).unwrap(),
            )))),
            4 => Ok(Endpoint::Ip(IpAddr::V6(Ipv6Addr::from(
                <[u8; 16]>::try_from(self.take(16)?).unwrap(),
            )))),
            5 => Ok(Endpoint::Domain(self.string()?, self.u16()?)),
            6 => {
                let namespace = self.string()?;
                let address = self.bytes()?;
                let port = match self.u8()? {
                    0 => None,
                    1 => Some(self.u16()?),
                    value => {
                        return Err(Error::Protocol(format!(
                            "invalid optional port flag {value}"
                        )));
                    }
                };
                Ok(Endpoint::Service {
                    namespace,
                    address,
                    port,
                })
            }
            tag => Err(Error::Protocol(format!("unknown endpoint tag {tag}"))),
        }
    }

    fn optional_endpoint(&mut self) -> Result<Option<Endpoint>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.endpoint().map(Some),
            value => Err(Error::Protocol(format!("invalid optional flag {value}"))),
        }
    }

    fn capabilities(&mut self) -> Result<Capabilities> {
        let max_datagram_size = self.u32()?;
        let server_name = self.string()?;
        Ok(Capabilities {
            max_datagram_size,
            server_name,
        })
    }

    fn identity_capabilities(&mut self) -> Result<IdentityCapabilities> {
        Ok(IdentityCapabilities {
            send_streams: self.boolean()?,
            send_datagrams: self.boolean()?,
            receive_streams: self.boolean()?,
            receive_datagrams: self.boolean()?,
        })
    }

    fn identity_offer(&mut self) -> Result<IdentityOffer> {
        Ok(IdentityOffer {
            id: self.string()?,
            address: self.optional_identity_address()?,
            capabilities: self.identity_capabilities()?,
        })
    }

    fn optional_identity_address(&mut self) -> Result<Option<IdentityAddress>> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let address = match self.u8()? {
                    1 => {
                        let ip = Ipv4Addr::from(<[u8; 4]>::try_from(self.take(4)?).unwrap());
                        IdentityAddress::Socket(SocketAddr::new(IpAddr::V4(ip), self.u16()?))
                    }
                    2 => {
                        let ip = Ipv6Addr::from(<[u8; 16]>::try_from(self.take(16)?).unwrap());
                        let port = self.u16()?;
                        let flowinfo = self.u32()?;
                        let scope_id = self.u32()?;
                        IdentityAddress::Socket(SocketAddr::V6(SocketAddrV6::new(
                            ip, port, flowinfo, scope_id,
                        )))
                    }
                    3 => IdentityAddress::Ip(IpAddr::V4(Ipv4Addr::from(
                        <[u8; 4]>::try_from(self.take(4)?).unwrap(),
                    ))),
                    4 => IdentityAddress::Ip(IpAddr::V6(Ipv6Addr::from(
                        <[u8; 16]>::try_from(self.take(16)?).unwrap(),
                    ))),
                    5 => IdentityAddress::Domain(self.string()?),
                    6 => IdentityAddress::Service {
                        namespace: self.string()?,
                        address: self.bytes()?,
                    },
                    tag => {
                        return Err(Error::Protocol(format!(
                            "unknown identity address tag {tag}"
                        )));
                    }
                };
                Ok(Some(address))
            }
            value => Err(Error::Protocol(format!(
                "invalid optional identity address flag {value}"
            ))),
        }
    }

    fn identities(&mut self) -> Result<Vec<IdentityOffer>> {
        let count = usize::from(self.u16()?);
        if count > MAX_IDENTITY_OFFERS {
            return Err(Error::Protocol("too many identity offers".to_owned()));
        }
        (0..count).map(|_| self.identity_offer()).collect()
    }

    fn identity_lease(&mut self) -> Result<IdentityLease> {
        Ok(IdentityLease {
            id: self.string()?,
            identity: self.identity_offer()?,
        })
    }

    fn identity_use(&mut self) -> Result<IdentityUse> {
        Ok(IdentityUse {
            identity: self.identity_offer()?,
            lease_id: self.optional_string()?,
        })
    }

    fn identity_selection(&mut self) -> Result<IdentitySelection> {
        match self.u8()? {
            0 => Ok(IdentitySelection::Any),
            1 => Ok(IdentitySelection::Offer(self.string()?)),
            2 => Ok(IdentitySelection::Lease(self.string()?)),
            tag => Err(Error::Protocol(format!(
                "unknown identity selection tag {tag}"
            ))),
        }
    }

    fn finish(self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::Protocol("trailing bytes in SOCKS6 frame".to_owned()))
        }
    }
}

fn validate_identities(identities: &[IdentityOffer]) -> Result<()> {
    if identities.len() > MAX_IDENTITY_OFFERS {
        return Err(Error::Protocol("too many identity offers".to_owned()));
    }
    for identity in identities {
        identity.validate()?;
    }
    Ok(())
}

fn check_identity_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > MAX_IDENTITY_ID_LENGTH {
        return Err(Error::Protocol(format!(
            "identity id must contain 1 to {MAX_IDENTITY_ID_LENGTH} bytes"
        )));
    }
    Ok(())
}

fn ensure_version(version: u16) -> Result<()> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(Error::Protocol(format!(
            "unsupported protocol version {version}"
        )))
    }
}

fn error_code_to_wire(code: ErrorCode) -> u8 {
    match code {
        ErrorCode::InvalidRequest => 1,
        ErrorCode::Unsupported => 2,
        ErrorCode::Unavailable => 3,
        ErrorCode::LimitExceeded => 4,
        ErrorCode::Internal => 5,
        ErrorCode::VersionMismatch => 6,
    }
}

fn error_code_from_wire(value: u8) -> Result<ErrorCode> {
    match value {
        1 => Ok(ErrorCode::InvalidRequest),
        2 => Ok(ErrorCode::Unsupported),
        3 => Ok(ErrorCode::Unavailable),
        4 => Ok(ErrorCode::LimitExceeded),
        5 => Ok(ErrorCode::Internal),
        6 => Ok(ErrorCode::VersionMismatch),
        _ => Err(Error::Protocol(format!("unknown error code {value}"))),
    }
}
