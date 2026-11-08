use crate::endpoint::{Endpoint, EndpointKind};
use crate::error::{Error, Result};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAGIC: [u8; 2] = *b"S6";
pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_FRAME_SIZE: usize = 1024 * 1024;
pub const MAX_DATAGRAM_SIZE: usize = 65_507;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Capabilities {
    pub tcp_connect: bool,
    pub udp_associate: bool,
    pub udp_bind: bool,
    pub endpoint_kinds: Vec<EndpointKind>,
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
    },
    OpenTcp {
        target: Endpoint,
    },
    TcpReady {
        source: Option<Endpoint>,
    },
    OpenUdp {
        bind: Option<Endpoint>,
    },
    UdpReady {
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
            } => {
                encoder.u16(*version);
                encoder.u8(2);
                encoder.u32(*max_frame_size);
                encoder.capabilities(capabilities)?;
            }
            Self::OpenTcp { target } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(3);
                encoder.endpoint(target)?;
            }
            Self::TcpReady { source } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(4);
                encoder.optional_endpoint(source.as_ref())?;
            }
            Self::OpenUdp { bind } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(5);
                encoder.optional_endpoint(bind.as_ref())?;
            }
            Self::UdpReady { bound } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(6);
                encoder.optional_endpoint(bound.as_ref())?;
            }
            Self::Datagram { endpoint, payload } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(7);
                encoder.endpoint(endpoint)?;
                encoder.bytes(payload)?;
            }
            Self::StatusRequest => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(8);
            }
            Self::Status(status) => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(9);
                encoder.u8(u8::from(status.connected));
                encoder.optional_string(status.detail.as_deref())?;
            }
            Self::Ping => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(10);
            }
            Self::Pong => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(11);
            }
            Self::Close => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(12);
            }
            Self::Error { code, message } => {
                encoder.u16(PROTOCOL_VERSION);
                encoder.u8(13);
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
            },
            3 => {
                ensure_version(version)?;
                Self::OpenTcp {
                    target: decoder.endpoint()?,
                }
            }
            4 => {
                ensure_version(version)?;
                Self::TcpReady {
                    source: decoder.optional_endpoint()?,
                }
            }
            5 => {
                ensure_version(version)?;
                Self::OpenUdp {
                    bind: decoder.optional_endpoint()?,
                }
            }
            6 => {
                ensure_version(version)?;
                Self::UdpReady {
                    bound: decoder.optional_endpoint()?,
                }
            }
            7 => {
                ensure_version(version)?;
                Self::Datagram {
                    endpoint: decoder.endpoint()?,
                    payload: decoder.bytes()?,
                }
            }
            8 => {
                ensure_version(version)?;
                Self::StatusRequest
            }
            9 => {
                ensure_version(version)?;
                Self::Status(ServiceStatus {
                    connected: decoder.boolean()?,
                    detail: decoder.optional_string()?,
                })
            }
            10 => {
                ensure_version(version)?;
                Self::Ping
            }
            11 => {
                ensure_version(version)?;
                Self::Pong
            }
            12 => {
                ensure_version(version)?;
                Self::Close
            }
            13 => {
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
            } => {
                if *version == 0 || *max_frame_size < 128 {
                    return Err(Error::Protocol("invalid welcome parameters".to_owned()));
                }
                if capabilities.max_datagram_size as usize > MAX_DATAGRAM_SIZE {
                    return Err(Error::Protocol(
                        "server advertised an unsupported datagram size".to_owned(),
                    ));
                }
                Ok(())
            }
            Self::OpenTcp { target } => check_endpoint(target),
            Self::TcpReady { source } => source.as_ref().map_or(Ok(()), check_endpoint),
            Self::OpenUdp { bind } => bind.as_ref().map_or(Ok(()), check_endpoint),
            Self::UdpReady { bound } => bound.as_ref().map_or(Ok(()), check_endpoint),
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
        self.u8(u8::from(capabilities.tcp_connect));
        self.u8(u8::from(capabilities.udp_associate));
        self.u8(u8::from(capabilities.udp_bind));
        self.u32(capabilities.max_datagram_size);
        self.string(&capabilities.server_name)?;
        let count = u16::try_from(capabilities.endpoint_kinds.len())
            .map_err(|_| Error::Protocol("too many endpoint kinds".to_owned()))?;
        self.u16(count);
        for kind in &capabilities.endpoint_kinds {
            self.u8(endpoint_kind_to_wire(*kind));
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
        let tcp_connect = self.boolean()?;
        let udp_associate = self.boolean()?;
        let udp_bind = self.boolean()?;
        let max_datagram_size = self.u32()?;
        let server_name = self.string()?;
        let count = usize::from(self.u16()?);
        let mut endpoint_kinds = Vec::with_capacity(count);
        for _ in 0..count {
            endpoint_kinds.push(endpoint_kind_from_wire(self.u8()?)?);
        }
        Ok(Capabilities {
            tcp_connect,
            udp_associate,
            udp_bind,
            endpoint_kinds,
            max_datagram_size,
            server_name,
        })
    }

    fn finish(self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::Protocol("trailing bytes in SOCKS6 frame".to_owned()))
        }
    }
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

fn endpoint_kind_to_wire(kind: EndpointKind) -> u8 {
    match kind {
        EndpointKind::Socket => 1,
        EndpointKind::Ip => 2,
        EndpointKind::Domain => 3,
        EndpointKind::Service => 4,
    }
}

fn endpoint_kind_from_wire(value: u8) -> Result<EndpointKind> {
    match value {
        1 => Ok(EndpointKind::Socket),
        2 => Ok(EndpointKind::Ip),
        3 => Ok(EndpointKind::Domain),
        4 => Ok(EndpointKind::Service),
        _ => Err(Error::Protocol(format!(
            "unknown endpoint capability {value}"
        ))),
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
