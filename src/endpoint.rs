use binrw::{binrw, io::Cursor, BinRead, BinWrite};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

#[binrw]
#[brw(big)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndpointWire {
    #[brw(magic = 0x01u8)]
    Ipv4Socket { ip: [u8; 4], port: u16 },

    #[brw(magic = 0x02u8)]
    Ipv6Socket { ip: [u8; 16], port: u16 },

    #[brw(magic = 0x03u8)]
    Ipv4Bare { ip: [u8; 4] },

    #[brw(magic = 0x04u8)]
    Ipv6Bare { ip: [u8; 16] },

    #[brw(magic = 0x05u8)]
    Lokinet { key: [u8; 32] },

    #[brw(magic = 0x06u8)]
    Domain {
        #[br(temp)]
        #[bw(calc = name.len() as u8)]
        len: u8,
        #[br(count = len, try_map = |bytes: Vec<u8>| String::from_utf8(bytes))]
        #[bw(map = |s: &String| s.as_bytes())]
        name: String,
        port: u16,
    },
}

/// Endpoint address types recognized in SOCKS6
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Endpoint {
    /// Standard UDP/Transport socket (IPv4/IPv6 + Port)
    Socket(SocketAddr),
    /// Bare IP without port (Layer 3 endpoint)
    Ip(IpAddr),
    /// Cryptographic / Overlay network identity (e.g. Lokinet .loki / 32-byte pubkey)
    Lokinet([u8; 32]),
    /// Named endpoint / Domain + Port (e.g. FQDN)
    Domain(String, u16),
}

impl From<&Endpoint> for EndpointWire {
    fn from(ep: &Endpoint) -> Self {
        match ep {
            Endpoint::Socket(SocketAddr::V4(s)) => EndpointWire::Ipv4Socket {
                ip: s.ip().octets(),
                port: s.port(),
            },
            Endpoint::Socket(SocketAddr::V6(s)) => EndpointWire::Ipv6Socket {
                ip: s.ip().octets(),
                port: s.port(),
            },
            Endpoint::Ip(IpAddr::V4(ip)) => EndpointWire::Ipv4Bare { ip: ip.octets() },
            Endpoint::Ip(IpAddr::V6(ip)) => EndpointWire::Ipv6Bare { ip: ip.octets() },
            Endpoint::Lokinet(key) => EndpointWire::Lokinet { key: *key },
            Endpoint::Domain(name, port) => EndpointWire::Domain {
                name: name.clone(),
                port: *port,
            },
        }
    }
}

impl From<EndpointWire> for Endpoint {
    fn from(wire: EndpointWire) -> Self {
        match wire {
            EndpointWire::Ipv4Socket { ip, port } => {
                Endpoint::Socket(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(ip)), port))
            }
            EndpointWire::Ipv6Socket { ip, port } => {
                Endpoint::Socket(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(ip)), port))
            }
            EndpointWire::Ipv4Bare { ip } => Endpoint::Ip(IpAddr::V4(Ipv4Addr::from(ip))),
            EndpointWire::Ipv6Bare { ip } => Endpoint::Ip(IpAddr::V6(Ipv6Addr::from(ip))),
            EndpointWire::Lokinet { key } => Endpoint::Lokinet(key),
            EndpointWire::Domain { name, port } => Endpoint::Domain(name, port),
        }
    }
}

impl BinRead for Endpoint {
    type Args<'a> = ();

    fn read_options<R: binrw::io::Read + binrw::io::Seek>(
        reader: &mut R,
        endian: binrw::Endian,
        args: Self::Args<'_>,
    ) -> binrw::BinResult<Self> {
        let wire = EndpointWire::read_options(reader, endian, args)?;
        Ok(Endpoint::from(wire))
    }
}

impl BinWrite for Endpoint {
    type Args<'a> = ();

    fn write_options<W: binrw::io::Write + binrw::io::Seek>(
        &self,
        writer: &mut W,
        endian: binrw::Endian,
        args: Self::Args<'_>,
    ) -> binrw::BinResult<()> {
        let wire = EndpointWire::from(self);
        wire.write_options(writer, endian, args)
    }
}

impl Endpoint {
    pub fn to_bytes(&self) -> binrw::BinResult<Vec<u8>> {
        let mut cursor = Cursor::new(Vec::new());
        self.write_be(&mut cursor)?;
        Ok(cursor.into_inner())
    }

    pub fn from_bytes(bytes: &[u8]) -> binrw::BinResult<Self> {
        let mut cursor = Cursor::new(bytes);
        Self::read_be(&mut cursor)
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Endpoint::Socket(addr) => write!(f, "Socket({})", addr),
            Endpoint::Ip(ip) => write!(f, "Ip({})", ip),
            Endpoint::Lokinet(k) => write!(f, "Lokinet({:02x}{:02x}..)", k[0], k[1]),
            Endpoint::Domain(d, p) => write!(f, "Domain({}:{})", d, p),
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Endpoint::Socket(addr) => write!(f, "{}", addr),
            Endpoint::Ip(ip) => write!(f, "{}", ip),
            Endpoint::Lokinet(k) => {
                let hex: String = k.iter().map(|b| format!("{:02x}", b)).collect();
                write!(f, "lokinet:{}", hex)
            }
            Endpoint::Domain(d, p) => write!(f, "{}:{}", d, p),
        }
    }
}

