use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use binrw::{binrw, io::Cursor, BinRead, BinWrite};
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[binrw]
#[brw(big)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub binding_id: u32,
    pub peer_endpoint: Endpoint,
    #[br(temp)]
    #[bw(calc = payload.len() as u32)]
    payload_len: u32,
    #[br(count = payload_len, map = |v: Vec<u8>| Bytes::from(v))]
    #[bw(map = |b: &Bytes| b.as_ref())]
    pub payload: Bytes,
}

/// Minimal SOCKS6 Control & Data Frames powered by binrw
#[binrw]
#[brw(big)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// Client -> Server: Query available capabilities/endpoints
    #[brw(magic = 0x01u8)]
    QueryCapabilities,

    /// Server -> Client: List of supported endpoint types & external addresses
    #[brw(magic = 0x02u8)]
    Capabilities {
        #[br(temp)]
        #[bw(calc = endpoints.len() as u16)]
        count: u16,
        #[br(count = count)]
        endpoints: Vec<Endpoint>,
    },

    /// Client -> Server: Request inbound public binding for an endpoint type
    #[brw(magic = 0x03u8)]
    BindRequest {
        #[br(temp)]
        #[bw(calc = if desired.is_some() { 1u8 } else { 0u8 })]
        has_desired: u8,
        #[br(if(has_desired != 0))]
        desired: Option<Endpoint>,
    },

    /// Server -> Client: Result of bind request with allocated public endpoint
    #[brw(magic = 0x04u8)]
    BindResponse {
        binding_id: u32,
        assigned_endpoint: Endpoint,
    },

    /// Data packet envelope (bidirectional)
    #[brw(magic = 0x05u8)]
    Packet(Packet),

    /// Keep-alive / Ping
    #[brw(magic = 0x06u8)]
    Ping,

    /// Pong response
    #[brw(magic = 0x07u8)]
    Pong,
}

impl Frame {
    pub fn to_bytes(&self) -> binrw::BinResult<Vec<u8>> {
        let mut cursor = Cursor::new(Vec::new());
        self.write_be(&mut cursor)?;
        Ok(cursor.into_inner())
    }

    pub fn from_bytes(bytes: &[u8]) -> binrw::BinResult<Self> {
        let mut cursor = Cursor::new(bytes);
        Self::read_be(&mut cursor)
    }

    pub async fn write_to<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<()> {
        let bytes = self.to_bytes()?;
        let len = bytes.len() as u32;

        let mut len_buf = [0u8; 4];
        let mut cursor = Cursor::new(&mut len_buf[..]);
        len.write_be(&mut cursor)?;

        writer.write_all(&len_buf).await?;
        writer.write_all(&bytes).await?;
        writer.flush().await?;
        Ok(())
    }

    pub async fn read_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let mut cursor = Cursor::new(&len_buf[..]);
        let len: u32 = u32::read_be(&mut cursor)?;

        let mut body = vec![0u8; len as usize];
        reader.read_exact(&mut body).await?;

        let frame = Self::from_bytes(&body)?;
        Ok(frame)
    }
}
