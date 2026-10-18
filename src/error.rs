use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Binrw error: {0}")]
    Binrw(#[from] binrw::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Buffer too short")]
    BufferTooShort,

    #[error("Invalid endpoint type: {0}")]
    InvalidEndpointType(u8),

    #[error("Invalid frame opcode: {0}")]
    InvalidOpcode(u8),

    #[error("Unsupported endpoint: {0:?}")]
    UnsupportedEndpoint(String),

    #[error("Connection closed")]
    ConnectionClosed,
}

pub type Result<T> = std::result::Result<T, Error>;
