use crate::frame::ErrorCode;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("frame size {size} exceeds the negotiated limit {limit}")]
    FrameTooLarge { size: usize, limit: usize },

    #[error("server returned {code:?}: {message}")]
    Remote { code: ErrorCode, message: String },

    #[error("server connection failed during {phase:?}: {code:?}: {message}")]
    ConnectFailed {
        code: ErrorCode,
        message: String,
        phase: Option<String>,
        retry_after: Option<Duration>,
    },

    #[error("connection closed")]
    ConnectionClosed,
}

pub type Result<T> = std::result::Result<T, Error>;
