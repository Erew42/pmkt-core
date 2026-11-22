//! Lossless public WebSocket books and verified, offline raw-capture reconstruction.
use serde::Deserialize;

pub mod book;
pub mod capture;
pub mod decimal;
pub mod emit;
pub mod feed;
pub mod health;
pub mod merge;
pub mod wire;

pub use book::{Book, BookMode, VenueBooks};
pub use decimal::ScaledDecimal;
pub use feed::ShardFeed;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Venue {
    Polymarket,
    Kalshi,
}

impl Venue {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Polymarket => "polymarket",
            Self::Kalshi => "kalshi",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),
}

pub type Result<T> = std::result::Result<T, Error>;
