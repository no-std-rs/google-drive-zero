#![no_std]
#![forbid(unsafe_code)]
#![allow(async_fn_in_trait)]
#![doc = include_str!("../README.md")]

extern crate alloc;
#[cfg(feature = "ureq")]
extern crate std;

#[cfg(feature = "ureq")]
pub mod host;

mod client;
mod models;
mod stream;
pub mod transport;
mod upload;

pub use client::{Client, ListOptions};
pub use models::{Capabilities, File, FilePage};
pub use stream::{ByteRange, Download};
pub use upload::{NewFile, UploadOutcome, UploadSession, UPLOAD_ALIGNMENT};

use core::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidInput(pub &'static str);
impl fmt::Display for InvalidInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl core::error::Error for InvalidInput {}

/// HTTP errors preserve the status (including 401, 403, 429 and 416), without
/// retaining server response bodies or automatically retrying requests.
#[derive(Debug, PartialEq, Eq)]
pub enum Error<E> {
    Input(InvalidInput),
    Transport(E),
    Http(u16),
    InvalidResponse(&'static str),
    Json,
    BodyTooLarge,
}
impl<E> From<InvalidInput> for Error<E> {
    fn from(error: InvalidInput) -> Self {
        Self::Input(error)
    }
}
impl<E> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input(error) => error.fmt(f),
            Self::Transport(_) => f.write_str("Drive HTTPS transport failed"),
            Self::Http(status) => write!(f, "Drive returned HTTP {status}"),
            Self::InvalidResponse(reason) => write!(f, "invalid Drive response: {reason}"),
            Self::Json => f.write_str("invalid Drive JSON response"),
            Self::BodyTooLarge => {
                f.write_str("Drive metadata response exceeds the configured limit")
            }
        }
    }
}
impl<E: fmt::Debug> core::error::Error for Error<E> {}
