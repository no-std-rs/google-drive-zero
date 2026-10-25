//! Transport contract: authenticate TLS for www.googleapis.com, preserve body
//! bytes without decompression, and never forward credentials across redirects.
//! The client treats every redirect as an HTTP error. Send exactly one request;
//! timeouts, scheduling, connection pooling and cancellation belong to the host.
use alloc::{string::String, vec::Vec};
use core::fmt;
use zeroize::Zeroizing;

/// Dropping before EOF must close or invalidate the connection, or otherwise
/// ensure unread response bytes cannot become the next response on reuse.
pub trait Body {
    type Error;
    /// Read at most buffer.len() bytes; zero means EOF. Empty buffers are a no-op.
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error>;
}

/// Borrow a response/source body inside a transport callback without collecting
/// it or transferring ownership out of the callback.
impl<B: Body + ?Sized> Body for &mut B {
    type Error = B::Error;
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        B::read(self, buffer).await
    }
}

pub trait Transport {
    type Error;
    type Body<'a>: Body<Error = Self::Error>
    where
        Self: 'a;

    async fn send<'a>(
        &'a mut self,
        request: &Request,
    ) -> Result<Response<Self::Body<'a>>, Self::Error>;
}

/// Consume request bodies incrementally, without collecting them in memory.
/// Send exactly Content-Length and propagate source errors. Never replay a body
/// or follow redirects (including 308).
pub trait UploadTransport: Transport {
    async fn send_body<'a, B: Body<Error = Self::Error>>(
        &'a mut self,
        request: &Request,
        body: &mut B,
    ) -> Result<Response<Self::Body<'a>>, Self::Error>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Patch,
    Put,
}
impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Patch => "PATCH",
            Self::Put => "PUT",
        }
    }
}

/// Debug omits credentials and capability-bearing upload session URLs.
pub struct Request {
    pub(crate) url: Zeroizing<String>,
    pub(crate) authorization: Zeroizing<String>,
    pub(crate) range: Option<String>,
    pub(crate) accept: &'static str,
    pub(crate) method: Method,
    pub(crate) extra: Vec<(&'static str, String)>,
    pub(crate) sensitive_url: bool,
}
impl Request {
    pub fn url(&self) -> &str {
        &self.url
    }
    pub fn method(&self) -> Method {
        self.method
    }
    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        [
            ("Authorization", Some(self.authorization.as_str())),
            ("Accept", Some(self.accept)),
            ("Accept-Encoding", Some("identity")),
            ("Range", self.range.as_deref()),
        ]
        .into_iter()
        .filter_map(|(name, value)| value.map(|value| (name, value)))
        .chain(self.extra.iter().map(|(k, v)| (*k, v.as_str())))
    }
}
impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
            .field("method", &self.method)
            .field(
                "url",
                &if self.sensitive_url {
                    "[redacted]"
                } else {
                    self.url.as_str()
                },
            )
            .field("authorization", &"[redacted]")
            .field("range", &self.range)
            .finish()
    }
}

pub struct Response<B> {
    pub status: u16,
    /// Preserve duplicate headers so the client can reject ambiguous lengths.
    pub headers: Vec<(String, String)>,
    pub body: B,
}
impl<B> Response<B> {
    pub(crate) fn header(&self, name: &str) -> Result<Option<&str>, &'static str> {
        let mut values = self
            .headers
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case(name));
        let value = values.next().map(|(_, value)| value.as_str().trim());
        if values.next().is_some() {
            return Err("duplicate response header");
        }
        Ok(value)
    }
}
