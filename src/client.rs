use crate::{
    transport::{Body, Method, Request, Response, Transport},
    ByteRange, Download, Error, File, FilePage, InvalidInput,
};
use alloc::{format, string::String, vec::Vec};
use core::fmt;
use serde::de::DeserializeOwned;
use zeroize::Zeroizing;

const ORIGIN: &str = "https://www.googleapis.com/drive/v3";
pub(crate) const FIELDS: &str = "id,name,mimeType,size,version,parents,modifiedTime,md5Checksum,headRevisionId,capabilities(canDownload)";

pub struct ListOptions<'a> {
    pub query: Option<&'a str>,
    pub page_token: Option<&'a str>,
    pub page_size: u16,
    /// Set to search a shared drive rather than the user's corpus.
    pub drive_id: Option<&'a str>,
}
impl Default for ListOptions<'_> {
    fn default() -> Self {
        Self {
            query: None,
            page_token: None,
            page_size: 100,
            drive_id: None,
        }
    }
}

pub struct Client<T> {
    pub(crate) transport: T,
    token: Zeroizing<String>,
    pub(crate) json_limit: usize,
}
impl<T> fmt::Debug for Client<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("access_token", &"[redacted]")
            .field("json_limit", &self.json_limit)
            .finish()
    }
}
impl<T> Client<T> {
    pub fn new(transport: T, access_token: impl Into<String>) -> Result<Self, InvalidInput> {
        Ok(Self {
            transport,
            token: token(access_token.into())?,
            json_limit: 1024 * 1024,
        })
    }
    /// Replace an expired token with one supplied by the credential service.
    pub fn set_access_token(
        &mut self,
        access_token: impl Into<String>,
    ) -> Result<(), InvalidInput> {
        self.token = token(access_token.into())?;
        Ok(())
    }
    pub fn set_json_limit(&mut self, bytes: usize) -> Result<(), InvalidInput> {
        if bytes == 0 {
            return Err(InvalidInput("metadata limit must be nonzero"));
        }
        self.json_limit = bytes;
        Ok(())
    }
    pub fn into_transport(self) -> T {
        self.transport
    }
    pub(crate) fn request(&self, url: String, range: Option<ByteRange>) -> Request {
        Request {
            url: Zeroizing::new(url),
            method: Method::Get,
            extra: Vec::new(),
            sensitive_url: false,
            authorization: Zeroizing::new(format!("Bearer {}", self.token.as_str())),
            range: range.map(|r| format!("bytes={}-{}", r.start, r.end)),
            accept: if range.is_some() {
                "application/octet-stream"
            } else {
                "application/json"
            },
        }
    }
}
impl<T: Transport> Client<T> {
    pub async fn list(&mut self, options: &ListOptions<'_>) -> Result<FilePage, Error<T::Error>> {
        if !(1..=1000).contains(&options.page_size) {
            return Err(InvalidInput("page size must be 1..1000").into());
        }
        let mut url = format!("{ORIGIN}/files?supportsAllDrives=true&includeItemsFromAllDrives=true&pageSize={}&fields={}", options.page_size, encode(&format!("nextPageToken,incompleteSearch,files({FIELDS})")));
        parameter(&mut url, "q", options.query.unwrap_or("trashed = false"));
        if let Some(value) = options.page_token {
            parameter(&mut url, "pageToken", value);
        }
        if let Some(value) = options.drive_id {
            identifier(value)?;
            parameter(&mut url, "corpora", "drive");
            parameter(&mut url, "driveId", value);
        }
        let request = self.request(url, None);
        let response = self
            .transport
            .send(&request)
            .await
            .map_err(Error::Transport)?;
        json(response, self.json_limit).await
    }
    pub async fn metadata(&mut self, file_id: &str) -> Result<File, Error<T::Error>> {
        identifier(file_id)?;
        let request = self.request(
            format!(
                "{ORIGIN}/files/{}?supportsAllDrives=true&fields={}",
                encode(file_id),
                encode(FIELDS)
            ),
            None,
        );
        let response = self
            .transport
            .send(&request)
            .await
            .map_err(Error::Transport)?;
        json(response, self.json_limit).await
    }
    /// Reads the current file content; separate requests can observe changes.
    /// Use a retained revision for a stable multi-request export.
    pub async fn read_range(
        &mut self,
        file_id: &str,
        range: ByteRange,
    ) -> Result<Download<T::Body<'_>>, Error<T::Error>> {
        self.media(file_id, None, range).await
    }
    /// Google permits revision downloads only for blob revisions marked Keep
    /// Forever. The caller must arrange retention; this method does
    /// not mutate retention or silently fall back to the latest file.
    pub async fn read_revision_range(
        &mut self,
        file_id: &str,
        revision_id: &str,
        range: ByteRange,
    ) -> Result<Download<T::Body<'_>>, Error<T::Error>> {
        self.media(file_id, Some(revision_id), range).await
    }
    async fn media(
        &mut self,
        file_id: &str,
        revision_id: Option<&str>,
        range: ByteRange,
    ) -> Result<Download<T::Body<'_>>, Error<T::Error>> {
        identifier(file_id)?;
        let mut url = format!("{ORIGIN}/files/{}", encode(file_id));
        if let Some(revision) = revision_id {
            identifier(revision)?;
            url.push_str(&format!("/revisions/{}?alt=media", encode(revision)));
        } else {
            url.push_str("?alt=media&supportsAllDrives=true");
        }
        let request = self.request(url, Some(range));
        let response = self
            .transport
            .send(&request)
            .await
            .map_err(Error::Transport)?;
        Download::new(response, range)
    }
}

fn token(value: String) -> Result<Zeroizing<String>, InvalidInput> {
    let value = Zeroizing::new(value);
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-._~+/=".contains(&b))
    {
        return Err(InvalidInput("invalid bearer token encoding"));
    }
    Ok(value)
}
pub(crate) fn identifier(value: &str) -> Result<(), InvalidInput> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(InvalidInput("invalid Drive identifier"));
    }
    Ok(())
}
pub(crate) fn encode(value: &str) -> String {
    const HEX: &[u8] = b"0123456789ABCDEF";
    let mut out = String::new();
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 15) as usize] as char);
        }
    }
    out
}
fn parameter(url: &mut String, name: &str, value: &str) {
    url.push('&');
    url.push_str(name);
    url.push('=');
    url.push_str(&encode(value));
}
async fn json<V: DeserializeOwned, B: Body>(
    response: Response<B>,
    limit: usize,
) -> Result<V, Error<B::Error>> {
    if response.status != 200 {
        return Err(Error::Http(response.status));
    }
    decode_json(response, limit).await
}

pub(crate) async fn decode_json<V: DeserializeOwned, B: Body>(
    mut response: Response<B>,
    limit: usize,
) -> Result<V, Error<B::Error>> {
    let declared_length = if let Some(value) = response
        .header("content-length")
        .map_err(Error::InvalidResponse)?
    {
        let length: u64 = value
            .parse()
            .map_err(|_| Error::InvalidResponse("invalid Content-Length"))?;
        if length > limit as u64 {
            return Err(Error::BodyTooLarge);
        }
        Some(length)
    } else {
        None
    };
    if response
        .header("content-encoding")
        .map_err(Error::InvalidResponse)?
        .is_some_and(|v| !v.eq_ignore_ascii_case("identity"))
    {
        return Err(Error::InvalidResponse("encoded metadata body"));
    }
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        let n = response
            .body
            .read(&mut buffer)
            .await
            .map_err(Error::Transport)?;
        if n == 0 {
            break;
        }
        if n > limit.saturating_sub(bytes.len()) {
            return Err(Error::BodyTooLarge);
        }
        bytes.extend_from_slice(&buffer[..n]);
    }
    if declared_length.is_some_and(|length| length != bytes.len() as u64) {
        return Err(Error::InvalidResponse("metadata Content-Length mismatch"));
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::Json)
}
