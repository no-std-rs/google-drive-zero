use crate::{
    client::{decode_json, encode, identifier, FIELDS},
    transport::{Body, Method, Request, Response, UploadTransport},
    Client, Error, File, InvalidInput,
};
use alloc::{
    format,
    string::{String, ToString},
    vec,
};
use core::{fmt, marker::PhantomData};
use serde::Serialize;
use zeroize::Zeroizing;

const UPLOAD: &str = "https://www.googleapis.com/upload/drive/v3/files";
/// Drive requires every nonfinal request to contain a multiple of 256 KiB.
pub const UPLOAD_ALIGNMENT: u64 = 256 * 1024;

pub struct NewFile<'a> {
    pub name: &'a str,
    pub mime_type: &'a str,
    pub parent: Option<&'a str>,
}

/// A resumable session URL is a secret capability. Persist it securely when
/// needed; Debug redacts it and drop zeroizes its owned storage.
pub struct UploadSession {
    url: Zeroizing<String>,
    total: Option<u64>,
    confirmed: u64,
    sent: Option<u64>,
    needs_probe: bool,
    finished: bool,
    expired: bool,
    file_id: Option<String>,
}
impl fmt::Debug for UploadSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadSession")
            .field("url", &"[redacted]")
            .field("total", &self.total)
            .field("confirmed", &self.confirmed)
            .field("needs_probe", &self.needs_probe)
            .field("finished", &self.finished)
            .field("expired", &self.expired)
            .finish()
    }
}
impl UploadSession {
    /// Restore after a restart. Probe the server before providing more bytes.
    /// Pass the same total size used when the session was created, if known.
    pub fn resume(url: impl Into<String>, total: Option<u64>) -> Result<Self, InvalidInput> {
        let url = Zeroizing::new(url.into());
        let file_id = validate_url(&url)?;
        Ok(Self {
            url,
            total,
            confirmed: 0,
            sent: None,
            needs_probe: true,
            finished: false,
            expired: false,
            file_id,
        })
    }
    pub fn session_url(&self) -> &str {
        &self.url
    }
    /// Number of bytes confirmed by Drive, never the number merely sent.
    pub fn offset(&self) -> u64 {
        self.confirmed
    }
    pub fn total_size(&self) -> Option<u64> {
        self.total
    }
    pub fn needs_probe(&self) -> bool {
        self.needs_probe
    }
    pub fn is_complete(&self) -> bool {
        self.finished
    }
    pub fn is_expired(&self) -> bool {
        self.expired
    }
    fn active(&self) -> Result<(), InvalidInput> {
        if self.finished {
            return Err(InvalidInput("upload already complete"));
        }
        if self.expired {
            return Err(InvalidInput("upload session expired"));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum UploadOutcome {
    Incomplete { confirmed: u64 },
    Complete(File),
}

impl<T: UploadTransport> Client<T> {
    /// Create a blob file. Initialization allocates only the small JSON metadata.
    pub async fn create_upload(
        &mut self,
        file: NewFile<'_>,
        total: Option<u64>,
    ) -> Result<UploadSession, Error<T::Error>> {
        if file.name.is_empty() {
            return Err(InvalidInput("file name is empty").into());
        }
        if let Some(parent) = file.parent {
            identifier(parent)?;
        }
        #[derive(Serialize)]
        struct Metadata<'a> {
            name: &'a str,
            #[serde(rename = "mimeType")]
            mime_type: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            parents: Option<[&'a str; 1]>,
        }
        let metadata = serde_json::to_vec(&Metadata {
            name: file.name,
            mime_type: file.mime_type,
            parents: file.parent.map(|p| [p]),
        })
        .map_err(|_| Error::Json)?;
        self.start_upload(None, file.mime_type, total, &metadata)
            .await
    }
    /// Replace the complete content of an existing blob. Drive does not provide
    /// random in-place writes; stage edits locally before replacing the object.
    pub async fn update_upload(
        &mut self,
        file_id: &str,
        mime_type: &str,
        total: Option<u64>,
    ) -> Result<UploadSession, Error<T::Error>> {
        identifier(file_id)?;
        self.start_upload(Some(file_id), mime_type, total, b"{}")
            .await
    }
    async fn start_upload(
        &mut self,
        file_id: Option<&str>,
        mime_type: &str,
        total: Option<u64>,
        metadata: &[u8],
    ) -> Result<UploadSession, Error<T::Error>> {
        // Header-safe media type. Google Workspace conversion is outside this API.
        if mime_type.is_empty()
            || !mime_type.bytes().all(|b| (0x21..=0x7e).contains(&b))
            || !mime_type.contains('/')
            || mime_type.starts_with("application/vnd.google-apps.")
        {
            return Err(InvalidInput("invalid blob media type").into());
        }
        let path = file_id.map(|id| format!("/{id}")).unwrap_or_default();
        let mut request = self.request(
            format!(
                "{UPLOAD}{path}?uploadType=resumable&supportsAllDrives=true&fields={}",
                encode(FIELDS)
            ),
            None,
        );
        request.method = if file_id.is_some() {
            Method::Patch
        } else {
            Method::Post
        };
        request.extra = vec![
            ("Content-Type", "application/json; charset=UTF-8".into()),
            ("Content-Length", metadata.len().to_string()),
            ("X-Upload-Content-Type", mime_type.into()),
        ];
        if let Some(total) = total {
            request
                .extra
                .push(("X-Upload-Content-Length", total.to_string()));
        }
        let mut body = SliceBody::<T::Error>(metadata, PhantomData);
        let response = self
            .transport
            .send_body(&request, &mut body)
            .await
            .map_err(Error::Transport)?;
        if response.status != 200 {
            return Err(Error::Http(response.status));
        }
        if !body.0.is_empty() {
            return Err(Error::InvalidResponse("transport did not send metadata"));
        }
        let location = Zeroizing::new(
            response
                .header("location")
                .map_err(Error::InvalidResponse)?
                .ok_or(Error::InvalidResponse("missing upload Location"))?
                .to_string(),
        );
        let location_id = validate_url(&location)
            .map_err(|_| Error::InvalidResponse("invalid upload Location"))?;
        if file_id
            .zip(location_id.as_deref())
            .is_some_and(|(a, b)| a != b)
        {
            return Err(Error::InvalidResponse(
                "upload Location file identity mismatch",
            ));
        }
        empty(response).await?;
        Ok(UploadSession {
            url: location,
            total,
            confirmed: 0,
            sent: Some(0),
            needs_probe: false,
            finished: false,
            expired: false,
            file_id: file_id.map(String::from).or(location_id),
        })
    }

    /// Send a borrowed chunk without copying it. Keep/reload bytes starting at
    /// the confirmed offset if Drive acknowledges only part of the request.
    pub async fn write_chunk(
        &mut self,
        session: &mut UploadSession,
        bytes: &[u8],
        is_final: bool,
    ) -> Result<UploadOutcome, Error<T::Error>> {
        self.write_stream(
            session,
            &mut SliceBody(bytes, PhantomData),
            bytes.len() as u64,
            is_final,
        )
        .await
    }

    /// Stream exactly `length` bytes from a source positioned at session.offset().
    /// A single final request can stream a whole file with bounded memory and
    /// minimal HTTP overhead. A nonfinal request must be a multiple of 256 KiB.
    /// On errors or cancellation, probe before seeking/replaying the source.
    pub async fn write_stream<B: Body<Error = T::Error>>(
        &mut self,
        session: &mut UploadSession,
        source: &mut B,
        length: u64,
        is_final: bool,
    ) -> Result<UploadOutcome, Error<T::Error>> {
        session.active()?;
        if session.needs_probe {
            return Err(InvalidInput("probe upload status before writing").into());
        }
        let end = session
            .confirmed
            .checked_add(length)
            .ok_or(InvalidInput("upload size overflow"))?;
        if !is_final && (length == 0 || !length.is_multiple_of(UPLOAD_ALIGNMENT)) {
            return Err(InvalidInput("nonfinal chunk must be a multiple of 256 KiB").into());
        }
        if length == 0 && (session.confirmed != 0 || !is_final) {
            return Err(
                InvalidInput("only an empty file can have a zero length final request").into(),
            );
        }
        if let Some(total) = session.total {
            if end > total || (is_final && end != total) || (!is_final && end == total) {
                return Err(InvalidInput("chunk does not match the declared total").into());
            }
        }
        if is_final {
            session.total = Some(end);
        }
        let total = session
            .total
            .map(|n| n.to_string())
            .unwrap_or_else(|| "*".into());
        let range = if length == 0 {
            format!("bytes */{total}")
        } else {
            format!("bytes {}-{}/{total}", session.confirmed, end - 1)
        };
        let request = self.upload_request(session, length, range);
        let mut limited = Limited {
            source,
            remaining: length,
        };
        // Set before awaiting: dropping the future leaves an uncertain session.
        session.needs_probe = true;
        session.sent = Some(session.sent.unwrap_or(0).max(end));
        let response = self
            .transport
            .send_body(&request, &mut limited)
            .await
            .map_err(Error::Transport)?;
        if matches!(response.status, 200 | 201 | 308) && limited.remaining != 0 {
            return Err(Error::InvalidResponse(
                "request body was not fully consumed",
            ));
        }
        finish(response, session, self.json_limit).await
    }

    /// Recover after a lost response, cancellation, restart or token renewal.
    /// No automatic retries: the caller controls backoff and source replay.
    pub async fn query_upload(
        &mut self,
        session: &mut UploadSession,
    ) -> Result<UploadOutcome, Error<T::Error>> {
        session.active()?;
        let total = session
            .total
            .map(|n| n.to_string())
            .unwrap_or_else(|| "*".into());
        let request = self.upload_request(session, 0, format!("bytes */{total}"));
        session.needs_probe = true;
        let response = self
            .transport
            .send_body(&request, &mut SliceBody(&[], PhantomData))
            .await
            .map_err(Error::Transport)?;
        finish(response, session, self.json_limit).await
    }
    fn upload_request(&self, session: &UploadSession, length: u64, range: String) -> Request {
        let mut request = self.request(session.url.to_string(), None);
        request.method = Method::Put;
        request.sensitive_url = true;
        request.extra = vec![
            ("Content-Length", length.to_string()),
            ("Content-Range", range),
        ];
        request
    }
}

async fn finish<B: Body>(
    response: Response<B>,
    session: &mut UploadSession,
    limit: usize,
) -> Result<UploadOutcome, Error<B::Error>> {
    match response.status {
        308 => {
            let confirmed = match response.header("range").map_err(Error::InvalidResponse)? {
                None => 0,
                Some(value) => value
                    .strip_prefix("bytes=0-")
                    .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
                    .and_then(|v| v.parse::<u64>().ok())
                    .and_then(|n| n.checked_add(1))
                    .ok_or(Error::InvalidResponse("invalid upload Range"))?,
            };
            if confirmed < session.confirmed
                || session.sent.is_some_and(|n| confirmed > n)
                || session.total.is_some_and(|n| confirmed > n)
            {
                return Err(Error::InvalidResponse(
                    "upload acknowledgement out of bounds",
                ));
            }
            empty(response).await?;
            session.confirmed = confirmed;
            session.sent = Some(session.sent.unwrap_or(confirmed));
            session.needs_probe = session.total == Some(confirmed);
            Ok(UploadOutcome::Incomplete { confirmed })
        }
        200 | 201 => {
            let file: File = decode_json(response, limit).await?;
            let size = file
                .size
                .ok_or(Error::InvalidResponse("missing uploaded file size"))?;
            if (session.total.is_none() && session.sent.is_some())
                || session.total.is_some_and(|n| n != size)
                || size < session.confirmed
                || session.sent.is_some_and(|n| n < size)
                || session.file_id.as_ref().is_some_and(|id| id != &file.id)
                || identifier(&file.id).is_err()
            {
                return Err(Error::InvalidResponse(
                    "uploaded file identity or size mismatch",
                ));
            }
            session.total = Some(size);
            session.confirmed = size;
            session.finished = true;
            session.needs_probe = false;
            Ok(UploadOutcome::Complete(file))
        }
        404 => {
            session.expired = true;
            Err(Error::Http(404))
        }
        status => Err(Error::Http(status)),
    }
}

async fn empty<B: Body>(mut response: Response<B>) -> Result<(), Error<B::Error>> {
    if response
        .header("content-length")
        .map_err(Error::InvalidResponse)?
        .is_some_and(|v| v != "0")
        || response
            .header("content-encoding")
            .map_err(Error::InvalidResponse)?
            .is_some_and(|v| !v.eq_ignore_ascii_case("identity"))
    {
        return Err(Error::InvalidResponse("expected empty upload response"));
    }
    let mut byte = [0];
    if response
        .body
        .read(&mut byte)
        .await
        .map_err(Error::Transport)?
        != 0
    {
        return Err(Error::InvalidResponse("expected empty upload response"));
    }
    Ok(())
}

fn validate_url(url: &str) -> Result<Option<String>, InvalidInput> {
    let bad = InvalidInput("invalid Google upload session URL");
    let rest = url.strip_prefix(UPLOAD).ok_or(bad)?;
    let (path, query) = rest.split_once('?').ok_or(bad)?;
    let file_id = if path.is_empty() {
        None
    } else {
        let id = path.strip_prefix('/').ok_or(bad)?;
        identifier(id).map_err(|_| bad)?;
        Some(id.into())
    };
    if !query
        .bytes()
        .all(|b| (0x21..=0x7e).contains(&b) && !b"#\\<>\"`{}|^".contains(&b))
    {
        return Err(bad);
    }
    let mut upload_type = None;
    let mut upload_id = None;
    for parameter in query.split('&') {
        let (key, value) = parameter.split_once('=').ok_or(bad)?;
        match key {
            "uploadType" if upload_type.replace(value).is_some() => return Err(bad),
            "upload_id" if upload_id.replace(value).is_some() => return Err(bad),
            _ => (),
        }
    }
    if upload_type != Some("resumable") || upload_id.is_none_or(str::is_empty) {
        return Err(bad);
    }
    Ok(file_id)
}

struct SliceBody<'a, E>(&'a [u8], PhantomData<E>);
impl<E> Body for SliceBody<'_, E> {
    type Error = E;
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, E> {
        let n = self.0.len().min(buffer.len());
        buffer[..n].copy_from_slice(&self.0[..n]);
        self.0 = &self.0[n..];
        Ok(n)
    }
}
struct Limited<'a, B> {
    source: &'a mut B,
    remaining: u64,
}
impl<B: Body> Body for Limited<'_, B> {
    type Error = B::Error;
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        let cap = self.remaining.min(buffer.len() as u64) as usize;
        if cap == 0 {
            return Ok(0);
        }
        let n = self.source.read(&mut buffer[..cap]).await?;
        self.remaining -= n as u64;
        Ok(n)
    }
}
