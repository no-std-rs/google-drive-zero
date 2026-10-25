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
use core::{cell::Cell, fmt, marker::PhantomData};
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

/// Initialization metadata is small and owned. Use `body` as the streamed
/// request source, then accept the borrowed response inside an HTTP callback.
pub struct PreparedUploadStart {
    request: Request,
    metadata: alloc::vec::Vec<u8>,
    total: Option<u64>,
    file_id: Option<String>,
    remaining: Cell<u64>,
    accepted: Cell<bool>,
}
impl PreparedUploadStart {
    pub fn request(&self) -> &Request {
        &self.request
    }
    pub fn length(&self) -> u64 {
        self.metadata.len() as u64
    }
    pub fn body<'a, E: 'a>(&'a self) -> UploadBody<'a, impl Body<Error = E> + 'a> {
        UploadBody {
            source: SliceBody(&self.metadata, PhantomData),
            remaining: &self.remaining,
        }
    }
    pub async fn accept<B: Body>(
        &self,
        response: Response<B>,
    ) -> Result<UploadSession, Error<B::Error>> {
        if self.accepted.replace(true) {
            return Err(Error::InvalidResponse("upload response already accepted"));
        }
        if response.status != 200 {
            return Err(Error::Http(response.status));
        }
        if self.remaining.get() != 0 {
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
        if self
            .file_id
            .as_deref()
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
            total: self.total,
            confirmed: 0,
            sent: Some(0),
            needs_probe: false,
            finished: false,
            expired: false,
            file_id: self.file_id.clone().or(location_id),
        })
    }
}

/// A prepared write or status probe borrows the session until dropped. Preparing
/// marks its outcome uncertain: cancellation, a lost response, or dropping the
/// operation requires a probe before another write. No request is replayed.
pub struct PreparedUpload<'s> {
    request: Request,
    session: Cell<Option<&'s mut UploadSession>>,
    length: u64,
    remaining: Cell<u64>,
    accepted: Cell<bool>,
    limit: usize,
}
impl PreparedUpload<'_> {
    pub fn request(&self) -> &Request {
        &self.request
    }
    pub fn length(&self) -> u64 {
        self.length
    }
    /// Forward at most the declared length without collecting the source.
    /// Response acceptance verifies that every declared byte was consumed.
    pub fn body<B: Body>(&self, source: B) -> UploadBody<'_, B> {
        UploadBody {
            source,
            remaining: &self.remaining,
        }
    }
    pub async fn accept<B: Body>(
        &self,
        response: Response<B>,
    ) -> Result<UploadOutcome, Error<B::Error>> {
        if self.accepted.replace(true) {
            return Err(Error::InvalidResponse("upload response already accepted"));
        }
        if matches!(response.status, 200 | 201 | 308) && self.remaining.get() != 0 {
            return Err(Error::InvalidResponse(
                "request body was not fully consumed",
            ));
        }
        finish(
            response,
            self.session.take().expect("one response per operation"),
            self.limit,
        )
        .await
    }
}

/// A source window belonging to a prepared upload. This forwards reads directly
/// into the caller's buffer and records consumption for response validation.
pub struct UploadBody<'a, B> {
    source: B,
    remaining: &'a Cell<u64>,
}
impl<B: Body> Body for UploadBody<'_, B> {
    type Error = B::Error;
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        let cap = self.remaining.get().min(buffer.len() as u64) as usize;
        if cap == 0 {
            return Ok(0);
        }
        let n = self.source.read(&mut buffer[..cap]).await?;
        self.remaining.set(self.remaining.get() - n as u64);
        Ok(n)
    }
}

impl<T> Client<T> {
    pub fn prepare_create_upload(
        &self,
        file: NewFile<'_>,
        total: Option<u64>,
    ) -> Result<PreparedUploadStart, InvalidInput> {
        if file.name.is_empty() {
            return Err(InvalidInput("file name is empty"));
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
        .map_err(|_| InvalidInput("invalid upload metadata"))?;
        self.prepare_start_upload(None, file.mime_type, total, metadata)
    }
    pub fn prepare_update_upload(
        &self,
        file_id: &str,
        mime_type: &str,
        total: Option<u64>,
    ) -> Result<PreparedUploadStart, InvalidInput> {
        identifier(file_id)?;
        self.prepare_start_upload(Some(file_id), mime_type, total, b"{}".to_vec())
    }
    fn prepare_start_upload(
        &self,
        file_id: Option<&str>,
        mime_type: &str,
        total: Option<u64>,
        metadata: alloc::vec::Vec<u8>,
    ) -> Result<PreparedUploadStart, InvalidInput> {
        if mime_type.is_empty()
            || !mime_type.bytes().all(|b| (0x21..=0x7e).contains(&b))
            || !mime_type.contains('/')
            || mime_type.starts_with("application/vnd.google-apps.")
        {
            return Err(InvalidInput("invalid blob media type"));
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
        Ok(PreparedUploadStart {
            request,
            remaining: Cell::new(metadata.len() as u64),
            metadata,
            total,
            file_id: file_id.map(String::from),
            accepted: Cell::new(false),
        })
    }
    /// Prepare one exact source window. This changes the session to require a
    /// probe until a valid response is accepted, even if the operation is dropped.
    pub fn prepare_write_stream<'s>(
        &self,
        session: &'s mut UploadSession,
        length: u64,
        is_final: bool,
    ) -> Result<PreparedUpload<'s>, InvalidInput> {
        session.active()?;
        if session.needs_probe {
            return Err(InvalidInput("probe upload status before writing"));
        }
        let end = session
            .confirmed
            .checked_add(length)
            .ok_or(InvalidInput("upload size overflow"))?;
        if !is_final && (length == 0 || !length.is_multiple_of(UPLOAD_ALIGNMENT)) {
            return Err(InvalidInput("nonfinal chunk must be a multiple of 256 KiB"));
        }
        if length == 0 && (session.confirmed != 0 || !is_final) {
            return Err(InvalidInput(
                "only an empty file can have a zero length final request",
            ));
        }
        if let Some(total) = session.total {
            if end > total || (is_final && end != total) || (!is_final && end == total) {
                return Err(InvalidInput("chunk does not match the declared total"));
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
        session.needs_probe = true;
        session.sent = Some(session.sent.unwrap_or(0).max(end));
        Ok(PreparedUpload {
            request,
            session: Cell::new(Some(session)),
            length,
            remaining: Cell::new(length),
            accepted: Cell::new(false),
            limit: self.json_limit,
        })
    }
    pub fn prepare_query_upload<'s>(
        &self,
        session: &'s mut UploadSession,
    ) -> Result<PreparedUpload<'s>, InvalidInput> {
        session.active()?;
        let total = session
            .total
            .map(|n| n.to_string())
            .unwrap_or_else(|| "*".into());
        let request = self.upload_request(session, 0, format!("bytes */{total}"));
        session.needs_probe = true;
        Ok(PreparedUpload {
            request,
            session: Cell::new(Some(session)),
            length: 0,
            remaining: Cell::new(0),
            accepted: Cell::new(false),
            limit: self.json_limit,
        })
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

impl<T: UploadTransport> Client<T> {
    /// Create a blob file. Initialization allocates only the small JSON metadata.
    pub async fn create_upload(
        &mut self,
        file: NewFile<'_>,
        total: Option<u64>,
    ) -> Result<UploadSession, Error<T::Error>> {
        let operation = self.prepare_create_upload(file, total)?;
        let mut body = operation.body::<T::Error>();
        let response = self
            .transport
            .send_body(operation.request(), &mut body)
            .await
            .map_err(Error::Transport)?;
        operation.accept(response).await
    }
    /// Replace the complete content of an existing blob. Drive does not provide
    /// random in-place writes; stage edits locally before replacing the object.
    pub async fn update_upload(
        &mut self,
        file_id: &str,
        mime_type: &str,
        total: Option<u64>,
    ) -> Result<UploadSession, Error<T::Error>> {
        let operation = self.prepare_update_upload(file_id, mime_type, total)?;
        let mut body = operation.body::<T::Error>();
        let response = self
            .transport
            .send_body(operation.request(), &mut body)
            .await
            .map_err(Error::Transport)?;
        operation.accept(response).await
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
    /// Stream exactly length bytes, probing after errors or cancellation before
    /// replaying any source bytes.
    pub async fn write_stream<B: Body<Error = T::Error>>(
        &mut self,
        session: &mut UploadSession,
        source: &mut B,
        length: u64,
        is_final: bool,
    ) -> Result<UploadOutcome, Error<T::Error>> {
        let operation = self.prepare_write_stream(session, length, is_final)?;
        let mut body = operation.body(source);
        let response = self
            .transport
            .send_body(operation.request(), &mut body)
            .await
            .map_err(Error::Transport)?;
        operation.accept(response).await
    }
    /// Recover after a lost response, cancellation, restart or token renewal.
    /// No automatic retries: the caller controls backoff and source replay.
    pub async fn query_upload(
        &mut self,
        session: &mut UploadSession,
    ) -> Result<UploadOutcome, Error<T::Error>> {
        let operation = self.prepare_query_upload(session)?;
        let response = self
            .transport
            .send_body(operation.request(), &mut SliceBody(&[], PhantomData))
            .await
            .map_err(Error::Transport)?;
        operation.accept(response).await
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
