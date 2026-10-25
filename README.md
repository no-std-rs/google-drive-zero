# google-drive-zero

A `no_std + alloc`, unsafe-free Google Drive v3 client with streaming reads and
resumable writes. Supply an OAuth access token and an HTTPS transport; the core
requires no executor, operating system, browser login or TLS implementation.
MIT licensed, maintained under [no-std-rs](https://github.com/no-std-rs).

- Paginated listing and metadata, including shared drives and duplicate names.
- Streamed byte ranges from current files or retained blob revisions.
- Create new blob files or replace existing content with resumable uploads.
- Stream an entire file in one PUT, or send borrowed chunks without a file-sized
  allocation. Both paths pass transport buffers directly to the source.
- Recover using Drive's confirmed offset after a lost response, cancellation,
  token renewal or process restart. No blind write retries.

The default library is `no_std`. The optional `ureq` feature adds a blocking,
pooled Rustls HTTPS adapter for hosted applications. Firmware can implement the
same async traits using its own authenticated HTTPS stack.

## Use

Until a crates.io release is available, use the Git repository and pin a tested
commit in your application's Cargo.toml:

```toml
[dependencies]
google-drive-zero = { git = "https://github.com/no-std-rs/google-drive-zero", default-features = false }
# For a hosted application, add features = ["ureq"].
```

Streaming a range into a fixed buffer:

```rust
use google_drive_zero::{Client, ByteRange, Error, transport::Transport};

async fn read<T: Transport>(
    client: &mut Client<T>, file_id: &str, offset: u64, length: u64,
    mut consume: impl FnMut(&[u8]),
) -> Result<(), Error<T::Error>> {
    let mut stream = client.read_range(file_id, ByteRange::new(offset, length)?).await?;
    let mut buffer = [0; 64 * 1024];
    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 { break; }
        consume(&buffer[..n]);
    }
    // Publish a cached range only after this loop succeeds.
    Ok(())
}
```

Streaming a new file from a caller-owned source:

```rust
use google_drive_zero::{Client, Error, NewFile, UploadOutcome,
    transport::{Body, UploadTransport}};

async fn write<T: UploadTransport, B: Body<Error = T::Error>>(
    client: &mut Client<T>, source: &mut B, size: u64,
) -> Result<UploadOutcome, Error<T::Error>> {
    let mut session = client.create_upload(NewFile {
        name: "image.iso", mime_type: "application/octet-stream", parent: None,
    }, Some(size)).await?;
    // Persist session.session_url() securely here if restart recovery is needed.
    client.write_stream(&mut session, source, size, true).await
}
```

For chunked transfers, use `write_chunk` or `write_stream` with `is_final=false`.
Nonfinal lengths must be multiples of 256 KiB; 1–8 MiB chunks reduce request/RTT
cost where memory allows. A whole-file stream needs only one initialization and
one content request. Choose chunk size for network conditions rather than
assuming the smallest legal chunk is the fastest.

Unknown total sizes use `None` at initialization. Retain a final nonempty chunk
until EOF is known, then send it with `is_final=true` to establish the total.
A zero-length final request is reserved for an empty file.

After an error, call `query_upload` before sending more data. `offset()` reports
bytes acknowledged by Drive, which can be fewer than the bytes sent. Seek or
reload your source at that offset and resend the remainder. If Drive reports
`Incomplete`, including after a final request, keep the session and continue
recovery. `UploadSession::resume(url, total)` restores a saved session and
requires a probe before writing. Sessions can expire; HTTP 404 marks one expired.
The client never creates a replacement session automatically, avoiding duplicate
files. The caller controls retry backoff and credential refresh.

`update_upload` replaces the complete content of an existing blob; remote random
in-place writes are unavailable. A filesystem integration must stage edits.
Google Workspace document conversion/export, sync, caching and filesystem
mounts are outside this crate's scope.

## Callback-based transports

Use `Client::new((), token)` and `prepare_*` when your HTTP driver owns a TLS
connection and lends its response body only inside a callback. Preparation does
no network I/O. Each operation exposes `request()` and an `accept` method, so
both transport styles use the same request builder and response validation.
The existing `Transport`/`UploadTransport` API remains available.

For example, prepare a range with `prepare_read_range` or
`prepare_read_revision_range`. Build the HTTP request from `operation.request()`;
inside its response callback, pass the borrowed body to this helper:

```rust
use google_drive_zero::{Error, PreparedRange, transport::{Body, Response}};
use alloc::{string::String, vec::Vec};
extern crate alloc;

async fn accept_range<B: Body>(
    operation: &PreparedRange,
    status: u16,
    headers: Vec<(String, String)>,
    body: &mut B,
    mut consume: impl FnMut(&[u8]),
) -> Result<(), Error<B::Error>> {
    let mut stream = operation.accept(Response { status, headers, body })?;
    let mut buffer = [0; 64 * 1024];
    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 { break; }
        consume(&buffer[..n]);
    }
    Ok(())
}
```

`prepare_list` and `prepare_metadata` return a `PreparedJson` whose async
`accept` method decodes the same bounded metadata response. Header duplicates,
status codes, range clipping, declared lengths, encoding and exact EOF have the
same rules for borrowed and owned bodies. Keep the HTTP connection alive while
the callback runs; its driver must close/invalidate it when the callback fails
or is cancelled. Never publish partial bytes after a failed or cancelled read.

For uploads, `prepare_create_upload` and `prepare_update_upload` return a
`PreparedUploadStart`. Stream its `body::<YourTransportError>()` as the request
source, using `length()` as Content-Length, then call `accept` in the response
callback to obtain an `UploadSession`. A prepared write comes from
`prepare_write_stream(&mut session, length, is_final)`; stream its `body(source)`
wrapper and call `accept` for the `UploadOutcome`. The wrapper forwards reads
without a payload allocation and records consumption, so a successful HTTP
response cannot hide an incomplete source. One prepared upload represents one
attempt; do not replay it or call its response acceptor twice.

Preparing a write or `prepare_query_upload` borrows the session and immediately
marks it uncertain. After dropping a cancelled or unused write, use a new
`prepare_query_upload` operation before replaying bytes. Only its validated
acknowledgement clears probe-required state and advances the confirmed offset.
This also applies when cancellation occurs while reading a response body.

## Transport and credentials

Implement `transport::Body` and `Transport`; add `UploadTransport` for writes.
Payloads flow incrementally through borrowed buffers. Upload adapters must not
collect request bodies, retry them transparently, or follow redirects, including
HTTP 308, which is an upload acknowledgement. Response bodies must preserve byte
content and duplicate headers. Dropping an unread body must invalidate/drain its
connection before reuse. Authenticate TLS for `www.googleapis.com` and disable
transparent decompression. The client confines upload Locations to that HTTPS
origin and the Drive upload endpoint.

`UreqTransport` uses blocking I/O inside async methods. Run it on a dedicated
worker thread. Streaming sources must complete there or wake it independently;
a source that needs progress on the blocked reactor thread will stall. Core
users can supply a fully asynchronous transport instead.

Owned tokens and session URLs are zeroized on drop and redacted from Debug.
Treat `session_url()` as a credential and persist it securely. On HTTP 401, get a
fresh token from your credential service and call `set_access_token`. The core
does not perform OAuth or refresh tokens. The caller's token copies and its
transport's header buffers remain the caller/transport's responsibility.

Metadata JSON has a configurable 1 MiB default response limit. Range reads
validate 206 status, exact Content-Range, encoding, length and EOF. An error or
cancelled read invalidates the stream; discard partial cached data. Latest-file
ranges can observe changes between requests. Stable multi-request reads require
a blob revision retained as Keep Forever, arranged by the caller.

## Hosted examples

The examples read a supplied token from `DRIVE_ACCESS_TOKEN`. Listing is read-only;
the upload example creates a new Drive file from the specified local file.

```sh
cargo run --locked --features ureq --example list
cargo run --locked --features ureq --example upload -- /path/to/image.iso
```

## Validation and performance

```sh
cargo fmt --check
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo test --locked
cargo test --locked --all-features
rustup target add aarch64-unknown-none x86_64-unknown-uefi
cargo check --locked --lib --target aarch64-unknown-none
cargo check --locked --lib --target x86_64-unknown-uefi
cargo test --release --locked --features ureq --lib loopback_throughput -- --ignored --nocapture
```

The offline suite covers exact binary bytes, bounded streaming sources, partial
acknowledgements, malformed responses, cancellation and recovery. Local TCP tests
exercise the hosted adapter in both directions. The manual benchmark compares
the client with direct `ureq` using the same buffers and validates every payload
byte. It measures local HTTP wrapper overhead, excluding TLS, Internet latency,
Drive quotas and server throughput. See [BENCHMARKS.md](BENCHMARKS.md). Live Drive
throughput is unmeasured; these results are not a cloud performance guarantee.

## Existing crates considered

Published manifests and sources were checked on 2026-10-23:

| Crate | Version inspected | `no_std` constraint |
| --- | --- | --- |
| [google-drive3](https://docs.rs/google-drive3/) | 7.0.0+20251218 | Mandatory Hyper, Tokio and standard-library API |
| [google-drive](https://docs.rs/google-drive/) | 0.10.0 | Mandatory Reqwest, Tokio and standard-library API |
| [drive-v3](https://docs.rs/drive-v3/) | 0.6.1 | Blocking Reqwest and filesystem/browser OAuth |
| [opendal-service-gdrive](https://docs.rs/opendal-service-gdrive/) | 0.59.5 | opendal-core explicitly enables std in dependencies |

OpenDAL remains a useful hosted option for Drive, Dropbox, S3 and Google Cloud
Storage. This crate implements the portable Drive protocol core directly.

API contracts: [files.list](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/list),
[files.get](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/get),
[downloads and revision retention](https://developers.google.com/workspace/drive/api/guides/manage-downloads),
[resumable uploads and recovery](https://developers.google.com/workspace/drive/api/guides/manage-uploads),
[files.create](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/create),
[files.update](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/update).
