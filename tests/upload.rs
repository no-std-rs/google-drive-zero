use futures_executor::block_on;
use google_drive_zero::{
    transport::{Body, Method, Request, Response, Transport, UploadTransport},
    Client, Error, NewFile, UploadOutcome, UploadSession, UPLOAD_ALIGNMENT,
};
use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    rc::Rc,
    task::{Context, Poll, Waker},
};

const URL: &str = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=secret-session";
#[derive(Debug, PartialEq, Eq)]
struct Failure;
struct Bytes(VecDeque<u8>);
impl Body for Bytes {
    type Error = Failure;
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Failure> {
        let n = buffer.len().min(self.0.len()).min(17);
        for byte in &mut buffer[..n] {
            *byte = self.0.pop_front().unwrap();
        }
        Ok(n)
    }
}
#[derive(Debug)]
struct Sent {
    method: Method,
    url: String,
    headers: Vec<(String, String)>,
    count: u64,
    prefix: Vec<u8>,
    hash: u64,
    debug: String,
}
impl Sent {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}
type Audit = Rc<RefCell<Vec<Sent>>>;
struct Mock {
    replies: VecDeque<Result<Response<Bytes>, Failure>>,
    audit: Audit,
    early: bool,
}
impl Transport for Mock {
    type Error = Failure;
    type Body<'a> = Bytes;
    async fn send(&mut self, _: &Request) -> Result<Response<Bytes>, Failure> {
        panic!("unexpected GET")
    }
}
impl UploadTransport for Mock {
    async fn send_body<B: Body<Error = Failure>>(
        &mut self,
        request: &Request,
        body: &mut B,
    ) -> Result<Response<Bytes>, Failure> {
        let mut sent = Sent {
            method: request.method(),
            url: request.url().into(),
            headers: request
                .headers()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            count: 0,
            prefix: Vec::new(),
            hash: 0,
            debug: format!("{request:?}"),
        };
        let mut buffer = [0; 8192];
        if !self.early {
            loop {
                let n = body.read(&mut buffer).await?;
                if n == 0 {
                    break;
                }
                for b in &buffer[..n] {
                    sent.hash = sent.hash.wrapping_mul(31).wrapping_add(*b as u64);
                }
                sent.count += n as u64;
                let keep = n.min(512 - sent.prefix.len());
                sent.prefix.extend_from_slice(&buffer[..keep]);
            }
        }
        self.audit.borrow_mut().push(sent);
        self.replies.pop_front().expect("unexpected request")
    }
}
fn reply(status: u16, headers: &[(&str, &str)], bytes: &[u8]) -> Response<Bytes> {
    Response {
        status,
        headers: headers
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect(),
        body: Bytes(bytes.iter().copied().collect()),
    }
}
fn init() -> Response<Bytes> {
    reply(200, &[("Location", URL)], b"")
}
fn ack(n: u64) -> Response<Bytes> {
    reply(308, &[("Range", &format!("bytes=0-{}", n - 1))], b"")
}
fn complete(size: u64) -> Response<Bytes> {
    reply(201, &[], format!(r#"{{"id":"file-1","name":"test","mimeType":"application/octet-stream","size":"{size}"}}"#).as_bytes())
}
fn client(
    replies: impl IntoIterator<Item = Result<Response<Bytes>, Failure>>,
) -> (Client<Mock>, Audit) {
    let audit = Audit::default();
    (
        Client::new(
            Mock {
                replies: replies.into_iter().collect(),
                audit: audit.clone(),
                early: false,
            },
            "test-token",
        )
        .unwrap(),
        audit,
    )
}
async fn create(drive: &mut Client<Mock>, total: Option<u64>) -> UploadSession {
    drive
        .create_upload(
            NewFile {
                name: "x\" /ą.bin",
                mime_type: "application/octet-stream",
                parent: Some("folder-1"),
            },
            total,
        )
        .await
        .unwrap()
}

#[test]
fn creates_and_replaces_with_streamed_binary_content_and_exact_headers() {
    block_on(async {
        let (mut drive, audit) = client([Ok(init()), Ok(complete(5)), Ok(init()), Ok(complete(3))]);
        let mut session = create(&mut drive, Some(5)).await;
        assert!(!session.needs_probe());
        assert!(matches!(
            drive
                .write_chunk(&mut session, &[0, 255, 128, 1, 0], true)
                .await
                .unwrap(),
            UploadOutcome::Complete(_)
        ));
        assert!(session.is_complete());
        assert_eq!(session.offset(), 5);
        assert!(drive.write_chunk(&mut session, b"", true).await.is_err());
        let mut session = drive
            .update_upload("file-1", "text/plain", Some(3))
            .await
            .unwrap();
        drive.write_chunk(&mut session, b"abc", true).await.unwrap();
        let sent = audit.borrow();
        assert_eq!(sent.len(), 4);
        assert_eq!(sent[0].method, Method::Post);
        assert_eq!(sent[2].method, Method::Patch);
        assert!(sent[2].url.contains("/files/file-1?uploadType=resumable"));
        let metadata: serde_json::Value = serde_json::from_slice(&sent[0].prefix).unwrap();
        assert_eq!(metadata["name"], "x\" /ą.bin");
        assert_eq!(metadata["parents"][0], "folder-1");
        assert_eq!(sent[0].header("X-Upload-Content-Length"), Some("5"));
        assert_eq!(sent[1].method, Method::Put);
        assert_eq!(sent[1].header("Content-Length"), Some("5"));
        assert_eq!(sent[1].header("Content-Range"), Some("bytes 0-4/5"));
        assert_eq!(sent[1].prefix, [0, 255, 128, 1, 0]);
        assert!(!sent[1].debug.contains("secret-session"));
        assert!(!sent[1].debug.contains("test-token"));
        assert!(!format!("{session:?}").contains("secret-session"));
    });
}

#[test]
fn partial_acknowledgement_controls_next_offset_and_tail() {
    block_on(async {
        let (mut drive, audit) = client([
            Ok(init()),
            Ok(ack(131072)),
            Ok(ack(393216)),
            Ok(complete(524291)),
        ]);
        let mut session = create(&mut drive, Some(524291)).await;
        let chunk = vec![42; UPLOAD_ALIGNMENT as usize];
        drive
            .write_chunk(&mut session, &chunk, false)
            .await
            .unwrap();
        assert_eq!(session.offset(), 131072);
        drive
            .write_chunk(&mut session, &chunk, false)
            .await
            .unwrap();
        assert_eq!(session.offset(), 393216);
        drive
            .write_chunk(&mut session, &vec![13; 131075], true)
            .await
            .unwrap();
        let sent = audit.borrow();
        assert_eq!(
            sent[2].header("Content-Range"),
            Some("bytes 131072-393215/524291")
        );
        assert_eq!(
            sent[3].header("Content-Range"),
            Some("bytes 393216-524290/524291")
        );
    });
}

#[test]
fn unknown_size_stream_uses_star_until_final_tail() {
    block_on(async {
        let (mut drive, audit) = client([Ok(init()), Ok(ack(262144)), Ok(complete(262147))]);
        let mut session = create(&mut drive, None).await;
        drive
            .write_chunk(&mut session, &vec![7; 262144], false)
            .await
            .unwrap();
        drive.write_chunk(&mut session, b"end", true).await.unwrap();
        assert_eq!(session.total_size(), Some(262147));
        let sent = audit.borrow();
        assert_eq!(sent[0].header("X-Upload-Content-Length"), None);
        assert_eq!(sent[1].header("Content-Range"), Some("bytes 0-262143/*"));
        assert_eq!(
            sent[2].header("Content-Range"),
            Some("bytes 262144-262146/262147")
        );
    });
}

#[test]
fn network_and_http_errors_require_probe_before_replay() {
    block_on(async {
        for failure in [
            Err(Failure),
            Ok(reply(503, &[], b"unavailable")),
            Ok(reply(401, &[], b"expired")),
        ] {
            let (mut drive, audit) = client([Ok(init()), failure, Ok(ack(2)), Ok(complete(5))]);
            let mut session = create(&mut drive, Some(5)).await;
            assert!(drive
                .write_chunk(&mut session, b"hello", true)
                .await
                .is_err());
            assert!(session.needs_probe());
            assert!(matches!(
                drive.write_chunk(&mut session, b"hello", true).await,
                Err(Error::Input(_))
            ));
            assert_eq!(audit.borrow().len(), 2);
            drive.set_access_token("renewed-token").unwrap();
            drive.query_upload(&mut session).await.unwrap();
            assert_eq!(session.offset(), 2);
            assert!(!session.needs_probe());
            drive.write_chunk(&mut session, b"llo", true).await.unwrap();
            let sent = audit.borrow();
            assert_eq!(sent[2].count, 0);
            assert_eq!(sent[2].header("Content-Range"), Some("bytes */5"));
            assert_eq!(
                sent[2].header("Authorization"),
                Some("Bearer renewed-token")
            );
            assert_eq!(sent[3].header("Content-Range"), Some("bytes 2-4/5"));
        }
    });
}

#[test]
fn restoring_probes_before_writing_and_recovers_completed_response() {
    block_on(async {
        let (mut drive, audit) = client([Ok(ack(262144)), Ok(complete(262147))]);
        let mut session = UploadSession::resume(URL, Some(262147)).unwrap();
        assert!(drive.write_chunk(&mut session, b"bad", true).await.is_err());
        assert!(audit.borrow().is_empty());
        drive.query_upload(&mut session).await.unwrap();
        drive.write_chunk(&mut session, b"end", true).await.unwrap();
        let (mut drive, _) = client([Ok(complete(9007199254740993))]);
        let mut session = UploadSession::resume(URL, None).unwrap();
        assert!(matches!(
            drive.query_upload(&mut session).await.unwrap(),
            UploadOutcome::Complete(_)
        ));
        assert_eq!(session.offset(), 9007199254740993);
    });
}

#[test]
fn missing_range_means_zero_and_regressions_are_rejected() {
    block_on(async {
        let (mut drive, _) = client([
            Ok(init()),
            Ok(reply(308, &[], b"")),
            Ok(ack(1)),
            Ok(reply(308, &[], b"")),
        ]);
        let mut session = create(&mut drive, Some(3)).await;
        drive.write_chunk(&mut session, b"abc", true).await.unwrap();
        assert_eq!(session.offset(), 0);
        drive.write_chunk(&mut session, b"abc", true).await.unwrap();
        assert_eq!(session.offset(), 1);
        assert!(matches!(
            drive.query_upload(&mut session).await,
            Err(Error::InvalidResponse(_))
        ));
        assert_eq!(session.offset(), 1);
        assert!(session.needs_probe());
    });
}

#[test]
fn expired_sessions_do_not_create_duplicate_files() {
    block_on(async {
        let (mut drive, audit) = client([Ok(reply(404, &[], b"expired"))]);
        let mut session = UploadSession::resume(URL, Some(3)).unwrap();
        assert_eq!(
            drive.query_upload(&mut session).await.unwrap_err(),
            Error::Http(404)
        );
        assert!(session.is_expired());
        assert!(drive.query_upload(&mut session).await.is_err());
        assert!(drive.write_chunk(&mut session, b"abc", true).await.is_err());
        assert_eq!(audit.borrow().len(), 1);
    });
}

#[test]
fn invalid_chunks_and_overflows_do_not_reach_transport() {
    block_on(async {
        let (mut drive, audit) = client([Ok(init())]);
        let mut session = create(&mut drive, Some(10)).await;
        for (bytes, final_chunk) in [
            (b"abc".as_slice(), false),
            (b"abc", true),
            (b"", false),
            (b"", true),
            (b"12345678901", true),
        ] {
            assert!(matches!(
                drive.write_chunk(&mut session, bytes, final_chunk).await,
                Err(Error::Input(_))
            ));
        }
        assert_eq!(audit.borrow().len(), 1);
        let (mut drive, _) = client([Ok(ack(u64::MAX))]);
        let mut session = UploadSession::resume(URL, None).unwrap();
        drive.query_upload(&mut session).await.unwrap();
        assert!(matches!(
            drive.write_chunk(&mut session, b"a", true).await,
            Err(Error::Input(_))
        ));
    });
}

#[test]
fn forged_ambiguous_and_nonempty_acknowledgements_leave_session_uncertain() {
    block_on(async {
        for response in [
            ack(4),
            reply(308, &[("Range", "bytes=1-2")], b""),
            reply(308, &[("Range", "bytes=0-18446744073709551615")], b""),
            reply(308, &[("Range", "bytes=0-+1")], b""),
            reply(308, &[("Range", "bytes=0-1"), ("range", "bytes=0-1")], b""),
            reply(308, &[("Range", "bytes=0-1")], b"unexpected"),
            reply(308, &[("Content-Length", "1")], b""),
            reply(308, &[("Content-Encoding", "gzip")], b""),
            complete(2),
            reply(200, &[], b"not json"),
            reply(302, &[("Location", "https://evil.example")], b""),
        ] {
            let (mut drive, audit) = client([Ok(init()), Ok(response)]);
            let mut session = create(&mut drive, Some(3)).await;
            assert!(drive.write_chunk(&mut session, b"abc", true).await.is_err());
            assert!(session.needs_probe());
            assert_eq!(session.offset(), 0);
            assert_eq!(audit.borrow().len(), 2);
        }
    });
}

#[test]
fn upload_location_is_confined_to_google_and_redacted() {
    block_on(async {
        for url in ["https://evil.example/upload/drive/v3/files?uploadType=resumable&upload_id=x",
            "http://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=x",
            "https://www.googleapis.com.evil/upload/drive/v3/files?uploadType=resumable&upload_id=x",
            "https://www.googleapis.com@evil/upload/drive/v3/files?uploadType=resumable&upload_id=x",
            "https://www.googleapis.com/upload/drive/v3/files/../bad?uploadType=resumable&upload_id=x",
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=x#fragment",
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=x\r\nInjected:y",
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=x&upload_id=y",
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=media&upload_id=x",
        ] {
            assert!(UploadSession::resume(url, None).is_err(), "{url}");
            let (mut drive, audit) = client([Ok(reply(200, &[("Location", url)], b""))]);
            assert!(drive.update_upload("id", "text/plain", None).await.is_err());
            assert_eq!(audit.borrow().len(), 1);
        }
        for headers in [vec![], vec![("Location", URL), ("location", URL)]] {
            let (mut drive, _) = client([Ok(reply(200, &headers, b""))]);
            assert!(drive.update_upload("id", "text/plain", None).await.is_err());
        }
    });
}

struct Generated {
    remaining: u64,
    produced: u64,
    max_read: usize,
    fail: bool,
    pending: bool,
}
impl Body for Generated {
    type Error = Failure;
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Failure> {
        if self.pending {
            core::future::pending::<()>().await;
        }
        if self.fail {
            return Err(Failure);
        }
        self.max_read = self.max_read.max(buffer.len());
        let n = self.remaining.min(buffer.len() as u64) as usize;
        for (i, b) in buffer[..n].iter_mut().enumerate() {
            *b = ((self.produced + i as u64) % 251) as u8;
        }
        self.remaining -= n as u64;
        self.produced += n as u64;
        Ok(n)
    }
}
fn generated(n: u64) -> Generated {
    Generated {
        remaining: n,
        produced: 0,
        max_read: 0,
        fail: false,
        pending: false,
    }
}

#[test]
fn large_single_request_has_bounded_source_reads_and_preserves_all_bytes() {
    block_on(async {
        let size = 32 * 1024 * 1024 + 7;
        let (mut drive, audit) = client([Ok(init()), Ok(complete(size))]);
        let mut session = create(&mut drive, Some(size)).await;
        let mut source = generated(size + 1);
        drive
            .write_stream(&mut session, &mut source, size, true)
            .await
            .unwrap();
        assert_eq!(source.remaining, 1, "must not consume next source byte");
        assert!(source.max_read <= 8192);
        let sent = audit.borrow();
        assert_eq!(sent.len(), 2, "one initialization and one content PUT");
        assert_eq!(sent[1].count, size);
        let expected = (0..size).fold(0u64, |sum, n| sum.wrapping_mul(31).wrapping_add(n % 251));
        assert_eq!(sent[1].hash, expected);
    });
}

#[test]
fn source_errors_truncation_and_cancellation_require_status_probe() {
    block_on(async {
        for fail in [false, true] {
            let (mut drive, _) = client([Ok(init()), Ok(complete(3))]);
            let mut session = create(&mut drive, Some(3)).await;
            let mut source = generated(2);
            source.fail = fail;
            assert!(drive
                .write_stream(&mut session, &mut source, 3, true)
                .await
                .is_err());
            assert!(session.needs_probe());
        }
        let (mut drive, _) = client([Ok(init()), Ok(reply(308, &[], b""))]);
        let mut session = create(&mut drive, Some(3)).await;
        let mut source = generated(3);
        source.pending = true;
        {
            let mut future = Box::pin(drive.write_stream(&mut session, &mut source, 3, true));
            assert!(matches!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
        }
        assert!(session.needs_probe());
        drive.query_upload(&mut session).await.unwrap();
        assert!(!session.needs_probe());
    });
}

#[test]
fn premature_transport_success_is_rejected() {
    block_on(async {
        let audit = Audit::default();
        let mut drive = Client::new(
            Mock {
                replies: [Ok(init())].into(),
                audit,
                early: true,
            },
            "token",
        )
        .unwrap();
        assert!(drive
            .update_upload("id", "text/plain", Some(3))
            .await
            .is_err());
    });
}

#[test]
fn final_metadata_is_bounded_and_update_identity_must_match() {
    block_on(async {
        let (mut drive, _) = client([Ok(init()), Ok(complete(3))]);
        let mut session = drive
            .update_upload("other-id", "text/plain", Some(3))
            .await
            .unwrap();
        assert!(drive.write_chunk(&mut session, b"abc", true).await.is_err());
        let (mut drive, _) = client([Ok(init()), Ok(complete(3))]);
        let mut session = create(&mut drive, Some(3)).await;
        drive.set_json_limit(10).unwrap();
        assert_eq!(
            drive
                .write_chunk(&mut session, b"abc", true)
                .await
                .unwrap_err(),
            Error::BodyTooLarge
        );
        assert!(session.needs_probe());
    });
}

#[test]
fn empty_file_and_fully_received_incomplete_status() {
    block_on(async {
        let (mut drive, audit) = client([Ok(init()), Ok(complete(0))]);
        let mut session = create(&mut drive, Some(0)).await;
        drive.write_chunk(&mut session, b"", true).await.unwrap();
        assert!(session.is_complete());
        assert_eq!(audit.borrow()[1].header("Content-Range"), Some("bytes */0"));
        let (mut drive, audit) = client([Ok(init()), Ok(ack(3)), Ok(complete(3))]);
        let mut session = create(&mut drive, Some(3)).await;
        drive.write_chunk(&mut session, b"abc", true).await.unwrap();
        assert!(!session.is_complete());
        assert!(session.needs_probe());
        drive.query_upload(&mut session).await.unwrap();
        assert!(session.is_complete());
        assert_eq!(audit.borrow()[2].header("Content-Range"), Some("bytes */3"));
    });
}

#[test]
fn unknown_nonfinal_request_cannot_complete_and_bad_inputs_never_send() {
    block_on(async {
        let (mut drive, audit) = client([Ok(init()), Ok(complete(262144))]);
        for mime in [
            "",
            "no-slash",
            "a/b\r\nInjected: yes",
            "application/vnd.google-apps.document",
        ] {
            assert!(drive.update_upload("id", mime, None).await.is_err());
        }
        assert!(drive
            .update_upload("../bad", "text/plain", None)
            .await
            .is_err());
        assert!(audit.borrow().is_empty());
        let mut session = create(&mut drive, None).await;
        assert!(drive
            .write_chunk(&mut session, &vec![0; 262144], false)
            .await
            .is_err());
        assert!(!session.is_complete());
        assert!(session.needs_probe());
    });
}

fn borrowed(response: &mut Response<Bytes>) -> Response<&mut Bytes> {
    Response {
        status: response.status,
        headers: response.headers.clone(),
        body: &mut response.body,
    }
}
async fn consume<B: Body<Error = Failure>>(source: &mut B) -> Result<Vec<u8>, Failure> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 19];
    loop {
        let n = source.read(&mut buffer).await?;
        if n == 0 {
            return Ok(bytes);
        }
        bytes.extend_from_slice(&buffer[..n]);
    }
}

#[test]
fn prepared_uploads_share_bounded_sources_and_borrow_callback_responses() {
    block_on(async {
        let drive = Client::new((), "callback-token").unwrap();
        let operation = drive
            .prepare_create_upload(
                NewFile {
                    name: "binary",
                    mime_type: "application/octet-stream",
                    parent: None,
                },
                Some(5),
            )
            .unwrap();
        let request = operation.request();
        assert_eq!(request.method(), Method::Post);
        let mut source = operation.body::<Failure>();
        let metadata = consume(&mut source).await.unwrap();
        assert_eq!(metadata.len() as u64, operation.length());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&metadata).unwrap()["name"],
            "binary"
        );
        let mut response = init();
        let mut session = operation.accept(borrowed(&mut response)).await.unwrap();
        assert_eq!(request.method(), Method::Post);
        assert!(!session.needs_probe());
        // The same operation cannot accidentally create a second session.
        assert!(operation.accept(borrowed(&mut response)).await.is_err());

        let operation = drive.prepare_write_stream(&mut session, 5, true).unwrap();
        let request = operation.request();
        let mut bytes = Bytes([0, 255, 128, 1, 0, 99].into());
        let mut source = operation.body(&mut bytes);
        assert_eq!(consume(&mut source).await.unwrap(), [0, 255, 128, 1, 0]);
        assert_eq!(operation.length(), 5);
        let mut response = complete(5);
        assert!(matches!(
            operation.accept(borrowed(&mut response)).await.unwrap(),
            UploadOutcome::Complete(_)
        ));
        assert!(request
            .headers()
            .any(|(k, v)| k == "Content-Range" && v == "bytes 0-4/5"));
        assert!(operation.accept(borrowed(&mut response)).await.is_err());
        assert_eq!(bytes.0, [99]);
        drop(operation);
        assert!(session.is_complete());
        assert_eq!(session.offset(), 5);
    });
}

#[test]
fn prepared_cancelled_short_or_failed_sources_require_probe() {
    block_on(async {
        let drive = Client::new((), "token").unwrap();
        for mode in ["short", "error", "cancel"] {
            let mut session = UploadSession::resume(URL, Some(3)).unwrap();
            let probe = drive.prepare_query_upload(&mut session).unwrap();
            probe
                .accept(borrowed(&mut reply(308, &[], b"")))
                .await
                .unwrap();
            drop(probe);
            let operation = drive.prepare_write_stream(&mut session, 3, true).unwrap();
            let mut generated = generated(2);
            generated.fail = mode == "error";
            generated.pending = mode == "cancel";
            let mut source = operation.body(&mut generated);
            if mode == "cancel" {
                let mut buffer = [0; 8];
                let mut future = Box::pin(source.read(&mut buffer));
                assert!(matches!(
                    future
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop())),
                    Poll::Pending
                ));
            } else {
                let result = consume(&mut source).await;
                assert_eq!(result.is_err(), mode == "error");
                assert!(matches!(
                    operation.accept(borrowed(&mut complete(3))).await,
                    Err(Error::InvalidResponse(
                        "request body was not fully consumed"
                    ))
                ));
            }
            drop(operation);
            assert!(session.needs_probe());
            assert!(!session.is_complete());
            assert!(drive.prepare_write_stream(&mut session, 3, true).is_err());
            let operation = drive.prepare_query_upload(&mut session).unwrap();
            assert!(matches!(
                operation.accept(borrowed(&mut ack(1))).await.unwrap(),
                UploadOutcome::Incomplete { confirmed: 1 }
            ));
            drop(operation);
            assert_eq!(session.offset(), 1);
            assert!(!session.needs_probe());
        }
    });
}

#[test]
fn dropping_a_prepared_write_before_io_also_requires_probe() {
    let drive = Client::new((), "token").unwrap();
    let mut session = UploadSession::resume(URL, Some(3)).unwrap();
    block_on(
        drive
            .prepare_query_upload(&mut session)
            .unwrap()
            .accept(borrowed(&mut reply(308, &[], b""))),
    )
    .unwrap();
    assert!(!session.needs_probe());
    drop(drive.prepare_write_stream(&mut session, 3, true).unwrap());
    assert!(session.needs_probe());
}

#[test]
fn cancelling_prepared_response_acceptance_keeps_the_session_uncertain() {
    struct Pending;
    impl Body for Pending {
        type Error = Failure;
        async fn read(&mut self, _: &mut [u8]) -> Result<usize, Failure> {
            core::future::pending().await
        }
    }
    block_on(async {
        let drive = Client::new((), "token").unwrap();
        let mut session = UploadSession::resume(URL, Some(3)).unwrap();
        drive
            .prepare_query_upload(&mut session)
            .unwrap()
            .accept(borrowed(&mut reply(308, &[], b"")))
            .await
            .unwrap();
        let operation = drive.prepare_write_stream(&mut session, 3, true).unwrap();
        let mut source = operation.body(generated(3));
        assert_eq!(consume(&mut source).await.unwrap().len(), 3);
        {
            let mut pending = Pending;
            let mut future = Box::pin(operation.accept(Response {
                status: 201,
                headers: vec![],
                body: &mut pending,
            }));
            assert!(matches!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
        }
        assert!(operation.accept(borrowed(&mut complete(3))).await.is_err());
        drop(operation);
        assert!(session.needs_probe());
        assert!(!session.is_complete());
        assert_eq!(session.offset(), 0);
        drive
            .prepare_query_upload(&mut session)
            .unwrap()
            .accept(borrowed(&mut complete(3)))
            .await
            .unwrap();
        assert!(session.is_complete());
    });
}
