use futures_executor::block_on;
use google_drive_zero::{
    transport::{Body, Request, Response, Transport},
    ByteRange, Client, Error, ListOptions,
};
use std::{
    cell::RefCell,
    collections::VecDeque,
    convert::Infallible,
    future::Future,
    rc::Rc,
    task::{Context, Poll, Waker},
};

const FILE: &[u8] = br#"{"id":"file-1","name":"hello","mimeType":"application/octet-stream","size":"5368709121","version":"9007199254740993","headRevisionId":"rev_2","capabilities":{"canDownload":true},"unknownFutureField":true}"#;
type Audit = Rc<RefCell<Vec<(String, Vec<(String, String)>, String)>>>;
#[derive(Debug, PartialEq, Eq)]
struct NetworkError;
struct Bytes {
    bytes: VecDeque<u8>,
    chunk: usize,
    calls: Rc<RefCell<usize>>,
    fail_after: Option<usize>,
    pending: bool,
}
impl Body for Bytes {
    type Error = NetworkError;
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        if buffer.is_empty() {
            return Ok(0);
        }
        *self.calls.borrow_mut() += 1;
        if self.pending {
            core::future::pending::<()>().await;
        }
        if self.fail_after == Some(0) {
            return Err(NetworkError);
        }
        if let Some(left) = &mut self.fail_after {
            *left -= 1;
        }
        let n = self.bytes.len().min(buffer.len()).min(self.chunk);
        for byte in &mut buffer[..n] {
            *byte = self.bytes.pop_front().unwrap();
        }
        Ok(n)
    }
}
struct Mock {
    replies: VecDeque<Result<Response<Bytes>, NetworkError>>,
    audit: Audit,
}
impl Transport for Mock {
    type Error = NetworkError;
    type Body<'a> = Bytes;
    async fn send<'a>(
        &'a mut self,
        request: &Request,
    ) -> Result<Response<Self::Body<'a>>, Self::Error> {
        self.audit.borrow_mut().push((
            request.url().to_owned(),
            request
                .headers()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
            format!("{request:?}"),
        ));
        self.replies
            .pop_front()
            .expect("unexpected extra HTTP request")
    }
}
fn reply(status: u16, headers: &[(&str, &str)], bytes: &[u8]) -> Response<Bytes> {
    Response {
        status,
        headers: headers
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect(),
        body: Bytes {
            bytes: bytes.iter().copied().collect(),
            chunk: 3,
            calls: Default::default(),
            fail_after: None,
            pending: false,
        },
    }
}
fn client(replies: impl IntoIterator<Item = Response<Bytes>>) -> (Client<Mock>, Audit) {
    let audit = Audit::default();
    let transport = Mock {
        replies: replies.into_iter().map(Ok).collect(),
        audit: audit.clone(),
    };
    (
        Client::new(transport, "mothership.secret-token").unwrap(),
        audit,
    )
}
async fn collect<B: Body<Error = NetworkError>>(
    mut download: google_drive_zero::Download<B>,
) -> Result<Vec<u8>, Error<NetworkError>> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 7];
    loop {
        let n = download.read(&mut buffer).await?;
        if n == 0 {
            return Ok(bytes);
        }
        bytes.extend_from_slice(&buffer[..n]);
    }
}

#[test]
fn listing_pagination_preserves_empty_pages_and_incomplete_search() {
    block_on(async {
        let (mut drive, audit) = client([
            reply(
                200,
                &[],
                r#"{"files":[],"nextPageToken":"+/&=?# ą","incompleteSearch":true}"#.as_bytes(),
            ),
            reply(200, &[], br#"{"files":[{"id":"a","name":"same","mimeType":"blob"},{"id":"b","name":"same","mimeType":"blob"}]}"#),
        ]);
        let options = ListOptions {
            query: Some("name = 'zażółć &?.iso'"),
            drive_id: Some("shared-drive_1"),
            ..Default::default()
        };
        let page = drive.list(&options).await.unwrap();
        assert!(page.files.is_empty());
        assert!(page.incomplete_search);
        assert!(page.next_page_token.is_some());
        let last = drive
            .list(&ListOptions {
                page_token: page.next_page_token.as_deref(),
                ..options
            })
            .await
            .unwrap();
        assert!(last.next_page_token.is_none());
        assert!(!last.incomplete_search);
        assert_eq!(last.files.len(), 2);
        assert_eq!(last.files[0].name, last.files[1].name);
        assert_ne!(last.files[0].id, last.files[1].id);
        let requests = audit.borrow();
        assert!(requests[0]
            .0
            .contains("&corpora=drive&driveId=shared-drive_1"));
        assert!(requests[0]
            .0
            .contains("za%C5%BC%C3%B3%C5%82%C4%87%20%26%3F.iso"));
        assert!(requests[1]
            .0
            .contains("pageToken=%2B%2F%26%3D%3F%23%20%C4%85"));
    });
}

#[test]
fn metadata_retains_large_integer_strings_and_optional_fields() {
    block_on(async {
        let (mut drive, _) = client([reply(200, &[], FILE), reply(200, &[], br#"{"id":"root","name":"My Drive","mimeType":"application/vnd.google-apps.folder"}"#)]);
        let file = drive.metadata("file-1").await.unwrap();
        assert_eq!(file.size, Some(5_368_709_121));
        assert_eq!(file.version, Some(9_007_199_254_740_993));
        assert_eq!(file.head_revision_id.as_deref(), Some("rev_2"));
        assert_eq!(file.capabilities.unwrap().can_download, Some(true));
        let folder = drive.metadata("root").await.unwrap();
        assert!(folder.is_folder());
        assert_eq!(folder.size, None);
    });
}

#[test]
fn current_and_revision_reads_stream_exact_binary_bytes_without_eager_reads() {
    block_on(async {
        let bytes = [0, 255, 1, 0, 128, 19, 42, 254, 10, 99];
        let response = reply(
            206,
            &[
                ("cOnTeNt-RaNgE", "bytes 512-521/1000"),
                ("Content-Length", "10"),
            ],
            &bytes,
        );
        let calls = response.body.calls.clone();
        let (mut drive, audit) = client([
            response,
            reply(206, &[("Content-Range", "bytes 0-9/1000")], &bytes),
        ]);
        let mut download = drive
            .read_range("file-1", ByteRange::new(512, 10).unwrap())
            .await
            .unwrap();
        assert_eq!(*calls.borrow(), 0, "body must stay lazy");
        assert_eq!(download.total_size(), 1000);
        assert_eq!(download.length(), 10);
        assert_eq!(download.remaining(), 10);
        assert_eq!(download.read(&mut []).await.unwrap(), 0);
        assert_eq!(*calls.borrow(), 0);
        assert_eq!(collect(download).await.unwrap(), bytes);
        let revision = drive
            .read_revision_range("file-1", "rev_2", ByteRange::new(0, 10).unwrap())
            .await
            .unwrap();
        assert_eq!(collect(revision).await.unwrap(), bytes);
        let requests = audit.borrow();
        assert_eq!(
            requests[0].0,
            "https://www.googleapis.com/drive/v3/files/file-1?alt=media&supportsAllDrives=true"
        );
        assert_eq!(
            requests[1].0,
            "https://www.googleapis.com/drive/v3/files/file-1/revisions/rev_2?alt=media"
        );
        assert!(requests[0]
            .1
            .iter()
            .any(|(k, v)| k == "Range" && v == "bytes=512-521"));
        assert!(requests[0]
            .1
            .iter()
            .any(|(k, v)| k == "Accept-Encoding" && v == "identity"));
    });
}

#[test]
fn ranges_clip_only_at_actual_file_eof() {
    block_on(async {
        let (mut drive, _) = client([reply(206, &[("Content-Range", "bytes 6-9/10")], b"tail")]);
        let download = drive
            .read_range("id", ByteRange::new(6, 20).unwrap())
            .await
            .unwrap();
        assert_eq!(download.length(), 4);
        assert_eq!(collect(download).await.unwrap(), b"tail");
    });
}

#[test]
fn range_construction_rejects_zero_length_and_overflow() {
    assert!(ByteRange::new(0, 0).is_err());
    assert!(ByteRange::new(u64::MAX, 2).is_err());
    let last = ByteRange::new(u64::MAX, 1).unwrap();
    assert_eq!(last.offset(), u64::MAX);
    assert_eq!(last.length(), 1);
    assert_eq!(ByteRange::new(0, u64::MAX).unwrap().length(), u64::MAX);
}

#[test]
fn bad_identifiers_and_page_sizes_never_reach_transport() {
    block_on(async {
        let (mut drive, audit) = client([]);
        for id in [
            "",
            ".",
            "..",
            "a/b",
            "a?alt=media",
            "id\r\nHost: evil",
            "a%2Fb",
            "ś",
        ] {
            assert!(matches!(drive.metadata(id).await, Err(Error::Input(_))));
            assert!(matches!(
                drive.read_range(id, ByteRange::new(0, 1).unwrap()).await,
                Err(Error::Input(_))
            ));
            assert!(matches!(
                drive
                    .read_revision_range("ok", id, ByteRange::new(0, 1).unwrap())
                    .await,
                Err(Error::Input(_))
            ));
        }
        for page_size in [0, 1001, u16::MAX] {
            assert!(matches!(
                drive
                    .list(&ListOptions {
                        page_size,
                        ..Default::default()
                    })
                    .await,
                Err(Error::Input(_))
            ));
        }
        assert!(audit.borrow().is_empty());
    });
}

#[test]
fn token_rotation_and_debug_do_not_leak_credentials() {
    block_on(async {
        let (mut drive, audit) = client([reply(200, &[], FILE), reply(200, &[], FILE)]);
        assert!(!format!("{drive:?}").contains("mothership.secret-token"));
        drive.metadata("id").await.unwrap();
        for bad in ["", "bad\r\nInjected: yes", "bad token", "é"] {
            assert!(drive.set_access_token(bad).is_err());
        }
        drive.set_access_token("replacement.token").unwrap();
        drive.metadata("id").await.unwrap();
        let requests = audit.borrow();
        for (i, token) in ["mothership.secret-token", "replacement.token"]
            .iter()
            .enumerate()
        {
            assert!(!requests[i].0.contains(token));
            assert!(!requests[i].2.contains(token));
            assert!(requests[i]
                .1
                .iter()
                .any(|(k, v)| k == "Authorization" && v == &format!("Bearer {token}")));
        }
    });
}

#[test]
fn http_failures_and_redirects_remain_visible_without_retry() {
    block_on(async {
        for status in [200, 301, 302, 401, 403, 404, 410, 416, 429, 500, 503] {
            let (mut drive, audit) = client([reply(status, &[], b"server error")]);
            let error = drive
                .read_range("id", ByteRange::new(0, 1).unwrap())
                .await
                .err()
                .unwrap();
            assert_eq!(error, Error::Http(status));
            assert_eq!(audit.borrow().len(), 1);
        }
        for status in [301, 401, 403, 429, 503] {
            let (mut drive, _) = client([reply(status, &[], b"not JSON")]);
            assert_eq!(drive.metadata("id").await.unwrap_err(), Error::Http(status));
        }
    });
}

#[test]
fn malformed_or_unrelated_ranges_are_rejected_before_body_read() {
    block_on(async {
        for value in [
            "",
            "items 10-13/100",
            "bytes */100",
            "bytes 10-13/*",
            "bytes 10-13/0",
            "bytes 9-12/100",
            "bytes 10-12/100",
            "bytes 10-14/100",
            "bytes 10-13/13",
            "bytes 10-9/100",
            "bytes +10-13/100",
            "bytes 10-13/18446744073709551616",
            "bytes 10-13/100, 20-30/100",
        ] {
            let response = reply(206, &[("Content-Range", value)], b"data");
            let calls = response.body.calls.clone();
            let (mut drive, _) = client([response]);
            assert!(
                matches!(
                    drive.read_range("id", ByteRange::new(10, 4).unwrap()).await,
                    Err(Error::InvalidResponse(_))
                ),
                "{value}"
            );
            assert_eq!(*calls.borrow(), 0);
        }
    });
}

#[test]
fn missing_duplicate_encoded_and_mismatched_range_headers_fail() {
    block_on(async {
        for headers in [
            vec![],
            vec![
                ("Content-Range", "bytes 0-3/10"),
                ("content-range", "bytes 0-3/10"),
            ],
            vec![
                ("Content-Range", "bytes 0-3/10"),
                ("Content-Encoding", "gzip"),
            ],
            vec![("Content-Range", "bytes 0-3/10"), ("Content-Length", "5")],
            vec![
                ("Content-Range", "bytes 0-3/10"),
                ("Content-Length", "nope"),
            ],
            vec![
                ("Content-Range", "bytes 0-3/10"),
                ("Content-Length", "4"),
                ("content-length", "4"),
            ],
            vec![
                ("Content-Range", "bytes 0-3/10"),
                ("Content-Encoding", "identity"),
                ("content-encoding", "identity"),
            ],
        ] {
            let (mut drive, _) = client([reply(206, &headers, b"data")]);
            assert!(matches!(
                drive.read_range("id", ByteRange::new(0, 4).unwrap()).await,
                Err(Error::InvalidResponse(_))
            ));
        }
    });
}

#[test]
fn short_and_oversized_bodies_cannot_be_completed() {
    block_on(async {
        for bytes in [b"dat".as_slice(), b"data!"] {
            let (mut drive, _) = client([reply(206, &[("Content-Range", "bytes 0-3/10")], bytes)]);
            let download = drive
                .read_range("id", ByteRange::new(0, 4).unwrap())
                .await
                .unwrap();
            assert!(matches!(
                collect(download).await,
                Err(Error::InvalidResponse(_))
            ));
        }
    });
}

#[test]
fn stream_transport_failure_is_sticky() {
    block_on(async {
        let mut response = reply(206, &[("Content-Range", "bytes 0-9/10")], b"0123456789");
        response.body.fail_after = Some(1);
        let (mut drive, _) = client([response]);
        let mut download = drive
            .read_range("id", ByteRange::new(0, 10).unwrap())
            .await
            .unwrap();
        let mut buffer = [0; 10];
        assert_eq!(download.read(&mut buffer).await.unwrap(), 3);
        assert_eq!(
            download.read(&mut buffer).await,
            Err(Error::Transport(NetworkError))
        );
        assert!(matches!(
            download.read(&mut buffer).await,
            Err(Error::InvalidResponse(_))
        ));
    });
}

#[test]
fn cancelling_a_pending_read_invalidates_the_download() {
    block_on(async {
        let mut response = reply(206, &[("Content-Range", "bytes 0-3/4")], b"data");
        response.body.pending = true;
        let (mut drive, _) = client([response]);
        let mut download = drive
            .read_range("id", ByteRange::new(0, 4).unwrap())
            .await
            .unwrap();
        let mut buffer = [0; 4];
        {
            let mut future = Box::pin(download.read(&mut buffer));
            assert!(matches!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
        }
        assert!(matches!(
            download.read(&mut buffer).await,
            Err(Error::InvalidResponse(_))
        ));
    });
}

#[test]
fn metadata_limits_apply_before_parsing_even_without_content_length() {
    block_on(async {
        for headers in [vec![], vec![("Content-Length", "2000000")]] {
            let response = reply(200, &headers, FILE);
            let calls = response.body.calls.clone();
            let (mut drive, _) = client([response]);
            assert!(drive.set_json_limit(0).is_err());
            drive.set_json_limit(8).unwrap();
            assert_eq!(drive.metadata("id").await, Err(Error::BodyTooLarge));
            if !headers.is_empty() {
                assert_eq!(*calls.borrow(), 0);
            }
        }
        let (mut drive, _) = client([reply(200, &[], FILE)]);
        drive.set_json_limit(FILE.len()).unwrap();
        assert!(drive.metadata("id").await.is_ok());
    });
}

#[test]
fn metadata_rejects_wrong_length_encoding_and_bad_integer_types() {
    block_on(async {
        for headers in [
            vec![("Content-Length", "1")],
            vec![("Content-Length", "bad")],
            vec![("Content-Length", "1"), ("content-length", "1")],
            vec![("Content-Encoding", "gzip")],
        ] {
            let (mut drive, _) = client([reply(200, &headers, FILE)]);
            assert!(matches!(
                drive.metadata("id").await,
                Err(Error::InvalidResponse(_))
            ));
        }
        for value in ["-1", "18446744073709551616"] {
            let bytes =
                format!(r#"{{"id":"id","name":"file","mimeType":"blob","size":"{value}"}}"#);
            let (mut drive, _) = client([reply(200, &[], bytes.as_bytes())]);
            assert_eq!(drive.metadata("id").await, Err(Error::Json));
        }
        let (mut drive, _) = client([reply(
            200,
            &[],
            br#"{"id":"id","name":"file","mimeType":"blob","size":12}"#,
        )]);
        assert_eq!(drive.metadata("id").await, Err(Error::Json));
    });
}

#[test]
fn send_errors_propagate_without_retry() {
    block_on(async {
        let (mut drive, audit) = client([]);
        let mut mock = drive.into_transport();
        mock.replies.push_back(Err(NetworkError));
        drive = Client::new(mock, "token").unwrap();
        assert_eq!(
            drive.metadata("id").await,
            Err(Error::Transport(NetworkError))
        );
        assert_eq!(audit.borrow().len(), 1);
    });
}

// Prove the transport can lend a response body from its own connection state.
struct Borrowing(VecDeque<u8>);
impl Body for &mut Borrowing {
    type Error = Infallible;
    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        let n = self.0.len().min(buffer.len());
        for byte in &mut buffer[..n] {
            *byte = self.0.pop_front().unwrap();
        }
        Ok(n)
    }
}
impl Transport for Borrowing {
    type Error = Infallible;
    type Body<'a> = &'a mut Self;
    async fn send<'a>(&'a mut self, _: &Request) -> Result<Response<Self::Body<'a>>, Self::Error> {
        Ok(Response {
            status: 200,
            headers: vec![],
            body: self,
        })
    }
}
#[test]
fn transport_can_borrow_its_connection_for_a_response() {
    block_on(async {
        let mut drive = Client::new(Borrowing(FILE.iter().copied().collect()), "token").unwrap();
        assert_eq!(drive.metadata("id").await.unwrap().name, "hello");
    });
}
