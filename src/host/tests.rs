use super::*;
use crate::{ByteRange, Client, NewFile, UploadOutcome};
use futures_executor::block_on;
use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    prelude::v1::*,
    time::Instant,
};

const SESSION: &str = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=dummy-loopback-session";
const BUFFER: usize = 64 * 1024;
struct Pattern(u64);
impl Read for Pattern {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let n = self.0.min(bytes.len() as u64) as usize;
        bytes[..n].fill(0x5a);
        self.0 -= n as u64;
        Ok(n)
    }
}
fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .max_redirects(0)
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(30)))
        .build()
        .into()
}
fn transport(url: String) -> UreqTransport {
    UreqTransport {
        agent: agent(),
        loopback: Some(url),
    }
}
fn read_headers(stream: &mut BufReader<TcpStream>) -> (String, Vec<(String, String)>) {
    let mut first = String::new();
    stream.read_line(&mut first).unwrap();
    let method = first.split_whitespace().next().unwrap().into();
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        stream.read_line(&mut line).unwrap();
        if line == "\r\n" {
            break;
        }
        assert!(!line.is_empty(), "unexpected request EOF");
        let (name, value) = line.trim().split_once(':').unwrap();
        headers.push((name.to_ascii_lowercase(), value.trim().into()));
    }
    (method, headers)
}
fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}
fn response(stream: &mut TcpStream, status: u16, headers: &str, body: &[u8]) {
    write!(
        stream,
        "HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(body).unwrap();
}
fn fixture(requests: usize, size: u64) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = alloc::format!("http://{}/", listener.local_addr().unwrap());
    let worker = thread::spawn(move || {
        for _ in 0..requests {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(30)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(30)))
                .unwrap();
            let mut stream = BufReader::new(stream);
            let (method, headers) = read_headers(&mut stream);
            let mut left = header(&headers, "content-length")
                .map(|s| s.parse::<u64>().unwrap())
                .unwrap_or(0);
            let mut buffer = [0; BUFFER];
            if method == "PUT" {
                assert_eq!(left, size);
            }
            while left != 0 {
                let cap = left.min(BUFFER as u64) as usize;
                let n = stream.read(&mut buffer[..cap]).unwrap();
                assert!(n > 0);
                if method == "PUT" {
                    assert!(buffer[..n].iter().all(|b| *b == 0x5a));
                }
                left -= n as u64;
            }
            let mut stream = stream.into_inner();
            match method.as_str() {
                "POST" => response(
                    &mut stream,
                    200,
                    &alloc::format!("Location: {SESSION}\r\n"),
                    b"",
                ),
                "PUT" => {
                    assert_eq!(
                        header(&headers, "content-range"),
                        Some(alloc::format!("bytes 0-{}/{size}", size - 1).as_str())
                    );
                    let json = alloc::format!(
                        r#"{{"id":"file-1","name":"loopback","mimeType":"application/octet-stream","size":"{size}"}}"#
                    );
                    response(
                        &mut stream,
                        200,
                        "Content-Type: application/json\r\n",
                        json.as_bytes(),
                    );
                }
                "GET" => {
                    write!(stream, "HTTP/1.1 206 Partial Content\r\nContent-Length: {size}\r\nContent-Range: bytes 0-{}/{size}\r\nConnection: close\r\n\r\n", size-1).unwrap();
                    io::copy(&mut Pattern(size), &mut stream).unwrap();
                }
                _ => std::panic!("unexpected method: {method}"),
            }
        }
    });
    (url, worker)
}
async fn upload(client: &mut Client<UreqTransport>, size: u64) -> Duration {
    let mut session = client
        .create_upload(
            NewFile {
                name: "loopback",
                mime_type: "application/octet-stream",
                parent: None,
            },
            Some(size),
        )
        .await
        .unwrap();
    let start = Instant::now();
    assert!(matches!(
        client
            .write_stream(&mut session, &mut ReadBody(Pattern(size)), size, true)
            .await
            .unwrap(),
        UploadOutcome::Complete(_)
    ));
    start.elapsed()
}
async fn download(client: &mut Client<UreqTransport>, size: u64) -> Duration {
    let start = Instant::now();
    let mut stream = client
        .read_range("file-1", ByteRange::new(0, size).unwrap())
        .await
        .unwrap();
    let mut bytes = [0; BUFFER];
    let mut total = 0;
    loop {
        let n = stream.read(&mut bytes).await.unwrap();
        if n == 0 {
            break;
        }
        assert!(bytes[..n].iter().all(|b| *b == 0x5a));
        total += n as u64;
    }
    assert_eq!(total, size);
    start.elapsed()
}

#[test]
fn host_adapter_streams_both_directions_over_http() {
    let size = 8 * 1024 * 1024 + 7;
    let (url, worker) = fixture(3, size);
    let mut client = Client::new(transport(url), "dummy-loopback-token").unwrap();
    block_on(async {
        upload(&mut client, size).await;
        download(&mut client, size).await;
    });
    worker.join().unwrap();
}

#[test]
fn host_adapter_never_follows_308_or_forwards_credentials() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = alloc::format!("http://{}/", listener.local_addr().unwrap());
    let worker = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut stream = BufReader::new(stream);
        read_headers(&mut stream);
        response(
            stream.get_mut(),
            308,
            "Location: http://127.0.0.1:1/must-not-follow\r\n",
            b"",
        );
    });
    let mut client = Client::new(transport(url), "dummy-loopback-token").unwrap();
    let mut session = crate::UploadSession::resume(SESSION, Some(10)).unwrap();
    assert!(matches!(
        block_on(client.query_upload(&mut session)).unwrap(),
        UploadOutcome::Incomplete { confirmed: 0 }
    ));
    worker.join().unwrap();
}

#[test]
fn bridge_handles_pending_source_inside_an_executor() {
    struct YieldOnce(bool);
    impl Body for YieldOnce {
        type Error = io::Error;
        async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            core::future::poll_fn(|cx| {
                if !self.0 {
                    self.0 = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    bytes[0] = 42;
                    Poll::Ready(Ok(1))
                }
            })
            .await
        }
    }
    block_on(async {
        let mut source = YieldOnce(false);
        let mut bridge = Bridge {
            body: &mut source,
            waker: Waker::from(Arc::new(ThreadWake(thread::current()))),
        };
        let mut byte = [0];
        assert_eq!(bridge.read(&mut byte).unwrap(), 1);
        assert_eq!(byte, [42]);
    });
}

/// Local HTTP only: isolates wrapper overhead, excludes TLS, Internet and Drive.
/// Run in release mode; outputs medians without a noisy CI timing threshold.
#[test]
#[ignore]
fn loopback_throughput() {
    const SIZE: u64 = 512 * 1024 * 1024;
    const ROUNDS: usize = 10;
    let (url, worker) = fixture(5 * ROUNDS, SIZE);
    let mut client = Client::new(transport(url.clone()), "dummy-loopback-token").unwrap();
    let baseline = agent();
    let mut writes = [Vec::new(), Vec::new()];
    let mut reads = [Vec::new(), Vec::new()];
    for round in 0..ROUNDS {
        // Alternate order to reduce warmup/order bias. Both paths validate every
        // payload byte and use identical ureq settings and source/read buffers.
        for path in [round % 2, 1 - round % 2] {
            let (write, read) = if path == 0 {
                block_on(async {
                    (
                        upload(&mut client, SIZE).await,
                        download(&mut client, SIZE).await,
                    )
                })
            } else {
                let start = Instant::now();
                let mut source = Pattern(SIZE);
                let mut result = baseline
                    .put(&url)
                    .header("Content-Length", SIZE.to_string())
                    .header(
                        "Content-Range",
                        alloc::format!("bytes 0-{}/{SIZE}", SIZE - 1),
                    )
                    .send(ureq::SendBody::from_reader(&mut source))
                    .unwrap();
                let mut json = String::new();
                result
                    .body_mut()
                    .as_reader()
                    .read_to_string(&mut json)
                    .unwrap();
                let _: serde_json::Value = serde_json::from_str(&json).unwrap();
                let write = start.elapsed();
                let start = Instant::now();
                let mut result = baseline
                    .get(&url)
                    .header("Range", alloc::format!("bytes=0-{}", SIZE - 1))
                    .call()
                    .unwrap();
                let mut reader = result.body_mut().as_reader();
                let mut buffer = [0; BUFFER];
                let mut total = 0;
                loop {
                    let n = reader.read(&mut buffer).unwrap();
                    if n == 0 {
                        break;
                    }
                    assert!(buffer[..n].iter().all(|b| *b == 0x5a));
                    total += n as u64;
                }
                assert_eq!(total, SIZE);
                (write, start.elapsed())
            };
            writes[path].push(write.as_secs_f64());
            reads[path].push(read.as_secs_f64());
        }
    }
    worker.join().unwrap();
    for (label, mut timings) in [("upload", writes), ("download", reads)] {
        for path in &mut timings {
            path.sort_by(f64::total_cmp);
        }
        let medians = timings.map(|t| (t[ROUNDS / 2 - 1] + t[ROUNDS / 2]) / 2.0);
        std::println!(
            "{label}: client {:.1} MiB/s; direct ureq {:.1} MiB/s; ratio {:.3}",
            (SIZE as f64 / 1048576.0) / medians[0],
            (SIZE as f64 / 1048576.0) / medians[1],
            medians[1] / medians[0]
        );
    }
}
