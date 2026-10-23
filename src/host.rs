//! Optional blocking Rustls/ureq adapter for hosted applications. It blocks the
//! calling thread; use a dedicated worker rather than an async reactor thread.
use crate::transport::{Body, Request, Response, Transport, UploadTransport};
use alloc::{string::ToString, sync::Arc};
use core::{
    future::Future,
    task::{Context, Poll, Waker},
};
use std::{
    io::{self, Read},
    task::Wake,
    thread,
    time::Duration,
};

/// A pooled, authenticated HTTPS transport with redirects and decompression
/// disabled. Transport errors omit request URLs and credentials.
pub struct UreqTransport {
    agent: ureq::Agent,
    #[cfg(test)]
    loopback: Option<alloc::string::String>,
}
impl UreqTransport {
    pub fn new(timeout: Duration) -> Self {
        Self {
            agent: ureq::Agent::config_builder()
                .max_redirects(0)
                .http_status_as_error(false)
                .timeout_global(Some(timeout))
                .build()
                .into(),
            #[cfg(test)]
            loopback: None,
        }
    }
    fn outgoing(&self, request: &Request) -> ureq::http::request::Builder {
        #[cfg(test)]
        let url = self.loopback.as_deref().unwrap_or(request.url());
        #[cfg(not(test))]
        let url = request.url();
        let mut outgoing = ureq::http::Request::builder()
            .method(request.method().as_str())
            .uri(url);
        for (name, value) in request.headers() {
            outgoing = outgoing.header(name, value);
        }
        outgoing
    }
}
impl Default for UreqTransport {
    fn default() -> Self {
        Self::new(Duration::from_secs(1800))
    }
}
pub struct ReadBody<R>(pub R);
impl<R: Read> Body for ReadBody<R> {
    type Error = io::Error;
    async fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0.read(buffer)
    }
}
impl Transport for UreqTransport {
    type Error = io::Error;
    type Body<'a> = ReadBody<ureq::BodyReader<'static>>;
    async fn send<'a>(&'a mut self, request: &Request) -> io::Result<Response<Self::Body<'a>>> {
        let outgoing = self
            .outgoing(request)
            .body(())
            .map_err(|_| invalid_header())?;
        convert(self.agent.run(outgoing).map_err(|_| request_failed())?)
    }
}
impl UploadTransport for UreqTransport {
    async fn send_body<'a, B: Body<Error = io::Error>>(
        &'a mut self,
        request: &Request,
        body: &mut B,
    ) -> io::Result<Response<Self::Body<'a>>> {
        // One wake allocation per HTTP request, never per payload read. This
        // poll loop also works when called inside an existing block_on executor.
        let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
        let mut reader = Bridge { body, waker };
        let outgoing = self
            .outgoing(request)
            .body(ureq::SendBody::from_reader(&mut reader))
            .map_err(|_| invalid_header())?;
        convert(self.agent.run(outgoing).map_err(|_| request_failed())?)
    }
}
struct ThreadWake(thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}
struct Bridge<'a, B> {
    body: &'a mut B,
    waker: Waker,
}
impl<B: Body<Error = io::Error>> Read for Bridge<'_, B> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let mut future = core::pin::pin!(self.body.read(buffer));
        let mut context = Context::from_waker(&self.waker);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(result) => return result,
                Poll::Pending => thread::park(),
            }
        }
    }
}
fn convert(
    response: ureq::http::Response<ureq::Body>,
) -> io::Result<Response<ReadBody<ureq::BodyReader<'static>>>> {
    let (parts, body) = response.into_parts();
    let headers = parts
        .headers
        .iter()
        .map(|(k, v)| {
            Ok((
                k.to_string(),
                v.to_str().map_err(|_| invalid_header())?.to_string(),
            ))
        })
        .collect::<io::Result<_>>()?;
    Ok(Response {
        status: parts.status.as_u16(),
        headers,
        body: ReadBody(body.into_reader()),
    })
}
fn request_failed() -> io::Error {
    io::Error::other("HTTPS request failed")
}
fn invalid_header() -> io::Error {
    io::Error::other("invalid HTTP header")
}

#[cfg(test)]
mod tests;
