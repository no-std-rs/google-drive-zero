use crate::{
    transport::{Body, Response},
    Error, InvalidInput,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
}
impl ByteRange {
    pub fn new(offset: u64, length: u64) -> Result<Self, InvalidInput> {
        let end = length
            .checked_sub(1)
            .and_then(|n| offset.checked_add(n))
            .ok_or(InvalidInput("range must be nonempty and fit in u64"))?;
        Ok(Self { start: offset, end })
    }
    pub fn offset(self) -> u64 {
        self.start
    }
    pub fn length(self) -> u64 {
        self.end - self.start + 1
    }
}

/// Read through EOF before publishing a cache entry. Dropping cancels the body;
/// partial bytes must not be marked as a complete cached chunk.
pub struct Download<B> {
    body: B,
    total_size: u64,
    length: u64,
    remaining: u64,
    finished: bool,
    failed: bool,
}
impl<B: Body> Download<B> {
    pub(crate) fn new(
        response: Response<B>,
        requested: ByteRange,
    ) -> Result<Self, Error<B::Error>> {
        if response.status != 206 {
            return Err(Error::Http(response.status));
        }
        let encoding = response
            .header("content-encoding")
            .map_err(Error::InvalidResponse)?;
        if encoding.is_some_and(|value| !value.eq_ignore_ascii_case("identity")) {
            return Err(Error::InvalidResponse("encoded range body"));
        }
        let value = response
            .header("content-range")
            .map_err(Error::InvalidResponse)?
            .ok_or(Error::InvalidResponse("missing Content-Range"))?;
        let (span, total) = value
            .strip_prefix("bytes ")
            .and_then(|v| v.split_once('/'))
            .ok_or(Error::InvalidResponse("malformed Content-Range"))?;
        let (start, end) = span
            .split_once('-')
            .ok_or(Error::InvalidResponse("malformed Content-Range"))?;
        let parse = |v: &str| -> Result<u64, Error<B::Error>> {
            if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
                return Err(Error::InvalidResponse("invalid range integer"));
            }
            v.parse()
                .map_err(|_| Error::InvalidResponse("range integer overflow"))
        };
        let (start, end, total) = (parse(start)?, parse(end)?, parse(total)?);
        if total == 0
            || start != requested.start
            || start >= total
            || end != requested.end.min(total - 1)
        {
            return Err(Error::InvalidResponse(
                "Content-Range does not match requested bytes",
            ));
        }
        let length = end - start + 1;
        if let Some(value) = response
            .header("content-length")
            .map_err(Error::InvalidResponse)?
        {
            if parse(value)? != length {
                return Err(Error::InvalidResponse(
                    "Content-Length does not match range",
                ));
            }
        }
        Ok(Self {
            body: response.body,
            total_size: total,
            length,
            remaining: length,
            finished: false,
            failed: false,
        })
    }
    pub fn total_size(&self) -> u64 {
        self.total_size
    }
    pub fn length(&self) -> u64 {
        self.length
    }
    pub fn remaining(&self) -> u64 {
        self.remaining
    }
    pub async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error<B::Error>> {
        if self.failed {
            return Err(Error::InvalidResponse("download previously failed"));
        }
        if buffer.is_empty() || self.finished {
            return Ok(0);
        }
        self.failed = true;
        let limit = self.remaining.min(buffer.len() as u64) as usize;
        let n = self
            .body
            .read(&mut buffer[..limit])
            .await
            .map_err(Error::Transport)?;
        if n == 0 {
            return Err(Error::InvalidResponse("truncated range body"));
        }
        self.remaining -= n as u64;
        if self.remaining == 0 {
            let mut extra = [0];
            if self.body.read(&mut extra).await.map_err(Error::Transport)? != 0 {
                return Err(Error::InvalidResponse("range body exceeds declared length"));
            }
            self.finished = true;
        }
        self.failed = false;
        Ok(n)
    }
}
