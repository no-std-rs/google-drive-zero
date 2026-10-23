# Local streaming benchmark

Measured on 2026-10-23, Linux x86_64, AMD Ryzen 7 7840HS, Rust 1.95.0,
release profile, ureq 3.5.0. Reproduce with:

```sh
cargo test --release --locked --features ureq --lib loopback_throughput -- --ignored --nocapture
```

Ten rounds transfer 512 MiB in each direction using the client and direct ureq,
alternating the order. The receiver validates every payload byte. Both paths use
the same ureq settings and generated input; download consumers and the server
use 64 KiB buffers. Upload sources fill the transport's requested buffers.
Initialization is outside the upload timer. The fixture closes each HTTP
connection, so these numbers do not evaluate connection pooling or TLS reuse.

| Direction | Client median | Direct ureq median | Client / direct throughput |
| --- | ---: | ---: | ---: |
| Upload | 2779.6 MiB/s | 2773.5 MiB/s | 1.002 |
| Download | 2250.9 MiB/s | 2273.1 MiB/s | 0.990 |

The small upload advantage is measurement noise, not an acceleration claim.
Earlier shorter 128 MiB runs varied substantially (download ratios 0.664 and
0.952); use the longer run rather than interpreting a single short timing.
There is no CI throughput threshold on shared machines.

These are loopback HTTP results. They exclude TLS encryption, real network
latency, Drive service throughput, quotas and retry costs. No live Drive account
was used. End-to-end cloud performance remains unmeasured.

The deterministic suite separately streams a generated 32 MiB+7 payload in one
content request, checks its full rolling checksum, limits source reads to an
8 KiB transport buffer, and verifies the following source byte remains unread.
Payload memory stays bounded by caller/transport buffers; metadata, URLs and
headers allocate independently of file size. The core does not allocate per
payload read or accumulate the complete file. Chunk uploads borrow their input.
