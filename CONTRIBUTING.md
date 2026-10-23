Use a feature branch and a pull request with a Conventional Commit title.
Keep the default library `no_std + alloc`, free of unsafe code and runtime/TLS
dependencies. Hosted adapters belong behind optional features.

Run the checks in README.md before submitting changes. Protocol changes need
tests for interrupted transfers and malformed responses, with links to Google's
API contract. Timing benchmarks are informational; correctness and bounded
payload memory should be checked deterministically.

Do not put access tokens, resumable session URLs or private Drive content in
fixtures, issues or logs. Use the dummy offline transports for tests.
