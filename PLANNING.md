# PLANNING — Valkey GLIDE Rust wrapper (`glide-rs`)

## Goal
A first-class **Rust** client for Valkey/Redis OSS built directly on the shared
`glide-core` Rust crate (no FFI needed — Rust talks to Rust). Feature parity with
the **Python** GLIDE wrapper (`glide-async` + `glide-sync`) as the baseline, for
both the API surface and the test suite. Async + sync clients, comprehensive
unit + integration tests, and validated performance.

## Why this is different from the other wrappers
Python / Java / Node / Go all wrap `glide-core` through the FFI / socket layer
because they are not Rust. A Rust wrapper links `glide-core` **directly**:
`glide_core::client::Client` is the exact same object those FFI layers drive, so
the Rust wrapper is the thinnest, fastest possible binding.

## Architecture (see DESIGN.md for detail)
- `config`    — `NodeAddress`, `GlideClientConfiguration`,
  `GlideClusterClientConfiguration`, TLS/auth/read-from/retry → builds
  `glide_core::client::ConnectionRequest`.
- `client`    — `GlideClient` (standalone) and `GlideClusterClient` (cluster),
  async, both wrap `glide_core::client::Client`.
- `executor`  — `CommandExecutor` async trait: the single dispatch seam
  (`send_command`). Everything else is built on top.
- `commands`  — one module per command family; each is an extension trait with a
  blanket impl over `CommandExecutor`. Mirrors Python method names.
- `batch`     — `Batch`/`Transaction` (pipeline + MULTI/EXEC).
- `sync`      — blocking client that wraps the async client on a shared runtime,
  mirroring Python `glide-sync`.
- `custom_command` — escape hatch guaranteeing 100% *functional* command
  coverage even where a typed wrapper is not (yet) hand-written.

## Milestones
1. [x] Investigate `glide-core` public API + Python baseline + tooling.
2. [x] Scaffold repo, Cargo manifest (path deps on `glide-core` + vendored `redis`).
3. [x] Config + connection layer (standalone + cluster).
4. [x] Command families (string, generic, hash, list, set, sorted-set,
   hyperloglog, bitmap, geo, stream, connection-mgmt, server-mgmt, scripting,
   pubsub) + custom_command.
5. [x] Sync client wrapper.
6. [x] Batch / transaction support.
7. [x] Unit tests (config building, routing, option encoding) — Python parity.
8. [x] Integration tests per family against a live `valkey-server`.
9. [x] Benchmarks (latency + throughput).
10. [x] Build + clippy + full test-suite green; docs; final summary.

## Test strategy
- Unit tests: pure, no server. Validate that config → `ConnectionRequest` maps
  correctly, routing enums map to `RoutingInfo`, and option structs encode to the
  right argument vectors. Parity with `python/tests/test_config.py`,
  `test_options.py`, `test_routes` concepts.
- Integration tests: spawn a real `valkey-server` (binary discovered on disk),
  one test module per command family, asserting real round-trips. Parity subset
  of `python/tests/async_tests` + `sync_tests`.

## Server for integration tests
`valkey-server` binaries exist under
`/local/home/omerrubi/ECRedis/.../oss-valkey-releases/8.1.3/valkey-server`.
A small harness boots an ephemeral standalone server on a free port.
