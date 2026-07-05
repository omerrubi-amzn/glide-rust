# Valkey GLIDE for Rust (`glide`)

[![CI](https://github.com/omerrubi-amzn/glide-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/omerrubi-amzn/glide-rust/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

A first-class, native **Rust** client for [Valkey](https://valkey.io) and Redis OSS,
built directly on the shared **`glide-core`** engine that powers the official
GLIDE clients for Python, Java, Node, and Go.

Because `glide-core` is itself written in Rust, this wrapper links to it
**directly** — no FFI, no socket bridge — making it the thinnest and fastest
GLIDE binding.

## Highlights

- **Async first** — `GlideClient` (standalone) and `GlideClusterClient` (cluster)
  built on Tokio.
- **Blocking API** — a `sync` layer mirrors the async surface for non-async code
  (enabled by the default `sync` feature).
- **Broad command coverage** — typed methods across strings, generic/key, hash,
  list, set, sorted-set, HyperLogLog, bitmap, geo, stream, scripting,
  connection- and server-management, plus batches/transactions.
- **`custom_command` escape hatch** — run *any* command (with optional cluster
  routing) even where a typed wrapper is not provided, guaranteeing 100%
  functional coverage.
- **Batching** — pipelines and `MULTI`/`EXEC` transactions with configurable
  [`BatchOptions`] (per-batch timeout and pipeline retry strategy).
- **Dynamic authentication** — rotate the connection password at runtime with
  `update_connection_password`, or use **AWS IAM** auth (ElastiCache / MemoryDB)
  via `ServerCredentials::iam`.
- **Runtime Pub/Sub** — `subscribe`/`psubscribe`/`ssubscribe` (and the matching
  unsubscribes) in addition to connect-time subscriptions; messages arrive via
  `get_pubsub_message`.
- **OpenTelemetry** — export traces and metrics via the `glide::telemetry`
  module (gRPC / HTTP / file exporters).
- **Feature parity with the Python GLIDE wrapper** as the baseline for both the
  API surface and the test suite.

## Installation

### Prerequisites

- **Rust 1.85+** (the crate and `glide-core` use edition 2024).
- **Network access on the first build** — the crate links `glide-core` and its
  vendored `redis-rs` as pinned **git** dependencies, which Cargo fetches
  automatically (no monorepo checkout needed; see [Status & publishing](#status--publishing)).
- A running **Valkey** (or Redis OSS) server to connect to — e.g.
  `valkey-server` locally, `docker run -p 6379:6379 valkey/valkey`, or an
  ElastiCache/MemoryDB endpoint.

### 1. Add the dependency

The crate is not yet on crates.io (see [Status & publishing](#status--publishing)),
so depend on it via git. The package is named `glide-rust` and the library is
imported as `glide`:

```toml
# Cargo.toml
[dependencies]
glide-rust = { git = "https://github.com/omerrubi-amzn/glide-rust", branch = "main" }
# Async runtime (the async client is built on Tokio):
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Pin to a specific commit for reproducible builds with `rev = "<sha>"` instead of
`branch = "main"`.

### 2. Provide the required build-time environment (important)

The vendored `redis-rs` fork reads two variables **at compile time** (via
`env!`), so *any* crate that builds it — including yours — must define them, or
the build fails with `environment variable GLIDE_VERSION not defined`. Add a
`.cargo/config.toml` at your project (or workspace) root:

```toml
# .cargo/config.toml
[env]
GLIDE_NAME = "GlideRust"
GLIDE_VERSION = "0.1.0"
# Optional: avoids an aws-lc-rs CPU-jitter-entropy connection-latency regression.
AWS_LC_SYS_NO_JITTER_ENTROPY = "1"
```

(These identify the client library/version reported to the server on the
connection handshake.)

### 3. Build

```bash
cargo build
```

The first build fetches and compiles `glide-core` and its dependency tree, so it
takes a few minutes; subsequent builds are incremental.

### Contributor setup

Cloning this repository to develop the client? See **[DEVELOPER.md](./DEVELOPER.md)**
for the full workflow (build, run the unit + live integration tests — which spawn
a `valkey-server`, set `VALKEY_SERVER_PATH` to point at your binary — lint,
coverage, and benchmarks).

## Quick start (async)

```rust,no_run
use glide::{GlideClient, GlideClientConfiguration};
use glide::StringCommands; // brings command methods into scope

#[tokio::main]
async fn main() -> glide::Result<()> {
    let config = GlideClientConfiguration::with_address("localhost", 6379);
    let client = GlideClient::connect(config).await?;

    client.set("hello", "world").await?;
    let value = client.get("hello").await?;
    assert_eq!(value.as_deref(), Some(&b"world"[..]));
    Ok(())
}
```

## Quick start (sync)

```rust,no_run
use glide::sync::SyncGlideClient;
use glide::{GlideClientConfiguration, StringCommands};

fn main() -> glide::Result<()> {
    let client = SyncGlideClient::connect(
        GlideClientConfiguration::with_address("localhost", 6379),
    )?;
    client.set("hello", "world")?;
    let value = client.get("hello")?;
    assert_eq!(value.as_deref(), Some(&b"world"[..]));
    Ok(())
}
```

## Cluster & routing

```rust,no_run
use glide::{GlideClusterClient, GlideClusterClientConfiguration, Route, CustomCommand};

# async fn demo() -> glide::Result<()> {
let client = GlideClusterClient::connect(
    GlideClusterClientConfiguration::with_address("localhost", 7000),
).await?;

// Broadcast PING to all primaries.
client.custom_command_with_route(&["PING"], Route::AllPrimaries).await?;
# Ok(()) }
```

See `DESIGN.md` for architecture, and `DEVELOPER.md` for how to
build, test, and benchmark.

## Testing

The suite has three layers (all run in CI and are currently green):

- **Unit tests (server-free, ~300)** — pure logic with no server: config →
  `ConnectionRequest` lowering, route → `RoutingInfo` mapping, option/argument
  encoding, value conversion, and error mapping; plus a **command-family mock
  suite** that drives every typed command through an in-process executor to
  assert exact **request encoding** and **response decoding**.
- **Integration tests (live server, ~415 executions across 22 files)** — real
  round-trips against a spawned `valkey-server`, one `tests/it_<family>.rs` per
  command family with edge/error cases (wrong-type, missing key, bounds, expiry
  conditions), **parametrized over RESP2 and RESP3**, plus suites for batches,
  scan, pub/sub, auth, TLS, and a **native multi-shard cluster** harness. Each
  test boots its own ephemeral server and tears it down on drop; suites needing
  unavailable infra (cluster/TLS/auth) **skip gracefully** rather than fail.
- **Doctests** — the examples in this README and the API docs are compiled.

```bash
cargo test --lib     # fast unit + mock tests only (no server needed)
cargo test           # everything, incl. live integration tests + doctests
```

Integration tests auto-discover a `valkey-server`/`redis-server` on `PATH`; point
them at a specific binary with `VALKEY_SERVER_PATH=/path/to/valkey-server`. See
`DEVELOPER.md` for coverage and benchmarks.

## Status & publishing

This crate links `glide-core` and its vendored `redis-rs` via **git ("remote")
dependencies** pinned to a commit of the canonical `valkey-io/valkey-glide`
repository (see `DEVELOPER.md`). Consequences to be aware of:

- **Builds fetch the dependency automatically** — no local monorepo checkout is
  required; you just need network access to GitHub on the first build.
- **Not yet publishable to crates.io as-is** — `cargo publish` rejects **both**
  git and path dependencies, so the crate cannot be published while it links
  `glide-core` (and the vendored redis-rs fork) from git. The only route to
  crates.io is to **publish `glide-core` and the redis-rs fork to crates.io and
  switch these to versioned dependencies** (`glide-core = "x.y"`). A git
  dependency lets downstreams consume this crate straight from its repo, but does
  not itself enable a crates.io publish. This is an inherent consequence of the
  "link core directly, no FFI" design and is a deliberate release-time decision.
- `docs.rs` builds would likewise need the dependency strategy resolved first.

## License

Licensed under the Apache License, Version 2.0. See the `LICENSE` file at the
repository root for the full text.
