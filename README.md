# Valkey GLIDE for Rust (`glide`)

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
- **Feature parity with the Python GLIDE wrapper** as the baseline for both the
  API surface and the test suite.

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

See `DESIGN.md` and `PLANNING.md` for architecture, and `DEVELOPER.md` for how to
build, test, and benchmark.

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
