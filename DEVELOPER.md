# DEVELOPER guide — `glide-rs`

## Prerequisites
- Rust 1.85+ (edition 2024; developed on 1.95). No MSRV is declared, matching the
  upstream valkey-glide Rust crates.
- The crate depends on `glide-core` and its vendored `redis-rs` via **git
  ("remote") dependencies** pinned to a specific commit of
  `github.com/valkey-io/valkey-glide` in `Cargo.toml`, so Cargo fetches them
  automatically — **no local monorepo checkout is required**. You do need network
  access to GitHub on the first build (Cargo caches it afterwards). To build
  against a different revision, update the `rev = "..."` in `Cargo.toml`.
- A `valkey-server` (or `redis-server`) binary for integration tests / benches.
  The harness auto-discovers one on `PATH`; override with:
  ```bash
  export VALKEY_SERVER_PATH=/path/to/valkey-server
  ```

## Required build environment
The vendored `redis-rs` reads two variables at **compile time** (via `env!`).
They are provided by `.cargo/config.toml` in this repo, so a plain `cargo build`
works out of the box:
```toml
[env]
GLIDE_NAME = { value = "GlideRust", force = true }
GLIDE_VERSION = "unknown"   # override at build time: `GLIDE_VERSION=1.2.3 cargo build`
AWS_LC_SYS_NO_JITTER_ENTROPY = "1"
```

## Build
```bash
cargo build            # debug
cargo build --release  # optimized
```

## Test
```bash
cargo test --lib              # fast, pure unit tests (no server)
cargo test --test it_string   # a single live integration suite (spawns valkey-server)
cargo test                    # everything, incl. doctests
```
Integration tests each boot their own ephemeral server on a free port and tear it
down on drop. When no server binary is found, they print `SKIP` and pass.

## Lint & format
```bash
cargo clippy --all-targets
cargo fmt
```

## Benchmarks
```bash
cargo bench
```
Prints a manual throughput probe (ops/sec at several concurrency levels) and runs
Criterion latency benchmarks for `SET`/`GET`/`INCR`.

## Layout
```
src/
  lib.rs          crate root + public re-exports
  error.rs        GlideError (mirrors Python exceptions)
  config.rs       client configuration -> glide_core ConnectionRequest
  routes.rs       cluster routing (Route -> RoutingInfo)
  value.rs        redis::Value -> typed Rust conversions (RESP2 + RESP3)
  executor.rs     CommandExecutor seam + custom_command
  client.rs       GlideClient / GlideClusterClient (async)
  batch.rs        Batch / transaction
  sync/mod.rs     blocking clients over a shared runtime
  commands/       one module per command family (blanket-impl traits)
tests/
  common/mod.rs   ephemeral server + cluster harness
  it_*.rs         per-family live tests (one file per command family)
benches/
  throughput.rs   latency + throughput
```

## Adding a command
1. Pick the family module in `src/commands/`.
2. Add an `async fn` to that family's trait following the template in
   `string.rs`: build a `redis::Cmd`, call `self.execute_command(cmd, None)`,
   convert with a `crate::value::*` helper.
3. Add an integration test in the family's `tests/it_<family>.rs` (use the
   `resp_test!` macro for RESP2/RESP3 coverage), and a server-free encoding test
   in `src/command_mock/<family>.rs`.
4. `cargo test && cargo clippy --all-targets`.

## Extending value conversion
Because the client negotiates **RESP3** by default, replies may arrive as
`Value::Map`, `Value::Double`, `Value::Boolean`, or `Value::VerbatimString`.
Prefer the helpers in `src/value.rs`, which already normalize these, and add new
shapes there rather than in individual commands.

## Regenerating `compat_commands.rs`

`src/compat_commands.rs` is **generated** — do not hand-edit it. It provides
the redis-rs-compatible `AsyncCommands` / `Commands` traits (owned-send, native
copy behavior) derived from the vendored redis-rs fork's `implement_commands!`
table. Regenerate it **whenever the pinned fork rev changes**:

```bash
# Resolves the fork's commands/mod.rs via `cargo metadata` (no hardcoded path)
# and formats the output with rustfmt --edition 2024:
python3 tools/gen_compat_commands.py
cargo fmt --check   # must be clean; the generator already runs rustfmt
cargo test          # the compat_commands_matches_fork drift test must pass
```

The generator asserts the fork exposes exactly **151** methods; if that
assertion fires after a rev bump, the fork's command surface changed — review
the delta, update the count in the generator deliberately, and refresh the
copy-parity docs and the pinned rev references (`PARITY.md`, `licenses/`,
`NOTICE`). The `compat_commands_matches_fork` test (in `tests/it_compat_gen.rs`)
re-runs the generator and diffs against the committed file, skipping gracefully
when the fork checkout is unavailable.
