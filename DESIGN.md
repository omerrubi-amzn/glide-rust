# DESIGN — `glide-rs`

## Dependency strategy
The crate declares **git ("remote") dependencies** on both `glide-core` and its
*vendored* `redis` (redis-rs fork), pinned to the same commit of the canonical
`valkey-io/valkey-glide` repository:

```toml
glide-core = { git = "https://github.com/valkey-io/valkey-glide", rev = "..." }
redis      = { git = "https://github.com/valkey-io/valkey-glide", package = "redis", rev = "...", features = [
    "aio", "tokio-comp", "cluster", "cluster-async",
] }
```

Because both point at the **same git source + rev**, Cargo unifies our
`redis::Cmd` / `redis::Value` with the exact types `glide-core` expects — no type
mismatch, no re-wrapping — and consumers don't need a local monorepo checkout.
(Previously these were local `path` deps; the git form removes the
sibling-checkout requirement but still does not enable a crates.io publish.)

## Dispatch seam — `CommandExecutor`
```rust
#[async_trait]
pub trait CommandExecutor: Send + Sync {
    async fn execute_command(&self, cmd: Cmd, routing: Option<RoutingInfo>) -> Result<Value>;
}
```
`glide_core::client::Client` is `Clone` (internally `Arc<RwLock<..>>`), and
`send_command` needs `&mut self`. So `execute_command` clones the inner client
(cheap Arc clone) and calls `send_command` on the clone. This matches exactly
what every other wrapper does.

`GlideClient` (standalone) and `GlideClusterClient` (cluster) both hold a
`glide_core::client::Client` and implement `CommandExecutor`. The cluster client
additionally accepts a `Route` on command variants (via dedicated
`*_with_route` helpers and `custom_command` routing).

## Command families
Each family is an **extension trait** with a blanket impl:
```rust
#[async_trait]
pub trait StringCommands: CommandExecutor {
    async fn get<K: ToRedisArgs + Send>(&self, key: K) -> Result<Option<Bytes>> { .. }
    async fn set<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(&self, k: K, v: V) -> Result<()> { .. }
    // ...
}
impl<T: CommandExecutor + ?Sized> StringCommands for T {}
```
Benefits: the async client, and any future executor (e.g. a routed handle), gets
every command for free; families live in isolated files (parallel-friendly).

- **Arguments**: generic over `redis::ToRedisArgs` — accepts `&str`, `String`,
  `&[u8]`, `Vec<u8>`, integers, floats, slices, etc. Mirrors Python `TEncodable`.
- **Returns**: typed via `redis::FromRedisValue` plus small hand conversions
  (e.g. `Option<Bytes>`, `HashMap<String,Bytes>`, `f64`, bool-from-int).
- **Options**: dedicated structs/enums (`SetOptions`, `ExpirySet`, `ExpireOptions`,
  `ScoreBound`, ...) each with an `add_to(&self, &mut Cmd)` method. Mirrors the
  Python option classes.

## Value conversion
`value` module provides helpers: `Value -> Option<Bytes>`, `-> String`, `-> i64`,
`-> f64`, `-> bool`, `-> Vec<T>`, `-> HashMap<..>`. Built on `FromRedisValue`
where possible, with Glide-specific handling for `Value::Nil`, `Value::Okay`,
and RESP3 maps/doubles/booleans (glide-core already converts many types).

## Routing (cluster)
`routes::Route` enum → `redis::cluster_routing::RoutingInfo`:
`AllNodes`, `AllPrimaries`, `RandomNode`, `SlotKey{key,type}`,
`SlotId{id,type}`, `ByAddress{host,port}`.

## Errors
`GlideError` enum mirrors the Python exception hierarchy:
`Connection`, `Timeout`, `ExecAbort`, `Request`, `Closing`, `Configuration`,
`CircuitBreaker`. Converts from `redis::RedisError` (by `ErrorKind`) and
`glide_core::client::ConnectionError`.

## Sync layer
`sync::SyncGlideClient` / `sync::SyncGlideClusterClient` own an async client and a
shared multi-thread `tokio::runtime::Runtime` (lazily created, process-wide), and
expose the same methods with `block_on`. Mirrors Python `glide-sync`.

## Batch / Transaction
`Batch::new(is_atomic)` collects `Cmd`s into a `redis::Pipeline`; the client
executes via `send_transaction` (atomic) or `send_pipeline` (non-atomic) and maps
the returned `Value::Array` into `Vec<Value>`.
