# Changelog

All notable changes to this crate are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## 0.2.0 (unreleased)

**Breaking — unified command API.** One command surface instead of two:

- `glide::AsyncCommands` / `glide::Commands` are now THE command API:
  GLIDE's own traits, source-compatible with redis-rs (names, generic order,
  and wire encoding match the vendored fork; existing redis-rs call sites
  compile unchanged), sent **by value** on the native zero-extra-copy path.
  Deliberate deviations: `&self` receivers; `RedisResult` errors.
- The duplicated native core command traits (string/hash/list/set/sorted-set/
  generic/bitmap/HyperLogLog) were **removed** where redis-rs covers the
  command. GLIDE-only commands remain as extension traits with concrete
  return types (streams, geo, `FT.*`, `JSON.*`, Pub/Sub, scripting/functions,
  server & connection management, hash field-TTL, `LCS`, `SINTERCARD`,
  `ZRANGESTORE`, `BITFIELD`, `SORT`, `DUMP`/`RESTORE`, `COPY`, …).
- `Batch`/`BatchOptions` **removed**: use `glide::pipe()` +
  `Pipeline::query_async` / `PipelineExt::query_glide`, or
  `execute_pipeline(&Pipeline, raise_on_error, &PipelineOptions)` for GLIDE
  execution controls (timeout, retry policy, cluster routing).
- redis-rs migration ergonomics: `from_url`/`from_connection_info`/`from_urls`,
  mutual TLS (`client_identity`), `Script` with `EVALSHA` caching, whole-crate
  `glide::redis` re-export, `ConnectionLike` implemented on all clients.
- No performance regression: typed calls ride the owned-send path (measured at
  native copy count; see DESIGN.md).

## [Unreleased]

### Added — Python-parity feature gaps

- **Dynamic password management**: `GlideClient::update_connection_password` /
  `GlideClusterClient::update_connection_password` (and blocking mirrors on the
  sync clients) to rotate the client's authentication password at runtime, with
  optional immediate re-`AUTH`.
- **AWS IAM authentication**: `ServerCredentials::iam(username, IamAuthConfig)`
  plus the new `IamAuthConfig` and `ServiceType` (`ElastiCache` / `MemoryDB`)
  types, lowered into the core `AuthenticationInfo`. `ServerCredentials.password`
  is now `Option<String>` (IAM-only credentials carry no static password), and
  `ServerCredentials`'s `Debug` now redacts the password.
- **OpenTelemetry**: a new `glide::telemetry` module exposing
  `OpenTelemetryConfig` (builder), `TelemetryExporter` (`grpc`/`http`/`file`),
  and `init` / `is_initialized` / `shutdown`, wrapping `glide-core`'s
  OpenTelemetry support (traces + metrics).
- **Runtime Pub/Sub**: `subscribe`, `psubscribe`, `ssubscribe`, `unsubscribe`,
  `punsubscribe`, `sunsubscribe` on `PubSubCommands`, plus an `enable_pubsub()`
  configuration opt-in that provisions the push channel without connect-time
  subscriptions. Received messages are delivered through `get_pubsub_message`.
  Note: runtime subscriptions are session-scoped and are not automatically
  restored on reconnect (connect-time subscriptions are).
- **Batch options**: `BatchOptions` (per-batch `timeout`, `retry_server_error`,
  `retry_connection_error`) with `exec_with_options` on both async clients and
  the sync mirrors. `exec` retains the previous default behaviour.
- **Sorted-set store variants**: `zrangestore_by_score` and `zrangestore_by_lex`
  (with `rev` and optional `LIMIT`).
- **Routed function calls**: `fcall_route` and `fcall_ro_route`.

### Changed

- `ServerCredentials.password` changed from `String` to `Option<String>`
  (source-breaking for direct field construction; the `password`/`new`
  constructors are unchanged).
