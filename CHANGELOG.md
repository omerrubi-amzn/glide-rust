# Changelog

All notable changes to this crate are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
