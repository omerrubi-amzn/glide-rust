// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod client;
pub mod commands;
pub mod config;
pub mod error;
pub mod executor;
pub mod pipeline_options;
pub mod routes;
pub mod script;
pub mod telemetry;
pub mod value;

#[cfg(feature = "sync")]
pub mod sync;

#[cfg(test)]
mod command_mock;

// ---- Primary public API re-exports (mirror Python's top-level `glide` package) ----

pub use client::{
    ClusterScanCursor, GlideClient, GlideClusterClient, PubSubMessage, PubSubMessageKind,
};
pub use error::{GlideError, Result};
pub use executor::{CommandExecutor, CustomCommand};
pub use pipeline_options::PipelineOptions;
pub use routes::{Route, SlotType};

pub use config::{
    BackoffStrategy, ClientIdentity, GlideClientConfiguration, GlideClusterClientConfiguration,
    IamAuthConfig, NodeAddress, NodeDiscoveryMode, PeriodicChecks, ProtocolVersion,
    PubSubChannelMode, PubSubSubscriptions, ReadFrom, ServerCredentials, ServiceType, TlsConfig,
};

/// All command traits in one import.
pub use commands::prelude::*;

/// All shared option types.
pub use commands::options::{
    ClientPauseMode, ConditionalChange, ExpireOptions, ExpirySet, FlushMode, FunctionRestorePolicy,
    HashFieldConditionalChange, Limit, MigrateOptions, ObjectType, OrderBy, RestoreOptions,
};

/// Family-specific option/type re-exports.
pub use commands::bitmap::{
    BitEncoding, BitFieldOffset, BitFieldSubcommand, BitOverflow, BitmapIndexType,
};
pub use commands::geo::{GeoSearchShape, GeoUnit, GeospatialData};
pub use commands::sorted_set::{AggregationType, LexBound, ScoreBound};
pub use commands::stream::{
    PendingConsumer, StreamAddOptions, StreamClaimOptions, StreamEntry, StreamGroupCreateOptions,
    StreamReadGroupOptions, StreamReadOptions, StreamTrimOptions, StreamTrimStrategy,
    XPendingEntry, XPendingSummary,
};

/// Re-export the underlying `redis` value type for advanced use.
pub use redis::Value;

// ---- redis-rs API parity re-exports ----
//
// `GlideClient` / `GlideClusterClient` implement `redis::aio::ConnectionLike`,
// so the full redis-rs typed API works on them directly. Downstream crates
// depend on `glide-rust` only — the vendored `redis` fork is a transitive git
// dependency they cannot name — so re-export everything a redis-rs codebase
// needs:
//
// ```rust,no_run
// use glide::{AsyncCommands, GlideClient, GlideClientConfiguration};
//
// # async fn demo() -> glide::RedisResult<()> {
// # let mut client = GlideClient::connect(GlideClientConfiguration::with_address("localhost", 6379)).await.unwrap();
// client.set::<_, _, ()>("my_key", 42).await?;
// let v: i64 = client.get("my_key").await?;
// # Ok(()) }
// ```

/// The redis-rs typed command trait — GLIDE-owned drop-in with **native copy
/// behavior** (commands sent by value, no per-call `Cmd` clone). Signature-
/// identical to the fork's `redis::AsyncCommands`, which remains available at
/// [`redis::AsyncCommands`] for code needing the
/// literal fork trait.
pub use commands::core::AsyncCommands;
/// The redis-rs **blocking** typed command trait — GLIDE-owned drop-in with
/// native copy behavior (see [`AsyncCommands`]). The fork's trait remains at
/// [`redis::Commands`].
#[cfg(feature = "sync")]
pub use commands::core::Commands;
/// The **whole vendored `redis` crate**, re-exported. Downstream crates cannot
/// name the git-dep fork directly, and the curated flat re-exports above are
/// deliberately incomplete where names collide with other exported types
/// (`redis::SetOptions`, `redis::Expiry`, the
/// `ConnectionLike` traits, `AsyncIter`, …). Everything is reachable as
/// `glide::redis::…` with zero collision risk:
///
/// ```rust,no_run
/// use glide::redis::{AsyncIter, Expiry, SetOptions};
/// ```
///
/// **Semver note:** this makes the fork's API part of this crate's public
/// surface — bumping the pinned fork rev is a breaking change.
pub use redis;
/// redis-rs connection-description types, accepted by
/// [`GlideClientConfiguration::from_connection_info`] and
/// [`GlideClusterClientConfiguration::from_urls`].
pub use redis::{ConnectionAddr, ConnectionInfo, IntoConnectionInfo};
/// Argument types appearing in redis-rs command signatures (`lmpop`, `lpos`, …).
pub use redis::{Direction, LposOptions};
/// redis-rs error and conversion types, for code ported from redis-rs.
pub use redis::{ErrorKind, FromRedisValue, RedisError, RedisResult, ToRedisArgs, cmd};
/// redis-rs pipeline / transaction support (`pipe()`, `Pipeline::query_async`).
pub use redis::{Pipeline, pipe};
/// Lua script helper with the redis-rs `Script` API shape (clean-room impl).
pub use script::{Script, ScriptInvocation};

/// Re-export `bytes::Bytes` — the byte-string type returned by binary-safe commands.
pub use bytes::Bytes;
