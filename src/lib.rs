// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod batch;
pub mod client;
pub mod commands;
pub mod config;
pub mod error;
pub mod executor;
pub mod routes;
pub mod telemetry;
pub mod value;

#[cfg(feature = "sync")]
pub mod sync;

#[cfg(test)]
mod command_mock;

// ---- Primary public API re-exports (mirror Python's top-level `glide` package) ----

pub use batch::{Batch, BatchOptions};
pub use client::{
    ClusterScanCursor, GlideClient, GlideClusterClient, PubSubMessage, PubSubMessageKind,
};
pub use error::{GlideError, Result};
pub use executor::{CommandExecutor, CustomCommand};
pub use routes::{Route, SlotType};

pub use config::{
    BackoffStrategy, GlideClientConfiguration, GlideClusterClientConfiguration, IamAuthConfig,
    NodeAddress, NodeDiscoveryMode, PeriodicChecks, ProtocolVersion, PubSubChannelMode,
    PubSubSubscriptions, ReadFrom, ServerCredentials, ServiceType, TlsConfig,
};

/// All command traits in one import.
pub use commands::prelude::*;

/// All shared option types.
pub use commands::options::{
    ClientPauseMode, ConditionalChange, ExpireOptions, ExpirySet, FlushMode, FunctionRestorePolicy,
    HashFieldConditionalChange, InsertPosition, Limit, ListDirection, MigrateOptions, ObjectType,
    OrderBy, RestoreOptions, SetOptions, UpdateOptions,
};

/// Family-specific option/type re-exports.
pub use commands::bitmap::{
    BitEncoding, BitFieldOffset, BitFieldSubcommand, BitOverflow, BitmapIndexType, BitwiseOperation,
};
pub use commands::geo::{GeoSearchShape, GeoUnit, GeospatialData};
pub use commands::sorted_set::{AggregationType, LexBound, ScoreBound, ScoreFilter, ZAddOptions};
pub use commands::stream::{
    PendingConsumer, StreamAddOptions, StreamClaimOptions, StreamEntry, StreamGroupCreateOptions,
    StreamReadGroupOptions, StreamReadOptions, StreamTrimOptions, StreamTrimStrategy,
    XPendingEntry, XPendingSummary,
};

/// Re-export the underlying `redis` value type for advanced use.
pub use redis::Value;

/// Re-export `bytes::Bytes` — the byte-string type returned by binary-safe commands.
pub use bytes::Bytes;
