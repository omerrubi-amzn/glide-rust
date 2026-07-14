// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! redis-rs interoperability for the async clients.
//!
//! * [`redis::aio::ConnectionLike`] impls make the clients usable everywhere
//!   redis-rs expects a connection object: `Pipeline::query_async`, the scan
//!   iterators, raw `cmd().query_async()`, and generic code bounded on the
//!   literal fork traits (`glide::redis::AsyncCommands`).
//! * The [`crate::commands::core::AsyncCommands`] impls provide the unified
//!   command API's owned-send dispatch (no per-call `Cmd` clone).

use super::{GlideClient, GlideClusterClient};
use glide_core::client::Client as CoreClient;
use redis::{Cmd, Value};

// ---- redis-rs API compatibility ---------------------------------------------
//
// The vendored redis-rs fork blanket-implements its entire typed API over
// `redis::aio::ConnectionLike`:
//
//     impl<T> AsyncCommands for T where T: crate::aio::ConnectionLike + Send + Sized {}
//
// Implementing that trait directly on our clients makes them **first-class
// redis-rs connection objects**: `redis::AsyncCommands` (every typed method,
// generic over `RV: FromRedisValue`, returning `RedisResult<RV>`),
// `Pipeline::query_async` (pipelined and MULTI/EXEC atomic), and the `scan*`
// async iterators all work on `GlideClient` / `GlideClusterClient` as-is —
// while every request is still executed by glide-core (multiplexing, cluster
// routing, reconnection, IAM/password refresh, timeouts).
//
// Note: our native command traits (`StringCommands`, ...) and redis-rs's
// `AsyncCommands` share method names (`get`, `set`, ...). Import only the
// trait family you use in a given scope; if both are imported, disambiguate
// with fully-qualified syntax.

/// Dispatch a redis-rs `Pipeline` through glide-core, matching the reply shape
/// `Pipeline::query_async` expects from `req_packed_commands`: one reply per
/// command for pipelines, and the single `EXEC` reply for atomic transactions.
async fn compat_pipeline(
    core: &mut CoreClient,
    pipeline: &redis::Pipeline,
    retry: Option<redis::PipelineRetryStrategy>,
) -> redis::RedisResult<Vec<Value>> {
    if pipeline.is_atomic() {
        let value = core.send_transaction(pipeline, None, None, true).await?;
        Ok(vec![value])
    } else {
        let value = core
            .send_pipeline(pipeline, None, true, None, retry.unwrap_or_default())
            .await?;
        match value {
            Value::Array(items) => Ok(items),
            // glide-core contracts an array of per-command replies for
            // pipelines; anything else means the contract was violated —
            // fail loudly rather than let the caller decode garbage.
            other => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "unexpected non-array pipeline reply from glide-core",
                format!("{other:?}"),
            ))),
        }
    }
}

impl redis::aio::ConnectionLike for GlideClient {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> redis::RedisFuture<'a, Value> {
        Box::pin(async move {
            // `send_command` needs `&mut Cmd` (compression / pubsub
            // interception may rewrite it); the trait hands us `&Cmd`, so a
            // clone (one arg-buffer copy) per typed call is unavoidable
            // without a glide-core `&Cmd` send path. Benchmarked: not
            // measurable against the network round-trip (~40 µs loopback).
            let mut cmd = cmd.clone();
            self.inner.send_command(&mut cmd, None).await
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        // `offset`/`count` describe the packed-bytes reply layout; the only
        // fork-internal caller is `Pipeline::query_async`, and glide-core
        // already returns exactly the contracted shape (one reply per
        // command, or the single EXEC reply), so they are not needed here.
        // The *sync* impl does use them (transaction detection).
        _offset: usize,
        _count: usize,
        pipeline_retry_strategy: Option<redis::PipelineRetryStrategy>,
    ) -> redis::RedisFuture<'a, Vec<Value>> {
        Box::pin(compat_pipeline(
            &mut self.inner,
            cmd,
            pipeline_retry_strategy,
        ))
    }

    fn get_db(&self) -> i64 {
        self.db
    }

    fn is_closed(&self) -> bool {
        // glide-core owns reconnection; the client is never observably
        // "closed", matching a managed (auto-reconnecting) connection.
        false
    }
}

impl redis::aio::ConnectionLike for GlideClusterClient {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> redis::RedisFuture<'a, Value> {
        Box::pin(async move {
            let mut cmd = cmd.clone();
            // Routing is decided by glide-core from the command's keys,
            // like redis-rs's `ClusterConnection`.
            self.inner.send_command(&mut cmd, None).await
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        _offset: usize,
        _count: usize,
        pipeline_retry_strategy: Option<redis::PipelineRetryStrategy>,
    ) -> redis::RedisFuture<'a, Vec<Value>> {
        Box::pin(compat_pipeline(
            &mut self.inner,
            cmd,
            pipeline_retry_strategy,
        ))
    }

    fn get_db(&self) -> i64 {
        0 // Cluster deployments always use database 0.
    }

    fn is_closed(&self) -> bool {
        false
    }
}

// ---- owned-send compat traits (native copy behavior) --------------------------
//
// `crate::AsyncCommands` methods build the `Cmd` themselves and hand it here
// **by value** — unlike the `ConnectionLike` path above, no `Cmd` clone is
// needed, so a typed compat call copies the payload exactly as many times as
// the native GLIDE API (build + glide-core's internal owned copy).

impl crate::commands::core::AsyncCommands for GlideClient {
    fn glide_send_owned<'a>(&'a self, mut cmd: Cmd) -> redis::RedisFuture<'a, Value> {
        // `Client` is Clone (Arc inside); operate on a cheap clone so the
        // unified API can take `&self` — same pattern as `execute_command`.
        let mut client = self.inner.clone();
        Box::pin(async move { client.send_command(&mut cmd, None).await })
    }
}

impl crate::commands::core::AsyncCommands for GlideClusterClient {
    fn glide_send_owned<'a>(&'a self, mut cmd: Cmd) -> redis::RedisFuture<'a, Value> {
        // Routing is decided by glide-core from the command's keys.
        let mut client = self.inner.clone();
        Box::pin(async move { client.send_command(&mut cmd, None).await })
    }
}
