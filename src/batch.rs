// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Batches and transactions (pipeline + `MULTI`/`EXEC`).
//!
//! Mirrors Python's `Batch`/`Transaction`. A [`Batch`] queues commands and is
//! executed in a single round-trip via [`crate::GlideClient::exec`] /
//! [`crate::GlideClusterClient::exec`]. When `is_atomic` is set, commands run
//! inside a `MULTI`/`EXEC` transaction.

use crate::error::{GlideError, Result};
use glide_core::client::Client as CoreClient;
use redis::cluster_routing::RoutingInfo;
use redis::{Cmd, Pipeline, PipelineRetryStrategy, ToRedisArgs, Value};
use std::time::Duration;

/// Execution options for a [`Batch`].
///
/// Mirrors Python's `BaseBatchOptions` / `BatchRetryStrategy`. The `retry_*`
/// flags apply only to **non-atomic pipelines**; they are ignored for atomic
/// transactions (a `MULTI`/`EXEC` is never partially retried).
///
/// Retrying is only safe for idempotent commands — enabling it for a pipeline
/// that contains non-idempotent commands (e.g. `INCR`, `LPUSH`) may apply them
/// more than once on a reconnect. Both flags default to `false`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchOptions {
    /// Per-batch timeout. `None` uses the client's configured request timeout.
    pub timeout: Option<Duration>,
    /// Retry the pipeline if the server returns a retryable error (pipeline only).
    pub retry_server_error: bool,
    /// Retry the pipeline on a connection error (pipeline only).
    pub retry_connection_error: bool,
}

impl BatchOptions {
    /// Options with no timeout override and retries disabled (the defaults).
    pub fn new() -> Self {
        BatchOptions::default()
    }

    /// Set a per-batch timeout. Builder form.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Enable/disable retrying the pipeline on a retryable server error
    /// (pipeline only). Builder form.
    #[must_use]
    pub fn with_retry_server_error(mut self, retry: bool) -> Self {
        self.retry_server_error = retry;
        self
    }

    /// Enable/disable retrying the pipeline on a connection error (pipeline
    /// only). Builder form.
    #[must_use]
    pub fn with_retry_connection_error(mut self, retry: bool) -> Self {
        self.retry_connection_error = retry;
        self
    }

    /// Timeout as whole milliseconds, saturating at `u32::MAX` (~49.7 days)
    /// instead of narrowing/overflowing.
    fn timeout_millis(&self) -> Option<u32> {
        self.timeout
            .map(|d| u32::try_from(d.as_millis()).unwrap_or(u32::MAX))
    }
}

/// A queued sequence of commands executed together.
///
/// Set `is_atomic` for a transaction (`MULTI`/`EXEC`); otherwise it is a
/// non-atomic pipeline.
#[derive(Clone)]
pub struct Batch {
    pipeline: Pipeline,
    is_atomic: bool,
    len: usize,
}

impl Batch {
    /// Create a new batch. Pass `true` for an atomic transaction.
    pub fn new(is_atomic: bool) -> Self {
        let mut pipeline = Pipeline::new();
        if is_atomic {
            pipeline.atomic();
        }
        Batch {
            pipeline,
            is_atomic,
            len: 0,
        }
    }

    /// Whether this batch runs as an atomic transaction.
    pub fn is_atomic(&self) -> bool {
        self.is_atomic
    }

    /// Number of queued commands.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the batch has no queued commands.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Queue an already-built [`Cmd`].
    pub fn add_cmd(&mut self, cmd: Cmd) -> &mut Self {
        self.pipeline.add_command(cmd);
        self.len += 1;
        self
    }

    /// Queue an arbitrary command from its arguments (the escape hatch — mirrors
    /// `custom_command`). The first argument is the command keyword.
    pub fn command<A: ToRedisArgs>(&mut self, args: &[A]) -> &mut Self {
        let mut cmd = Cmd::new();
        for a in args {
            cmd.arg(a);
        }
        self.add_cmd(cmd)
    }

    // ---- A few common typed conveniences (any command is available via `command`). ----

    /// Queue `GET key`.
    pub fn get<K: ToRedisArgs>(&mut self, key: K) -> &mut Self {
        let mut cmd = Cmd::new();
        cmd.arg("GET").arg(key);
        self.add_cmd(cmd)
    }

    /// Queue `SET key value`.
    pub fn set<K: ToRedisArgs, V: ToRedisArgs>(&mut self, key: K, value: V) -> &mut Self {
        let mut cmd = Cmd::new();
        cmd.arg("SET").arg(key).arg(value);
        self.add_cmd(cmd)
    }

    /// Queue `DEL key...`.
    pub fn del<K: ToRedisArgs>(&mut self, keys: &[K]) -> &mut Self {
        let mut cmd = Cmd::new();
        cmd.arg("DEL");
        for k in keys {
            cmd.arg(k);
        }
        self.add_cmd(cmd)
    }

    /// Queue `INCR key`.
    pub fn incr<K: ToRedisArgs>(&mut self, key: K) -> &mut Self {
        let mut cmd = Cmd::new();
        cmd.arg("INCR").arg(key);
        self.add_cmd(cmd)
    }

    /// Queue `PING`.
    pub fn ping(&mut self) -> &mut Self {
        let mut cmd = Cmd::new();
        cmd.arg("PING");
        self.add_cmd(cmd)
    }

    pub(crate) fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }
}

/// Execute a batch against a core client and normalize the reply to a `Vec`.
pub(crate) async fn run_batch(
    core: &CoreClient,
    batch: &Batch,
    routing: Option<RoutingInfo>,
    raise_on_error: bool,
    options: &BatchOptions,
) -> Result<Vec<Value>> {
    if batch.is_empty() {
        return Ok(Vec::new());
    }
    let timeout = options.timeout_millis();
    let mut client = core.clone();
    let value = if batch.is_atomic {
        client
            .send_transaction(batch.pipeline(), routing, timeout, raise_on_error)
            .await
            .map_err(GlideError::from)?
    } else {
        client
            .send_pipeline(
                batch.pipeline(),
                routing,
                raise_on_error,
                timeout,
                PipelineRetryStrategy {
                    retry_server_error: options.retry_server_error,
                    retry_connection_error: options.retry_connection_error,
                },
            )
            .await
            .map_err(GlideError::from)?
    };
    match value {
        Value::Array(items) => Ok(items),
        Value::Nil => Ok(Vec::new()),
        other => Ok(vec![other]),
    }
}
