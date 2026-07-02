// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! HyperLogLog commands. Mirrors Python's HLL command surface.

use crate::error::Result;
use crate::executor::CommandExecutor;
use crate::value;
use async_trait::async_trait;
use redis::{Cmd, ToRedisArgs};

/// HyperLogLog commands (`PFADD`, `PFCOUNT`, `PFMERGE`).
#[async_trait]
pub trait HyperLogLogCommands: CommandExecutor {
    /// Add elements to the HyperLogLog at `key` (`PFADD`).
    /// Returns `true` if the internal registers were altered.
    async fn pfadd<K: ToRedisArgs + Send, E: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        elements: &[E],
    ) -> Result<bool> {
        let mut cmd = Cmd::new();
        cmd.arg("PFADD").arg(key);
        for e in elements {
            cmd.arg(e);
        }
        value::to_bool(self.execute_command(cmd, None).await?)
    }

    /// Estimate the cardinality of one or more HyperLogLogs (`PFCOUNT`).
    async fn pfcount<K: ToRedisArgs + Send + Sync>(&self, keys: &[K]) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("PFCOUNT");
        for k in keys {
            cmd.arg(k);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Merge multiple HyperLogLogs into `destination` (`PFMERGE`).
    async fn pfmerge<D: ToRedisArgs + Send, K: ToRedisArgs + Send + Sync>(
        &self,
        destination: D,
        source_keys: &[K],
    ) -> Result<()> {
        let mut cmd = Cmd::new();
        cmd.arg("PFMERGE").arg(destination);
        for k in source_keys {
            cmd.arg(k);
        }
        value::to_unit(self.execute_command(cmd, None).await?)
    }
}

impl<T: CommandExecutor + ?Sized> HyperLogLogCommands for T {}
