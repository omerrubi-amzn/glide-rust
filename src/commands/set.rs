// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Set commands. Mirrors Python's set command surface.

use crate::error::Result;
use crate::executor::CommandExecutor;
use crate::value;
use async_trait::async_trait;
use bytes::Bytes;
use redis::{Cmd, ToRedisArgs};
use std::collections::HashSet;

fn collect_bytes(v: redis::Value) -> Result<Vec<Bytes>> {
    match v {
        redis::Value::Array(items) => items.into_iter().map(value::to_bytes).collect(),
        redis::Value::Set(items) => items.into_iter().map(value::to_bytes).collect(),
        redis::Value::Nil => Ok(Vec::new()),
        other => Ok(vec![value::to_bytes(other)?]),
    }
}

/// Set commands (`SADD`, `SREM`, `SMEMBERS`, `SINTER`, ...).
#[async_trait]
pub trait SetCommands: CommandExecutor {
    /// Add members to the set at `key` (`SADD`); returns members added.
    async fn sadd<K: ToRedisArgs + Send, M: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        members: &[M],
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("SADD").arg(key);
        for m in members {
            cmd.arg(m);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Remove members from the set (`SREM`); returns members removed.
    async fn srem<K: ToRedisArgs + Send, M: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        members: &[M],
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("SREM").arg(key);
        for m in members {
            cmd.arg(m);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get all members of the set (`SMEMBERS`).
    async fn smembers<K: ToRedisArgs + Send>(&self, key: K) -> Result<HashSet<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("SMEMBERS").arg(key);
        Ok(collect_bytes(self.execute_command(cmd, None).await?)?
            .into_iter()
            .collect())
    }

    /// Get the number of members in the set (`SCARD`).
    async fn scard<K: ToRedisArgs + Send>(&self, key: K) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("SCARD").arg(key);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Determine if `member` is in the set (`SISMEMBER`).
    async fn sismember<K: ToRedisArgs + Send, M: ToRedisArgs + Send>(
        &self,
        key: K,
        member: M,
    ) -> Result<bool> {
        let mut cmd = Cmd::new();
        cmd.arg("SISMEMBER").arg(key).arg(member);
        value::to_bool(self.execute_command(cmd, None).await?)
    }

    /// Check membership of multiple members (`SMISMEMBER`).
    async fn smismember<K: ToRedisArgs + Send, M: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        members: &[M],
    ) -> Result<Vec<bool>> {
        let mut cmd = Cmd::new();
        cmd.arg("SMISMEMBER").arg(key);
        for m in members {
            cmd.arg(m);
        }
        match self.execute_command(cmd, None).await? {
            redis::Value::Array(items) => items.into_iter().map(value::to_bool).collect(),
            other => Ok(vec![value::to_bool(other)?]),
        }
    }

    /// Pop a random member from the set (`SPOP`).
    async fn spop<K: ToRedisArgs + Send>(&self, key: K) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("SPOP").arg(key);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Pop `count` random members from the set (`SPOP key count`).
    async fn spop_count<K: ToRedisArgs + Send>(
        &self,
        key: K,
        count: i64,
    ) -> Result<HashSet<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("SPOP").arg(key).arg(count);
        Ok(collect_bytes(self.execute_command(cmd, None).await?)?
            .into_iter()
            .collect())
    }

    /// Get a random member without removing it (`SRANDMEMBER`).
    async fn srandmember<K: ToRedisArgs + Send>(&self, key: K) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("SRANDMEMBER").arg(key);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Compute the union of the given sets (`SUNION`).
    async fn sunion<K: ToRedisArgs + Send + Sync>(&self, keys: &[K]) -> Result<HashSet<Bytes>> {
        self.set_op("SUNION", keys).await
    }

    /// Compute the intersection of the given sets (`SINTER`).
    async fn sinter<K: ToRedisArgs + Send + Sync>(&self, keys: &[K]) -> Result<HashSet<Bytes>> {
        self.set_op("SINTER", keys).await
    }

    /// Compute the difference of the given sets (`SDIFF`).
    async fn sdiff<K: ToRedisArgs + Send + Sync>(&self, keys: &[K]) -> Result<HashSet<Bytes>> {
        self.set_op("SDIFF", keys).await
    }

    /// Cardinality of the intersection of the given sets (`SINTERCARD`).
    async fn sintercard<K: ToRedisArgs + Send + Sync>(&self, keys: &[K]) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("SINTERCARD").arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Store the union of the given sets into `destination` (`SUNIONSTORE`).
    async fn sunionstore<D: ToRedisArgs + Send, K: ToRedisArgs + Send + Sync>(
        &self,
        destination: D,
        keys: &[K],
    ) -> Result<i64> {
        self.set_op_store("SUNIONSTORE", destination, keys).await
    }

    /// Store the intersection of the given sets into `destination` (`SINTERSTORE`).
    async fn sinterstore<D: ToRedisArgs + Send, K: ToRedisArgs + Send + Sync>(
        &self,
        destination: D,
        keys: &[K],
    ) -> Result<i64> {
        self.set_op_store("SINTERSTORE", destination, keys).await
    }

    /// Store the difference of the given sets into `destination` (`SDIFFSTORE`).
    async fn sdiffstore<D: ToRedisArgs + Send, K: ToRedisArgs + Send + Sync>(
        &self,
        destination: D,
        keys: &[K],
    ) -> Result<i64> {
        self.set_op_store("SDIFFSTORE", destination, keys).await
    }

    /// Move `member` from `source` to `destination` (`SMOVE`).
    async fn smove<S: ToRedisArgs + Send, D: ToRedisArgs + Send, M: ToRedisArgs + Send>(
        &self,
        source: S,
        destination: D,
        member: M,
    ) -> Result<bool> {
        let mut cmd = Cmd::new();
        cmd.arg("SMOVE").arg(source).arg(destination).arg(member);
        value::to_bool(self.execute_command(cmd, None).await?)
    }

    /// Get `count` random members without removing them (`SRANDMEMBER key count`).
    /// A negative count allows repeated members.
    async fn srandmember_count<K: ToRedisArgs + Send>(
        &self,
        key: K,
        count: i64,
    ) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("SRANDMEMBER").arg(key).arg(count);
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Cardinality of the intersection of the given sets with a `LIMIT`
    /// (`SINTERCARD numkeys key [key ...] LIMIT limit`). A `limit` of `0` means
    /// no limit.
    async fn sintercard_limit<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        limit: i64,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("SINTERCARD").arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        cmd.arg("LIMIT").arg(limit);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Incrementally iterate a set (`SSCAN`). Returns `(cursor, members)`.
    /// A returned cursor of `"0"` indicates iteration is complete.
    async fn sscan<K: ToRedisArgs + Send>(
        &self,
        key: K,
        cursor: &str,
        pattern: Option<&[u8]>,
        count: Option<i64>,
    ) -> Result<(String, Vec<Bytes>)> {
        let mut cmd = Cmd::new();
        cmd.arg("SSCAN").arg(key).arg(cursor);
        if let Some(p) = pattern {
            cmd.arg("MATCH").arg(p);
        }
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        crate::commands::generic::parse_scan_reply(self.execute_command(cmd, None).await?)
    }

    #[doc(hidden)]
    async fn set_op<K: ToRedisArgs + Send + Sync>(
        &self,
        op: &'static str,
        keys: &[K],
    ) -> Result<HashSet<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg(op);
        for k in keys {
            cmd.arg(k);
        }
        Ok(collect_bytes(self.execute_command(cmd, None).await?)?
            .into_iter()
            .collect())
    }

    #[doc(hidden)]
    async fn set_op_store<D: ToRedisArgs + Send, K: ToRedisArgs + Send + Sync>(
        &self,
        op: &'static str,
        destination: D,
        keys: &[K],
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg(op).arg(destination);
        for k in keys {
            cmd.arg(k);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }
}

impl<T: CommandExecutor + ?Sized> SetCommands for T {}
