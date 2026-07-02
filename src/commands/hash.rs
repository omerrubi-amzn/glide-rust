// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Hash commands. Mirrors Python's hash command surface.

use crate::commands::options::{ExpireOptions, ExpirySet, HashFieldConditionalChange};
use crate::error::Result;
use crate::executor::CommandExecutor;
use crate::value;
use async_trait::async_trait;
use bytes::Bytes;
use redis::{Cmd, ToRedisArgs};
use std::collections::HashMap;

/// Hash commands (`HSET`, `HGET`, `HGETALL`, `HDEL`, ...).
#[async_trait]
pub trait HashCommands: CommandExecutor {
    /// Set `field` to `value` in the hash at `key`; returns fields added.
    async fn hset<K, F, V>(&self, key: K, field_values: &[(F, V)]) -> Result<i64>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    {
        let mut cmd = Cmd::new();
        cmd.arg("HSET").arg(key);
        for (f, v) in field_values {
            cmd.arg(f).arg(v);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get the value of `field` in the hash at `key`.
    async fn hget<K: ToRedisArgs + Send, F: ToRedisArgs + Send>(
        &self,
        key: K,
        field: F,
    ) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("HGET").arg(key).arg(field);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Set `field` to `value` only if it does not exist (`HSETNX`).
    async fn hsetnx<K: ToRedisArgs + Send, F: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        field: F,
        value: V,
    ) -> Result<bool> {
        let mut cmd = Cmd::new();
        cmd.arg("HSETNX").arg(key).arg(field).arg(value);
        crate::value::to_bool(self.execute_command(cmd, None).await?)
    }

    /// Delete the given fields; returns the number removed (`HDEL`).
    async fn hdel<K: ToRedisArgs + Send + Sync, F: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        fields: &[F],
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("HDEL").arg(key);
        for f in fields {
            cmd.arg(f);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get all fields and values of the hash at `key` (`HGETALL`).
    async fn hgetall<K: ToRedisArgs + Send>(&self, key: K) -> Result<HashMap<String, Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("HGETALL").arg(key);
        let map: HashMap<String, Vec<u8>> =
            value::from_value(self.execute_command(cmd, None).await?)?;
        Ok(map.into_iter().map(|(k, v)| (k, Bytes::from(v))).collect())
    }

    /// Get the values of multiple fields (`HMGET`).
    async fn hmget<K: ToRedisArgs + Send, F: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        fields: &[F],
    ) -> Result<Vec<Option<Bytes>>> {
        let mut cmd = Cmd::new();
        cmd.arg("HMGET").arg(key);
        for f in fields {
            cmd.arg(f);
        }
        match self.execute_command(cmd, None).await? {
            redis::Value::Array(items) => items.into_iter().map(value::to_opt_bytes).collect(),
            other => Ok(vec![value::to_opt_bytes(other)?]),
        }
    }

    /// Return whether `field` exists in the hash (`HEXISTS`).
    async fn hexists<K: ToRedisArgs + Send, F: ToRedisArgs + Send>(
        &self,
        key: K,
        field: F,
    ) -> Result<bool> {
        let mut cmd = Cmd::new();
        cmd.arg("HEXISTS").arg(key).arg(field);
        value::to_bool(self.execute_command(cmd, None).await?)
    }

    /// Get the number of fields in the hash (`HLEN`).
    async fn hlen<K: ToRedisArgs + Send>(&self, key: K) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("HLEN").arg(key);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get all field names of the hash (`HKEYS`).
    async fn hkeys<K: ToRedisArgs + Send>(&self, key: K) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("HKEYS").arg(key);
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get all values of the hash (`HVALS`).
    async fn hvals<K: ToRedisArgs + Send>(&self, key: K) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("HVALS").arg(key);
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Increment the integer value of `field` by `amount` (`HINCRBY`).
    async fn hincr_by<K: ToRedisArgs + Send, F: ToRedisArgs + Send>(
        &self,
        key: K,
        field: F,
        amount: i64,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("HINCRBY").arg(key).arg(field).arg(amount);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Increment the float value of `field` by `amount` (`HINCRBYFLOAT`).
    async fn hincr_by_float<K: ToRedisArgs + Send, F: ToRedisArgs + Send>(
        &self,
        key: K,
        field: F,
        amount: f64,
    ) -> Result<f64> {
        let mut cmd = Cmd::new();
        cmd.arg("HINCRBYFLOAT").arg(key).arg(field).arg(amount);
        value::to_f64(self.execute_command(cmd, None).await?)
    }

    /// Get the string length of a field's value (`HSTRLEN`).
    async fn hstrlen<K: ToRedisArgs + Send, F: ToRedisArgs + Send>(
        &self,
        key: K,
        field: F,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("HSTRLEN").arg(key).arg(field);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get a random field from the hash (`HRANDFIELD`).
    async fn hrandfield<K: ToRedisArgs + Send>(&self, key: K) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("HRANDFIELD").arg(key);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get `count` random fields from the hash (`HRANDFIELD key count`).
    async fn hrandfield_count<K: ToRedisArgs + Send>(
        &self,
        key: K,
        count: i64,
    ) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("HRANDFIELD").arg(key).arg(count);
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get `count` random fields with their values
    /// (`HRANDFIELD key count WITHVALUES`).
    async fn hrandfield_withvalues<K: ToRedisArgs + Send>(
        &self,
        key: K,
        count: i64,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let mut cmd = Cmd::new();
        cmd.arg("HRANDFIELD").arg(key).arg(count).arg("WITHVALUES");
        collect_pairs(self.execute_command(cmd, None).await?)
    }

    /// Incrementally iterate a hash (`HSCAN`). Returns `(cursor, [(field, value),
    /// ...])`. A returned cursor of `"0"` indicates iteration is complete.
    async fn hscan<K: ToRedisArgs + Send>(
        &self,
        key: K,
        cursor: &str,
        pattern: Option<&[u8]>,
        count: Option<i64>,
    ) -> Result<(String, Vec<(Bytes, Bytes)>)> {
        let mut cmd = Cmd::new();
        cmd.arg("HSCAN").arg(key).arg(cursor);
        if let Some(p) = pattern {
            cmd.arg("MATCH").arg(p);
        }
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        let (cursor, flat) =
            crate::commands::generic::parse_scan_reply(self.execute_command(cmd, None).await?)?;
        let mut out = Vec::with_capacity(flat.len() / 2);
        let mut iter = flat.into_iter();
        while let (Some(f), Some(v)) = (iter.next(), iter.next()) {
            out.push((f, v));
        }
        Ok((cursor, out))
    }

    /// Incrementally iterate a hash returning only field names
    /// (`HSCAN ... NOVALUES`). Returns `(cursor, fields)`.
    async fn hscan_novalues<K: ToRedisArgs + Send>(
        &self,
        key: K,
        cursor: &str,
        pattern: Option<&[u8]>,
        count: Option<i64>,
    ) -> Result<(String, Vec<Bytes>)> {
        let mut cmd = Cmd::new();
        cmd.arg("HSCAN").arg(key).arg(cursor);
        if let Some(p) = pattern {
            cmd.arg("MATCH").arg(p);
        }
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        cmd.arg("NOVALUES");
        crate::commands::generic::parse_scan_reply(self.execute_command(cmd, None).await?)
    }

    /// Set an expiry in seconds on one or more hash fields (`HEXPIRE`). Returns a
    /// per-field status code.
    async fn hexpire<K: ToRedisArgs + Send, F: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        seconds: i64,
        fields: &[F],
        option: Option<ExpireOptions>,
    ) -> Result<Vec<i64>> {
        self.hfield_expire("HEXPIRE", key, Some(seconds), fields, option)
            .await
    }

    /// Set an expiry at an absolute Unix time (seconds) on hash fields
    /// (`HEXPIREAT`).
    async fn hexpireat<K: ToRedisArgs + Send, F: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        unix_seconds: i64,
        fields: &[F],
        option: Option<ExpireOptions>,
    ) -> Result<Vec<i64>> {
        self.hfield_expire("HEXPIREAT", key, Some(unix_seconds), fields, option)
            .await
    }

    /// Get the absolute expiry Unix time (seconds) of hash fields
    /// (`HEXPIRETIME`).
    async fn hexpiretime<K: ToRedisArgs + Send, F: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        fields: &[F],
    ) -> Result<Vec<i64>> {
        self.hfield_expire::<K, F>("HEXPIRETIME", key, None, fields, None)
            .await
    }

    /// Set an expiry in milliseconds on hash fields (`HPEXPIRE`).
    async fn hpexpire<K: ToRedisArgs + Send, F: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        milliseconds: i64,
        fields: &[F],
        option: Option<ExpireOptions>,
    ) -> Result<Vec<i64>> {
        self.hfield_expire("HPEXPIRE", key, Some(milliseconds), fields, option)
            .await
    }

    /// Set an expiry at an absolute Unix time (milliseconds) on hash fields
    /// (`HPEXPIREAT`).
    async fn hpexpireat<K: ToRedisArgs + Send, F: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        unix_milliseconds: i64,
        fields: &[F],
        option: Option<ExpireOptions>,
    ) -> Result<Vec<i64>> {
        self.hfield_expire("HPEXPIREAT", key, Some(unix_milliseconds), fields, option)
            .await
    }

    /// Get the absolute expiry Unix time (milliseconds) of hash fields
    /// (`HPEXPIRETIME`).
    async fn hpexpiretime<K: ToRedisArgs + Send, F: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        fields: &[F],
    ) -> Result<Vec<i64>> {
        self.hfield_expire::<K, F>("HPEXPIRETIME", key, None, fields, None)
            .await
    }

    /// Get the remaining TTL in seconds of hash fields (`HTTL`).
    async fn httl<K: ToRedisArgs + Send, F: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        fields: &[F],
    ) -> Result<Vec<i64>> {
        self.hfield_expire::<K, F>("HTTL", key, None, fields, None)
            .await
    }

    /// Get the remaining TTL in milliseconds of hash fields (`HPTTL`).
    async fn hpttl<K: ToRedisArgs + Send, F: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        fields: &[F],
    ) -> Result<Vec<i64>> {
        self.hfield_expire::<K, F>("HPTTL", key, None, fields, None)
            .await
    }

    /// Remove the expiry from hash fields (`HPERSIST`). Returns a per-field
    /// status code.
    async fn hpersist<K: ToRedisArgs + Send, F: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        fields: &[F],
    ) -> Result<Vec<i64>> {
        self.hfield_expire::<K, F>("HPERSIST", key, None, fields, None)
            .await
    }

    #[doc(hidden)]
    async fn hfield_expire<K: ToRedisArgs + Send, F: ToRedisArgs + Send + Sync>(
        &self,
        op: &'static str,
        key: K,
        value: Option<i64>,
        fields: &[F],
        option: Option<ExpireOptions>,
    ) -> Result<Vec<i64>> {
        let mut cmd = Cmd::new();
        cmd.arg(op).arg(key);
        if let Some(v) = value {
            cmd.arg(v);
        }
        if let Some(o) = option {
            o.add_to(&mut cmd);
        }
        cmd.arg("FIELDS").arg(fields.len());
        for f in fields {
            cmd.arg(f);
        }
        collect_i64(self.execute_command(cmd, None).await?)
    }

    /// Get the values of hash fields, optionally changing their expiry
    /// (`HGETEX`).
    async fn hgetex<K: ToRedisArgs + Send, F: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        fields: &[F],
        expiry: Option<ExpirySet>,
    ) -> Result<Vec<Option<Bytes>>> {
        let mut cmd = Cmd::new();
        cmd.arg("HGETEX").arg(key);
        if let Some(e) = expiry {
            e.add_to(&mut cmd);
        }
        cmd.arg("FIELDS").arg(fields.len());
        for f in fields {
            cmd.arg(f);
        }
        match self.execute_command(cmd, None).await? {
            redis::Value::Array(items) => items.into_iter().map(value::to_opt_bytes).collect(),
            redis::Value::Nil => Ok(Vec::new()),
            other => Ok(vec![value::to_opt_bytes(other)?]),
        }
    }

    /// Set hash field values with an optional field condition and expiry
    /// (`HSETEX`). Returns `1` if all fields were set, `0` otherwise.
    async fn hsetex<K, F, V>(
        &self,
        key: K,
        field_values: &[(F, V)],
        condition: Option<HashFieldConditionalChange>,
        expiry: Option<ExpirySet>,
    ) -> Result<i64>
    where
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    {
        let mut cmd = Cmd::new();
        cmd.arg("HSETEX").arg(key);
        if let Some(c) = condition {
            cmd.arg(c.as_arg());
        }
        if let Some(e) = expiry {
            e.add_to(&mut cmd);
        }
        cmd.arg("FIELDS").arg(field_values.len());
        for (f, v) in field_values {
            cmd.arg(f).arg(v);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }
}

/// Collect an array reply into `Vec<i64>`.
fn collect_i64(v: redis::Value) -> Result<Vec<i64>> {
    match v {
        redis::Value::Nil => Ok(Vec::new()),
        redis::Value::Array(items) => items.into_iter().map(value::to_i64).collect(),
        other => Ok(vec![value::to_i64(other)?]),
    }
}

/// Parse a flat `[a, b, a, b, ...]` reply into `(a, b)` pairs.
fn collect_pairs(v: redis::Value) -> Result<Vec<(Bytes, Bytes)>> {
    match v {
        redis::Value::Nil => Ok(Vec::new()),
        redis::Value::Map(pairs) => pairs
            .into_iter()
            .map(|(a, b)| Ok((value::to_bytes(a)?, value::to_bytes(b)?)))
            .collect(),
        redis::Value::Array(items) => {
            if items
                .iter()
                .all(|it| matches!(it, redis::Value::Array(inner) if inner.len() == 2))
            {
                let mut out = Vec::with_capacity(items.len());
                for it in items {
                    if let redis::Value::Array(mut pair) = it {
                        let b = value::to_bytes(pair.pop().unwrap())?;
                        let a = value::to_bytes(pair.pop().unwrap())?;
                        out.push((a, b));
                    }
                }
                Ok(out)
            } else {
                let mut out = Vec::with_capacity(items.len() / 2);
                let mut iter = items.into_iter();
                while let (Some(a), Some(b)) = (iter.next(), iter.next()) {
                    out.push((value::to_bytes(a)?, value::to_bytes(b)?));
                }
                Ok(out)
            }
        }
        other => Ok(vec![(value::to_bytes(other)?, Bytes::new())]),
    }
}

fn collect_bytes(v: redis::Value) -> Result<Vec<Bytes>> {
    match v {
        redis::Value::Array(items) => items.into_iter().map(value::to_bytes).collect(),
        redis::Value::Nil => Ok(Vec::new()),
        other => Ok(vec![value::to_bytes(other)?]),
    }
}

impl<T: CommandExecutor + ?Sized> HashCommands for T {}
