// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! String commands. Mirrors Python's string command surface.

use crate::commands::options::{ExpirySet, SetOptions};
use crate::error::Result;
use crate::executor::CommandExecutor;
use crate::value;
use async_trait::async_trait;
use bytes::Bytes;
use redis::{Cmd, ToRedisArgs};

/// String commands (`GET`, `SET`, `APPEND`, `INCR`, ...).
#[async_trait]
pub trait StringCommands: CommandExecutor {
    /// Get the value of `key`. Returns `None` if the key does not exist.
    async fn get<K: ToRedisArgs + Send>(&self, key: K) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("GET").arg(key);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get the value of `key` and delete it (`GETDEL`).
    async fn getdel<K: ToRedisArgs + Send>(&self, key: K) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("GETDEL").arg(key);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get the value of `key`, optionally changing its expiry (`GETEX`).
    async fn getex<K: ToRedisArgs + Send>(
        &self,
        key: K,
        expiry: Option<ExpirySet>,
    ) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("GETEX").arg(key);
        if let Some(e) = expiry {
            e.add_to(&mut cmd);
        }
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Set `key` to `value`.
    async fn set<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<()> {
        let mut cmd = Cmd::new();
        cmd.arg("SET").arg(key).arg(value);
        crate::value::to_unit(self.execute_command(cmd, None).await?)
    }

    /// Set `key` to `value` with options. Returns the old value when
    /// [`SetOptions::return_old_value`] is set, or `None` if the conditional set
    /// did not apply.
    async fn set_options<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        value: V,
        options: SetOptions,
    ) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("SET").arg(key).arg(value);
        options.add_to(&mut cmd);
        // With GET, the reply is the old value (or nil). Without GET, it's OK/nil.
        crate::value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Append `value` to `key`, returning the new length.
    async fn append<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("APPEND").arg(key).arg(value);
        crate::value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get the length of the string stored at `key` (`STRLEN`).
    async fn strlen<K: ToRedisArgs + Send>(&self, key: K) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("STRLEN").arg(key);
        crate::value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get a substring of the string stored at `key` (`GETRANGE`).
    async fn getrange<K: ToRedisArgs + Send>(&self, key: K, start: i64, end: i64) -> Result<Bytes> {
        let mut cmd = Cmd::new();
        cmd.arg("GETRANGE").arg(key).arg(start).arg(end);
        crate::value::to_bytes(self.execute_command(cmd, None).await?)
    }

    /// Overwrite part of the string at `key` starting at `offset` (`SETRANGE`).
    async fn setrange<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        offset: i64,
        value: V,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("SETRANGE").arg(key).arg(offset).arg(value);
        crate::value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get the values of multiple keys (`MGET`).
    async fn mget<K: ToRedisArgs + Send + Sync>(&self, keys: &[K]) -> Result<Vec<Option<Bytes>>> {
        let mut cmd = Cmd::new();
        cmd.arg("MGET");
        for k in keys {
            cmd.arg(k);
        }
        let reply = self.execute_command(cmd, None).await?;
        match reply {
            redis::Value::Array(items) => items.into_iter().map(value::to_opt_bytes).collect(),
            other => Ok(vec![value::to_opt_bytes(other)?]),
        }
    }

    /// Set multiple key/value pairs (`MSET`).
    async fn mset<K: ToRedisArgs + Send + Sync, V: ToRedisArgs + Send + Sync>(
        &self,
        pairs: &[(K, V)],
    ) -> Result<()> {
        let mut cmd = Cmd::new();
        cmd.arg("MSET");
        for (k, v) in pairs {
            cmd.arg(k).arg(v);
        }
        crate::value::to_unit(self.execute_command(cmd, None).await?)
    }

    /// Set multiple key/value pairs only if none of the keys exist (`MSETNX`).
    async fn msetnx<K: ToRedisArgs + Send + Sync, V: ToRedisArgs + Send + Sync>(
        &self,
        pairs: &[(K, V)],
    ) -> Result<bool> {
        let mut cmd = Cmd::new();
        cmd.arg("MSETNX");
        for (k, v) in pairs {
            cmd.arg(k).arg(v);
        }
        crate::value::to_bool(self.execute_command(cmd, None).await?)
    }

    /// Increment the integer value of `key` by one (`INCR`).
    async fn incr<K: ToRedisArgs + Send>(&self, key: K) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("INCR").arg(key);
        crate::value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Increment the integer value of `key` by `amount` (`INCRBY`).
    async fn incr_by<K: ToRedisArgs + Send>(&self, key: K, amount: i64) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("INCRBY").arg(key).arg(amount);
        crate::value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Increment the float value of `key` by `amount` (`INCRBYFLOAT`).
    async fn incr_by_float<K: ToRedisArgs + Send>(&self, key: K, amount: f64) -> Result<f64> {
        let mut cmd = Cmd::new();
        cmd.arg("INCRBYFLOAT").arg(key).arg(amount);
        crate::value::to_f64(self.execute_command(cmd, None).await?)
    }

    /// Decrement the integer value of `key` by one (`DECR`).
    async fn decr<K: ToRedisArgs + Send>(&self, key: K) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("DECR").arg(key);
        crate::value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Decrement the integer value of `key` by `amount` (`DECRBY`).
    async fn decr_by<K: ToRedisArgs + Send>(&self, key: K, amount: i64) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("DECRBY").arg(key).arg(amount);
        crate::value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Longest common subsequence length between two keys (`LCS ... LEN`).
    async fn lcs_len<K1: ToRedisArgs + Send, K2: ToRedisArgs + Send>(
        &self,
        key1: K1,
        key2: K2,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("LCS").arg(key1).arg(key2).arg("LEN");
        crate::value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Atomically set `key` to `value` and return the old value (`GETSET`).
    async fn getset<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("GETSET").arg(key).arg(value);
        crate::value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Set `key` to `value` with an expiry in seconds (`SETEX`).
    async fn setex<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        seconds: u64,
        value: V,
    ) -> Result<()> {
        let mut cmd = Cmd::new();
        cmd.arg("SETEX").arg(key).arg(seconds).arg(value);
        crate::value::to_unit(self.execute_command(cmd, None).await?)
    }

    /// Set `key` to `value` with an expiry in milliseconds (`PSETEX`).
    async fn psetex<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        milliseconds: u64,
        value: V,
    ) -> Result<()> {
        let mut cmd = Cmd::new();
        cmd.arg("PSETEX").arg(key).arg(milliseconds).arg(value);
        crate::value::to_unit(self.execute_command(cmd, None).await?)
    }

    /// Set `key` to `value` only if it does not already exist (`SETNX`).
    /// Returns `true` if the key was set.
    async fn setnx<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<bool> {
        let mut cmd = Cmd::new();
        cmd.arg("SETNX").arg(key).arg(value);
        crate::value::to_bool(self.execute_command(cmd, None).await?)
    }

    /// Get a substring of the string stored at `key` (`SUBSTR`), an alias of
    /// `GETRANGE` retained for parity.
    async fn substr<K: ToRedisArgs + Send>(&self, key: K, start: i64, end: i64) -> Result<Bytes> {
        let mut cmd = Cmd::new();
        cmd.arg("SUBSTR").arg(key).arg(start).arg(end);
        crate::value::to_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get the longest common subsequence of two keys (`LCS`).
    async fn lcs<K1: ToRedisArgs + Send, K2: ToRedisArgs + Send>(
        &self,
        key1: K1,
        key2: K2,
    ) -> Result<Bytes> {
        let mut cmd = Cmd::new();
        cmd.arg("LCS").arg(key1).arg(key2);
        crate::value::to_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get the LCS match positions between two keys (`LCS ... IDX`). Returns the
    /// raw structured reply (a map of `matches`/`len`). Pass `min_match_len` to
    /// filter short matches, and `with_match_len` to include per-match lengths.
    async fn lcs_idx<K1: ToRedisArgs + Send, K2: ToRedisArgs + Send>(
        &self,
        key1: K1,
        key2: K2,
        min_match_len: Option<i64>,
        with_match_len: bool,
    ) -> Result<redis::Value> {
        let mut cmd = Cmd::new();
        cmd.arg("LCS").arg(key1).arg(key2).arg("IDX");
        if let Some(m) = min_match_len {
            cmd.arg("MINMATCHLEN").arg(m);
        }
        if with_match_len {
            cmd.arg("WITHMATCHLEN");
        }
        self.execute_command(cmd, None).await
    }
}

impl<T: CommandExecutor + ?Sized> StringCommands for T {}
