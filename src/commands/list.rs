// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! List commands. Mirrors Python's list command surface.

use crate::commands::options::{InsertPosition, ListDirection};
use crate::error::Result;
use crate::executor::CommandExecutor;
use crate::value;
use async_trait::async_trait;
use bytes::Bytes;
use redis::{Cmd, ToRedisArgs};

fn collect_bytes(v: redis::Value) -> Result<Vec<Bytes>> {
    match v {
        redis::Value::Array(items) => items.into_iter().map(value::to_bytes).collect(),
        redis::Value::Nil => Ok(Vec::new()),
        other => Ok(vec![value::to_bytes(other)?]),
    }
}

/// List commands (`LPUSH`, `RPUSH`, `LPOP`, `LRANGE`, `LMOVE`, ...).
#[async_trait]
pub trait ListCommands: CommandExecutor {
    /// Prepend values to the list at `key` (`LPUSH`); returns the new length.
    async fn lpush<K: ToRedisArgs + Send, V: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        values: &[V],
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("LPUSH").arg(key);
        for v in values {
            cmd.arg(v);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Prepend values only if the list already exists (`LPUSHX`).
    async fn lpushx<K: ToRedisArgs + Send, V: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        values: &[V],
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("LPUSHX").arg(key);
        for v in values {
            cmd.arg(v);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Append values to the list at `key` (`RPUSH`); returns the new length.
    async fn rpush<K: ToRedisArgs + Send, V: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        values: &[V],
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("RPUSH").arg(key);
        for v in values {
            cmd.arg(v);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Append values only if the list already exists (`RPUSHX`).
    async fn rpushx<K: ToRedisArgs + Send, V: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        values: &[V],
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("RPUSHX").arg(key);
        for v in values {
            cmd.arg(v);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Pop one element from the head of the list (`LPOP`).
    async fn lpop<K: ToRedisArgs + Send>(&self, key: K) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("LPOP").arg(key);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Pop up to `count` elements from the head of the list (`LPOP key count`).
    async fn lpop_count<K: ToRedisArgs + Send>(&self, key: K, count: i64) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("LPOP").arg(key).arg(count);
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Pop one element from the tail of the list (`RPOP`).
    async fn rpop<K: ToRedisArgs + Send>(&self, key: K) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("RPOP").arg(key);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Pop up to `count` elements from the tail of the list (`RPOP key count`).
    async fn rpop_count<K: ToRedisArgs + Send>(&self, key: K, count: i64) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("RPOP").arg(key).arg(count);
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get a range of elements from the list (`LRANGE`).
    async fn lrange<K: ToRedisArgs + Send>(
        &self,
        key: K,
        start: i64,
        stop: i64,
    ) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("LRANGE").arg(key).arg(start).arg(stop);
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get the length of the list (`LLEN`).
    async fn llen<K: ToRedisArgs + Send>(&self, key: K) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("LLEN").arg(key);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get the element at `index` (`LINDEX`).
    async fn lindex<K: ToRedisArgs + Send>(&self, key: K, index: i64) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("LINDEX").arg(key).arg(index);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Set the element at `index` to `value` (`LSET`).
    async fn lset<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        index: i64,
        value: V,
    ) -> Result<()> {
        let mut cmd = Cmd::new();
        cmd.arg("LSET").arg(key).arg(index).arg(value);
        crate::value::to_unit(self.execute_command(cmd, None).await?)
    }

    /// Trim the list to the given range (`LTRIM`).
    async fn ltrim<K: ToRedisArgs + Send>(&self, key: K, start: i64, stop: i64) -> Result<()> {
        let mut cmd = Cmd::new();
        cmd.arg("LTRIM").arg(key).arg(start).arg(stop);
        value::to_unit(self.execute_command(cmd, None).await?)
    }

    /// Remove `count` occurrences of `element` from the list (`LREM`).
    async fn lrem<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        count: i64,
        element: V,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("LREM").arg(key).arg(count).arg(element);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Insert `element` before/after `pivot` (`LINSERT`).
    async fn linsert<K: ToRedisArgs + Send, P: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        position: InsertPosition,
        pivot: P,
        element: V,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("LINSERT")
            .arg(key)
            .arg(position.as_arg())
            .arg(pivot)
            .arg(element);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Atomically move an element from one list to another (`LMOVE`).
    async fn lmove<S: ToRedisArgs + Send, D: ToRedisArgs + Send>(
        &self,
        source: S,
        destination: D,
        from: ListDirection,
        to: ListDirection,
    ) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("LMOVE")
            .arg(source)
            .arg(destination)
            .arg(from.as_arg())
            .arg(to.as_arg());
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Find the index of `element` in the list (`LPOS`).
    async fn lpos<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        element: V,
    ) -> Result<Option<i64>> {
        let mut cmd = Cmd::new();
        cmd.arg("LPOS").arg(key).arg(element);
        match self.execute_command(cmd, None).await? {
            redis::Value::Nil => Ok(None),
            other => Ok(Some(value::to_i64(other)?)),
        }
    }

    /// Blocking pop from the head of the first non-empty list (`BLPOP`).
    /// Returns `(key, element)` or `None` on timeout. `timeout` is in seconds
    /// (`0` blocks indefinitely).
    async fn blpop<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        timeout: f64,
    ) -> Result<Option<(Bytes, Bytes)>> {
        let mut cmd = Cmd::new();
        cmd.arg("BLPOP");
        for k in keys {
            cmd.arg(k);
        }
        cmd.arg(timeout);
        parse_key_value(self.execute_command(cmd, None).await?)
    }

    /// Blocking pop from the tail of the first non-empty list (`BRPOP`).
    /// Returns `(key, element)` or `None` on timeout.
    async fn brpop<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        timeout: f64,
    ) -> Result<Option<(Bytes, Bytes)>> {
        let mut cmd = Cmd::new();
        cmd.arg("BRPOP");
        for k in keys {
            cmd.arg(k);
        }
        cmd.arg(timeout);
        parse_key_value(self.execute_command(cmd, None).await?)
    }

    /// Blocking variant of `LMOVE` (`BLMOVE`). Returns the moved element or
    /// `None` on timeout.
    async fn blmove<S: ToRedisArgs + Send, D: ToRedisArgs + Send>(
        &self,
        source: S,
        destination: D,
        from: ListDirection,
        to: ListDirection,
        timeout: f64,
    ) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("BLMOVE")
            .arg(source)
            .arg(destination)
            .arg(from.as_arg())
            .arg(to.as_arg())
            .arg(timeout);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Blocking pop from the tail of `source` pushing onto the head of
    /// `destination` (`BRPOPLPUSH`). Returns the moved element or `None`.
    async fn brpoplpush<S: ToRedisArgs + Send, D: ToRedisArgs + Send>(
        &self,
        source: S,
        destination: D,
        timeout: f64,
    ) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("BRPOPLPUSH")
            .arg(source)
            .arg(destination)
            .arg(timeout);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Pop one or more elements from the first non-empty list (`LMPOP`).
    /// Returns `(key, elements)` or `None` if all lists are empty.
    async fn lmpop<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        direction: ListDirection,
        count: Option<i64>,
    ) -> Result<Option<(Bytes, Vec<Bytes>)>> {
        let mut cmd = Cmd::new();
        cmd.arg("LMPOP").arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        cmd.arg(direction.as_arg());
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        parse_key_values(self.execute_command(cmd, None).await?)
    }

    /// Blocking variant of `LMPOP` (`BLMPOP`). Returns `(key, elements)` or
    /// `None` on timeout.
    async fn blmpop<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        direction: ListDirection,
        count: Option<i64>,
        timeout: f64,
    ) -> Result<Option<(Bytes, Vec<Bytes>)>> {
        let mut cmd = Cmd::new();
        cmd.arg("BLMPOP").arg(timeout).arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        cmd.arg(direction.as_arg());
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        parse_key_values(self.execute_command(cmd, None).await?)
    }
}

/// Parse a `[key, element]` reply (BLPOP/BRPOP) into `Option<(key, element)>`.
fn parse_key_value(v: redis::Value) -> Result<Option<(Bytes, Bytes)>> {
    match v {
        redis::Value::Nil => Ok(None),
        redis::Value::Array(mut items) if items.len() == 2 => {
            let element = value::to_bytes(items.pop().unwrap())?;
            let key = value::to_bytes(items.pop().unwrap())?;
            Ok(Some((key, element)))
        }
        other => Err(crate::error::GlideError::Request(format!(
            "unexpected blocking-pop reply: {other:?}"
        ))),
    }
}

/// Parse a `[key, [elements...]]` reply (LMPOP/ZMPOP style) into
/// `Option<(key, elements)>`.
fn parse_key_values(v: redis::Value) -> Result<Option<(Bytes, Vec<Bytes>)>> {
    match v {
        redis::Value::Nil => Ok(None),
        redis::Value::Array(mut items) if items.len() == 2 => {
            let elements = collect_bytes(items.pop().unwrap())?;
            let key = value::to_bytes(items.pop().unwrap())?;
            Ok(Some((key, elements)))
        }
        other => Err(crate::error::GlideError::Request(format!(
            "unexpected LMPOP reply: {other:?}"
        ))),
    }
}

impl<T: CommandExecutor + ?Sized> ListCommands for T {}
