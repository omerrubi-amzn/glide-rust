// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Generic (key) commands. Mirrors Python's generic command surface.

use crate::commands::options::{Limit, MigrateOptions, OrderBy, RestoreOptions};
use crate::error::Result;
use crate::executor::CommandExecutor;
use crate::value;
use async_trait::async_trait;
use bytes::Bytes;
use redis::{Cmd, ToRedisArgs};

/// Generic key-space commands (`DEL`, `EXISTS`, `EXPIRE`, `TTL`, `RENAME`, ...).
#[async_trait]
pub trait GenericCommands: CommandExecutor {
    /// Get the absolute expiry Unix time in seconds (`EXPIRETIME`).
    async fn expiretime<K: ToRedisArgs + Send>(&self, key: K) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("EXPIRETIME").arg(key);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get the absolute expiry Unix time in milliseconds (`PEXPIRETIME`).
    async fn pexpiretime<K: ToRedisArgs + Send>(&self, key: K) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("PEXPIRETIME").arg(key);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Return a random key from the keyspace (`RANDOMKEY`).
    async fn randomkey(&self) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("RANDOMKEY");
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Serialize `key` (`DUMP`). Returns `None` if the key does not exist.
    async fn dump<K: ToRedisArgs + Send>(&self, key: K) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("DUMP").arg(key);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Touch the given keys, returning how many were touched (`TOUCH`).
    async fn touch<K: ToRedisArgs + Send + Sync>(&self, keys: &[K]) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("TOUCH");
        for k in keys {
            cmd.arg(k);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Copy `source` to `destination` (`COPY`). Set `replace` to overwrite.
    async fn copy<S: ToRedisArgs + Send, D: ToRedisArgs + Send>(
        &self,
        source: S,
        destination: D,
        replace: bool,
    ) -> Result<bool> {
        let mut cmd = Cmd::new();
        cmd.arg("COPY").arg(source).arg(destination);
        if replace {
            cmd.arg("REPLACE");
        }
        value::to_bool(self.execute_command(cmd, None).await?)
    }

    /// Sort the elements at `key` (`SORT`), optionally by order and with an
    /// optional `LIMIT offset count`.
    async fn sort<K: ToRedisArgs + Send>(
        &self,
        key: K,
        order: Option<OrderBy>,
        limit: Option<Limit>,
        alpha: bool,
    ) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("SORT").arg(key);
        if let Some(l) = limit {
            cmd.arg("LIMIT").arg(l.offset).arg(l.count);
        }
        if let Some(o) = order {
            cmd.arg(o.as_arg());
        }
        if alpha {
            cmd.arg("ALPHA");
        }
        match self.execute_command(cmd, None).await? {
            redis::Value::Array(items) => items.into_iter().map(value::to_bytes).collect(),
            redis::Value::Nil => Ok(Vec::new()),
            other => Ok(vec![value::to_bytes(other)?]),
        }
    }

    /// Copy `source` to `destination`, optionally into a different logical
    /// database (`COPY ... DB destination_db`). Set `replace` to overwrite.
    async fn copy_with_options<S: ToRedisArgs + Send, D: ToRedisArgs + Send>(
        &self,
        source: S,
        destination: D,
        destination_db: Option<i64>,
        replace: bool,
    ) -> Result<bool> {
        let mut cmd = Cmd::new();
        cmd.arg("COPY").arg(source).arg(destination);
        if let Some(db) = destination_db {
            cmd.arg("DB").arg(db);
        }
        if replace {
            cmd.arg("REPLACE");
        }
        value::to_bool(self.execute_command(cmd, None).await?)
    }

    /// Create a key from a serialized payload produced by `DUMP` (`RESTORE`).
    async fn restore<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        ttl_ms: i64,
        serialized: V,
        options: RestoreOptions,
    ) -> Result<()> {
        let mut cmd = Cmd::new();
        cmd.arg("RESTORE").arg(key).arg(ttl_ms).arg(serialized);
        options.add_to(&mut cmd);
        value::to_unit(self.execute_command(cmd, None).await?)
    }

    /// Block until `numreplicas` replicas acknowledge previous writes, or until
    /// `timeout_ms` elapses (`WAIT`). Returns the number of replicas reached.
    async fn wait(&self, numreplicas: i64, timeout_ms: i64) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("WAIT").arg(numreplicas).arg(timeout_ms);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Sort the elements at `key` and store the result into `destination`
    /// (`SORT ... STORE destination`). Returns the number of elements stored.
    async fn sort_store<K: ToRedisArgs + Send, D: ToRedisArgs + Send>(
        &self,
        key: K,
        destination: D,
        order: Option<OrderBy>,
        limit: Option<Limit>,
        alpha: bool,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("SORT").arg(key);
        if let Some(l) = limit {
            cmd.arg("LIMIT").arg(l.offset).arg(l.count);
        }
        if let Some(o) = order {
            cmd.arg(o.as_arg());
        }
        if alpha {
            cmd.arg("ALPHA");
        }
        cmd.arg("STORE").arg(destination);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Read-only variant of `SORT` (`SORT_RO`); returns the sorted elements.
    async fn sort_ro<K: ToRedisArgs + Send>(
        &self,
        key: K,
        order: Option<OrderBy>,
        limit: Option<Limit>,
        alpha: bool,
    ) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("SORT_RO").arg(key);
        if let Some(l) = limit {
            cmd.arg("LIMIT").arg(l.offset).arg(l.count);
        }
        if let Some(o) = order {
            cmd.arg(o.as_arg());
        }
        if alpha {
            cmd.arg("ALPHA");
        }
        match self.execute_command(cmd, None).await? {
            redis::Value::Array(items) => items.into_iter().map(value::to_bytes).collect(),
            redis::Value::Nil => Ok(Vec::new()),
            other => Ok(vec![value::to_bytes(other)?]),
        }
    }

    /// Move `key` to another logical database (`MOVE`). Returns whether the key
    /// was moved.
    async fn move_key<K: ToRedisArgs + Send>(&self, key: K, db: i64) -> Result<bool> {
        let mut cmd = Cmd::new();
        cmd.arg("MOVE").arg(key).arg(db);
        value::to_bool(self.execute_command(cmd, None).await?)
    }

    /// Atomically transfer a key to another instance (`MIGRATE`).
    async fn migrate<H: ToRedisArgs + Send, K: ToRedisArgs + Send>(
        &self,
        host: H,
        port: i64,
        key: K,
        destination_db: i64,
        timeout_ms: i64,
        options: MigrateOptions,
    ) -> Result<()> {
        let mut cmd = Cmd::new();
        cmd.arg("MIGRATE")
            .arg(host)
            .arg(port)
            .arg(key)
            .arg(destination_db)
            .arg(timeout_ms);
        options.add_to(&mut cmd);
        value::to_unit(self.execute_command(cmd, None).await?)
    }

    /// Watch the given keys for changes before a transaction (`WATCH`).
    async fn watch<K: ToRedisArgs + Send + Sync>(&self, keys: &[K]) -> Result<()> {
        let mut cmd = Cmd::new();
        cmd.arg("WATCH");
        for k in keys {
            cmd.arg(k);
        }
        value::to_unit(self.execute_command(cmd, None).await?)
    }

    /// Forget all watched keys (`UNWATCH`).
    async fn unwatch(&self) -> Result<()> {
        let mut cmd = Cmd::new();
        cmd.arg("UNWATCH");
        value::to_unit(self.execute_command(cmd, None).await?)
    }
}

pub(crate) fn parse_scan_reply(reply: redis::Value) -> Result<(String, Vec<Bytes>)> {
    match reply {
        redis::Value::Array(mut items) if items.len() == 2 => {
            let keys_val = items.pop().unwrap();
            let cursor_val = items.pop().unwrap();
            let cursor = value::to_string(cursor_val)?;
            let keys = match keys_val {
                redis::Value::Array(elems) => elems
                    .into_iter()
                    .map(value::to_bytes)
                    .collect::<Result<Vec<_>>>()?,
                _ => Vec::new(),
            };
            Ok((cursor, keys))
        }
        _ => Err(crate::error::GlideError::Request(
            "unexpected SCAN reply shape".into(),
        )),
    }
}

impl<T: CommandExecutor + ?Sized> GenericCommands for T {}
