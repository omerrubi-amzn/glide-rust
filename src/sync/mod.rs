// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Blocking (synchronous) clients.
//!
//! Mirrors Python `glide-sync`. [`SyncGlideClient`] / [`SyncGlideClusterClient`]
//! wrap the async clients and drive them on a shared, process-wide Tokio runtime.
//!
//! Every async command is reachable from sync code via [`SyncGlideClient::run`]
//! (and the cluster equivalent), and the most common commands also have direct
//! blocking methods.

use crate::batch::{Batch, BatchOptions};
use crate::client::{GlideClient, GlideClusterClient};
use crate::commands::options::SetOptions;
use crate::commands::prelude::*;
use crate::config::{GlideClientConfiguration, GlideClusterClientConfiguration};
use crate::error::Result;
use crate::executor::CustomCommand;
use crate::routes::Route;
use bytes::Bytes;
use redis::{ToRedisArgs, Value};
use std::future::Future;
use std::sync::OnceLock;
use tokio::runtime::{Builder, Runtime};

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("glide-sync")
            .build()
            .expect("failed to build the shared GLIDE sync runtime")
    })
}

/// Block on an arbitrary future using the shared runtime.
pub fn block_on<F: Future>(future: F) -> F::Output {
    runtime().block_on(future)
}

/// A blocking client for a **standalone** deployment.
#[derive(Clone)]
pub struct SyncGlideClient {
    inner: GlideClient,
}

impl SyncGlideClient {
    /// Connect using the given standalone configuration (blocking).
    pub fn connect(config: GlideClientConfiguration) -> Result<Self> {
        let inner = runtime().block_on(GlideClient::connect(config))?;
        Ok(SyncGlideClient { inner })
    }

    /// The underlying async client.
    pub fn async_client(&self) -> &GlideClient {
        &self.inner
    }

    /// Run an arbitrary async operation against the client, blocking until it
    /// completes. This unlocks the *entire* async command surface from sync code:
    ///
    /// ```rust,no_run
    /// # use glide::sync::SyncGlideClient;
    /// # use glide::{GlideClientConfiguration, StringCommands};
    /// # fn demo(client: SyncGlideClient) -> glide::Result<()> {
    /// let ttl = client.run(|c| async move { c.get("key").await })?;
    /// # let _ = ttl; Ok(()) }
    /// ```
    pub fn run<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce(GlideClient) -> Fut,
        Fut: Future<Output = T>,
    {
        runtime().block_on(f(self.inner.clone()))
    }

    /// Execute a [`Batch`] (blocking).
    pub fn exec(&self, batch: &Batch, raise_on_error: bool) -> Result<Vec<Value>> {
        runtime().block_on(self.inner.exec(batch, raise_on_error))
    }

    /// Execute a [`Batch`] with explicit [`BatchOptions`] (blocking).
    pub fn exec_with_options(
        &self,
        batch: &Batch,
        raise_on_error: bool,
        options: &BatchOptions,
    ) -> Result<Vec<Value>> {
        runtime().block_on(self.inner.exec_with_options(batch, raise_on_error, options))
    }

    /// Update the connection password (blocking). See
    /// [`GlideClient::update_connection_password`].
    pub fn update_connection_password(
        &self,
        password: Option<String>,
        immediate_auth: bool,
    ) -> Result<()> {
        runtime().block_on(
            self.inner
                .update_connection_password(password, immediate_auth),
        )
    }

    /// Run an arbitrary command (blocking escape hatch).
    pub fn custom_command<A: ToRedisArgs + Sync>(&self, args: &[A]) -> Result<Value> {
        runtime().block_on(self.inner.custom_command(args))
    }

    // ---- Common blocking conveniences ----

    /// Blocking `GET`.
    pub fn get<K: ToRedisArgs + Send>(&self, key: K) -> Result<Option<Bytes>> {
        runtime().block_on(self.inner.get(key))
    }
    /// Blocking `SET`.
    pub fn set<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<()> {
        runtime().block_on(self.inner.set(key, value))
    }
    /// Blocking `SET` with options.
    pub fn set_options<K: ToRedisArgs + Send, V: ToRedisArgs + Send>(
        &self,
        key: K,
        value: V,
        options: SetOptions,
    ) -> Result<Option<Bytes>> {
        runtime().block_on(self.inner.set_options(key, value, options))
    }
    /// Blocking `DEL`.
    pub fn del<K: ToRedisArgs + Send + Sync>(&self, keys: &[K]) -> Result<i64> {
        runtime().block_on(self.inner.del(keys))
    }
    /// Blocking `EXISTS`.
    pub fn exists<K: ToRedisArgs + Send + Sync>(&self, keys: &[K]) -> Result<i64> {
        runtime().block_on(self.inner.exists(keys))
    }
    /// Blocking `INCR`.
    pub fn incr<K: ToRedisArgs + Send>(&self, key: K) -> Result<i64> {
        runtime().block_on(self.inner.incr(key))
    }
    /// Blocking `EXPIRE`.
    pub fn expire<K: ToRedisArgs + Send>(&self, key: K, seconds: i64) -> Result<bool> {
        runtime().block_on(self.inner.expire(key, seconds))
    }
    /// Blocking `TTL`.
    pub fn ttl<K: ToRedisArgs + Send>(&self, key: K) -> Result<i64> {
        runtime().block_on(self.inner.ttl(key))
    }
    /// Blocking `PING`.
    pub fn ping(&self) -> Result<String> {
        runtime().block_on(self.inner.ping())
    }
}

/// A blocking client for a **cluster** deployment.
#[derive(Clone)]
pub struct SyncGlideClusterClient {
    inner: GlideClusterClient,
}

impl SyncGlideClusterClient {
    /// Connect using the given cluster configuration (blocking).
    pub fn connect(config: GlideClusterClientConfiguration) -> Result<Self> {
        let inner = runtime().block_on(GlideClusterClient::connect(config))?;
        Ok(SyncGlideClusterClient { inner })
    }

    /// The underlying async client.
    pub fn async_client(&self) -> &GlideClusterClient {
        &self.inner
    }

    /// Run an arbitrary async operation against the client (blocking).
    pub fn run<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce(GlideClusterClient) -> Fut,
        Fut: Future<Output = T>,
    {
        runtime().block_on(f(self.inner.clone()))
    }

    /// Run an arbitrary command (blocking escape hatch).
    pub fn custom_command<A: ToRedisArgs + Sync>(&self, args: &[A]) -> Result<Value> {
        runtime().block_on(self.inner.custom_command(args))
    }

    /// Run an arbitrary command with an explicit route (blocking).
    pub fn custom_command_with_route<A: ToRedisArgs + Sync>(
        &self,
        args: &[A],
        route: Route,
    ) -> Result<Value> {
        runtime().block_on(self.inner.custom_command_with_route(args, route))
    }

    /// Execute a [`Batch`] (blocking).
    pub fn exec(
        &self,
        batch: &Batch,
        raise_on_error: bool,
        route: Option<Route>,
    ) -> Result<Vec<Value>> {
        runtime().block_on(self.inner.exec(batch, raise_on_error, route))
    }

    /// Execute a [`Batch`] with explicit [`BatchOptions`] (blocking).
    pub fn exec_with_options(
        &self,
        batch: &Batch,
        raise_on_error: bool,
        route: Option<Route>,
        options: &BatchOptions,
    ) -> Result<Vec<Value>> {
        runtime().block_on(
            self.inner
                .exec_with_options(batch, raise_on_error, route, options),
        )
    }

    /// Update the connection password (blocking). See
    /// [`GlideClusterClient::update_connection_password`].
    pub fn update_connection_password(
        &self,
        password: Option<String>,
        immediate_auth: bool,
    ) -> Result<()> {
        runtime().block_on(
            self.inner
                .update_connection_password(password, immediate_auth),
        )
    }

    /// Blocking `PING`.
    pub fn ping(&self) -> Result<String> {
        runtime().block_on(self.inner.ping())
    }
}
