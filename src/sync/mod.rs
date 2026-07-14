// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Blocking (synchronous) clients.
//!
//! Mirrors Python `glide-sync`. [`SyncGlideClient`] / [`SyncGlideClusterClient`]
//! wrap the async clients and drive them on a shared, process-wide Tokio runtime.
//!
//! Every async command is reachable from sync code via [`SyncGlideClient::run`]
//! (and the cluster equivalent), and the most common commands also have direct
//! blocking methods.

use crate::client::{GlideClient, GlideClusterClient};
use crate::commands::prelude::*;
use crate::config::{GlideClientConfiguration, GlideClusterClientConfiguration};
use crate::error::Result;
use crate::executor::CustomCommand;
use crate::pipeline_options::PipelineOptions;
use crate::routes::Route;
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
    /// # use glide::{AsyncCommands, GlideClientConfiguration};
    /// # fn demo(client: SyncGlideClient) -> glide::RedisResult<()> {
    /// let value: Option<String> = client.run(|c| async move { c.get("key").await })?;
    /// # let _ = value; Ok(()) }
    /// ```
    pub fn run<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce(GlideClient) -> Fut,
        Fut: Future<Output = T>,
    {
        runtime().block_on(f(self.inner.clone()))
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

    /// Execute a redis-rs [`redis::Pipeline`] with GLIDE execution options
    /// (blocking). See [`crate::GlideClient::execute_pipeline`]; for plain
    /// typed execution prefer [`PipelineExt::query_glide`].
    pub fn execute_pipeline(
        &self,
        pipeline: &redis::Pipeline,
        raise_on_error: bool,
        options: &PipelineOptions,
    ) -> Result<Vec<Value>> {
        runtime().block_on(
            self.inner
                .execute_pipeline(pipeline, raise_on_error, options),
        )
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

    /// Execute a redis-rs [`redis::Pipeline`] with GLIDE execution options,
    /// optionally routed (blocking). See
    /// [`crate::GlideClusterClient::execute_pipeline`].
    pub fn execute_pipeline(
        &self,
        pipeline: &redis::Pipeline,
        raise_on_error: bool,
        route: Option<crate::Route>,
        options: &PipelineOptions,
    ) -> Result<Vec<Value>> {
        runtime().block_on(
            self.inner
                .execute_pipeline(pipeline, raise_on_error, route, options),
        )
    }

    /// Blocking `PING`.
    pub fn ping(&self) -> Result<String> {
        runtime().block_on(self.inner.ping())
    }
}

// ---- redis-rs sync API compatibility -----------------------------------------
//
// The fork blanket-implements the blocking typed API over the sync trait:
//
//     impl<T> Commands for T where T: ConnectionLike {}
//
// so implementing `redis::ConnectionLike` here makes `SyncGlideClient` /
// `SyncGlideClusterClient` first-class blocking redis-rs connection objects
// (`use glide::Commands;`), mirroring what the async clients do with
// `redis::aio::ConnectionLike`.
//
// The sync trait's required methods take *packed bytes*. Typed `Commands`
// methods always go through the provided `req_command(&Cmd)` — which we
// override to bridge straight to the async impl, no byte round-trip — and
// `Pipeline::query` sends `encode_pipeline` output: a sequence of RESP arrays
// of bulk strings that we decode back into commands with the fork's own
// `parse_redis_value`.

/// Decode packed command bytes (RESP arrays of bulk strings, as produced by
/// `Cmd::get_packed_command` / `encode_pipeline`) back into `Cmd`s.
///
/// The packed-command wire format is strict and self-delimiting —
/// `*<n>\r\n` followed by `n` × `$<len>\r\n<data>\r\n` — so consecutive
/// commands are parsed incrementally.
fn unpack_commands(bytes: &[u8]) -> redis::RedisResult<Vec<redis::Cmd>> {
    fn malformed(what: &'static str) -> redis::RedisError {
        redis::RedisError::from((
            redis::ErrorKind::ClientError,
            "malformed packed command",
            what.to_string(),
        ))
    }
    /// Read `<digits>\r\n` after the given type marker; returns (value, rest).
    fn read_len(bytes: &[u8], marker: u8) -> redis::RedisResult<(usize, &[u8])> {
        let rest = bytes
            .strip_prefix(&[marker])
            .ok_or_else(|| malformed("missing type marker"))?;
        let end = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or_else(|| malformed("missing CRLF after length"))?;
        let digits = &rest[..end];
        // Strictly ASCII digits: `parse::<usize>` alone would also accept a
        // leading `+`, which is not valid RESP.
        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return Err(malformed("invalid length"));
        }
        let len = std::str::from_utf8(digits)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or_else(|| malformed("invalid length"))?;
        Ok((len, &rest[end + 2..]))
    }

    let mut out = Vec::new();
    let mut rest = bytes;
    // Reused scratch: arg slices of the command currently being parsed.
    let mut args: Vec<&[u8]> = Vec::new();
    while !rest.is_empty() {
        let (argc, mut cur) = read_len(rest, b'*')?;
        // Two passes per command: collect the arg slices first so the `Cmd`
        // can be built pre-sized (`with_capacity`) — `cmd.arg()` into an
        // empty `Cmd` would regrow its data/args buffers as it copies.
        args.clear();
        let mut data_len = 0usize;
        for _ in 0..argc {
            let (len, data_and_rest) = read_len(cur, b'$')?;
            // `len` is attacker-controlled; `len + 2` must not overflow
            // (usize::MAX would wrap to 1 in release and pass the bounds
            // check below, panicking on the slice range).
            let total = len
                .checked_add(2)
                .ok_or_else(|| malformed("length overflow"))?;
            if data_and_rest.len() < total || &data_and_rest[len..total] != b"\r\n" {
                return Err(malformed("truncated bulk string"));
            }
            args.push(&data_and_rest[..len]);
            // Cannot overflow: every counted byte was verified present above.
            data_len += len;
            cur = &data_and_rest[total..];
        }
        let mut cmd = redis::Cmd::with_capacity(argc, data_len);
        for arg in &args {
            cmd.arg(*arg);
        }
        out.push(cmd);
        rest = cur;
    }
    Ok(out)
}

/// Is this command a bare `NAME` (single-arg command like MULTI/EXEC)?
fn is_bare_command(cmd: &redis::Cmd, name: &[u8]) -> bool {
    let mut args = cmd.args_iter();
    args.len() == 1
        && args.next().is_some_and(
            |a| matches!(a, redis::Arg::Simple(bytes) if bytes.eq_ignore_ascii_case(name)),
        )
}

/// Rebuild a logical [`redis::Pipeline`] from packed pipeline bytes.
///
/// Whether the pipeline is an atomic transaction is decided from the trait
/// call's `offset`/`count`, not by sniffing command names: the fork always
/// calls `req_packed_commands(bytes, len + 1, 1)` for transactions and
/// `(bytes, 0, len)` for plain pipelines (fork `pipeline.rs`). A plain
/// pipeline may legitimately *contain* literal `MULTI`/`EXEC` commands
/// (manual transaction management) and must not be misdetected as atomic.
///
/// For transactions, `encode_pipeline` wraps the commands in MULTI…EXEC;
/// strip them (validating the wrapper) and mark the pipeline atomic instead —
/// glide-core re-adds MULTI/EXEC in `send_transaction`.
fn unpack_pipeline(
    bytes: &[u8],
    offset: usize,
    count: usize,
) -> redis::RedisResult<redis::Pipeline> {
    let mut commands = unpack_commands(bytes)?;
    let is_transaction = offset > 0 && count == 1;
    let mut pipeline = redis::Pipeline::with_capacity(commands.len());
    if is_transaction {
        // Validate the MULTI…EXEC wrapper the fork's `encode_pipeline`
        // produces before stripping it.
        let well_formed = commands.len() >= 2
            && commands
                .first()
                .is_some_and(|c| is_bare_command(c, b"MULTI"))
            && commands.last().is_some_and(|c| is_bare_command(c, b"EXEC"));
        if !well_formed {
            return Err(redis::RedisError::from((
                redis::ErrorKind::ClientError,
                "malformed packed transaction",
                "expected MULTI…EXEC wrapper".to_string(),
            )));
        }
        pipeline.atomic();
        commands.pop(); // EXEC
        commands.drain(..1); // MULTI
    }
    for cmd in commands {
        pipeline.add_command(cmd);
    }
    Ok(pipeline)
}

/// Implement the sync `redis::ConnectionLike` by bridging to the async
/// `redis::aio::ConnectionLike` impl on the wrapped async client.
macro_rules! impl_sync_connection_like {
    ($sync_ty:ty, $get_db:expr) => {
        impl redis::ConnectionLike for $sync_ty {
            fn req_command(&mut self, cmd: &redis::Cmd) -> redis::RedisResult<Value> {
                // Fast path used by all typed `Commands` methods: no byte
                // round-trip.
                runtime().block_on(redis::aio::ConnectionLike::req_packed_command(
                    &mut self.inner,
                    cmd,
                ))
            }

            fn req_packed_command(&mut self, cmd: &[u8]) -> redis::RedisResult<Value> {
                let commands = unpack_commands(cmd)?;
                let [ref cmd] = commands[..] else {
                    return Err(redis::RedisError::from((
                        redis::ErrorKind::ClientError,
                        "expected exactly one packed command",
                    )));
                };
                runtime().block_on(redis::aio::ConnectionLike::req_packed_command(
                    &mut self.inner,
                    cmd,
                ))
            }

            fn req_packed_commands(
                &mut self,
                cmd: &[u8],
                offset: usize,
                count: usize,
            ) -> redis::RedisResult<Vec<Value>> {
                let pipeline = unpack_pipeline(cmd, offset, count)?;
                runtime().block_on(redis::aio::ConnectionLike::req_packed_commands(
                    &mut self.inner,
                    &pipeline,
                    offset,
                    count,
                    None,
                ))
            }

            fn get_db(&self) -> i64 {
                #[allow(clippy::redundant_closure_call)]
                ($get_db)(self)
            }

            fn check_connection(&mut self) -> bool {
                self.ping().is_ok()
            }

            fn is_open(&self) -> bool {
                // glide-core owns reconnection; see the async impl's
                // `is_closed`.
                true
            }
        }
    };
}

impl_sync_connection_like!(SyncGlideClient, |c: &SyncGlideClient| c.inner.db());
// Cluster deployments always use database 0.
impl_sync_connection_like!(SyncGlideClusterClient, |_c: &SyncGlideClusterClient| 0);

// ---- unified command API dispatch ---------------------------------------------
// See the async impls in `client/`: commands arrive **by value**, so the
// blocking typed API costs no `Cmd` clone and no packed-byte round-trip.

macro_rules! impl_sync_owned_send {
    ($sync_ty:ty) => {
        impl crate::commands::core::Commands for $sync_ty {
            fn glide_send_owned_sync(&self, cmd: redis::Cmd) -> redis::RedisResult<Value> {
                runtime().block_on(crate::commands::core::AsyncCommands::glide_send_owned(
                    &self.inner,
                    cmd,
                ))
            }
        }
    };
}

impl_sync_owned_send!(SyncGlideClient);
impl_sync_owned_send!(SyncGlideClusterClient);

// ---- native-copy sync pipelines ----------------------------------------------
//
// `redis::Pipeline::query` (blocking) serializes the pipeline to packed bytes
// and hands them to the sync `ConnectionLike` byte interface — so our sync
// impl must parse them back into a `Pipeline`, costing two extra payload
// copies vs the async path (whose `ConnectionLike` receives `&Pipeline`
// directly). `query_glide` sidesteps that: it drives the async
// `Pipeline::query_async(&Pipeline)` path on a clone of the wrapped async
// client, so a blocking pipeline copies the payload exactly as many times as
// the native `Batch` API. Drop-in shape: `.query(&mut c)` -> `.query_glide(&c)`.

/// A blocking GLIDE client that can run a redis-rs [`redis::Pipeline`] with
/// **native copy behavior**. Sealed — implemented only by
/// [`SyncGlideClient`] and [`SyncGlideClusterClient`].
pub trait SyncPipelineTarget: sealed::Sealed {
    /// The wrapped async connection type (a redis-rs async connection object).
    #[doc(hidden)]
    type Async: redis::aio::ConnectionLike;
    /// A cheap clone of the wrapped async client (Arc inside).
    #[doc(hidden)]
    fn async_conn(&self) -> Self::Async;
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::SyncGlideClient {}
    impl Sealed for super::SyncGlideClusterClient {}
}

impl SyncPipelineTarget for SyncGlideClient {
    type Async = GlideClient;
    fn async_conn(&self) -> GlideClient {
        self.inner.clone()
    }
}

impl SyncPipelineTarget for SyncGlideClusterClient {
    type Async = GlideClusterClient;
    fn async_conn(&self) -> GlideClusterClient {
        self.inner.clone()
    }
}

/// Extension for running a redis-rs [`redis::Pipeline`] on a blocking GLIDE
/// client with **native copy behavior** (no packed-byte round-trip).
///
/// Like the rest of the sync layer, this blocks on the internal runtime and
/// therefore **must not be called from within an async context** (doing so
/// panics with tokio's "cannot block the current thread from within a runtime"
/// — use [`redis::Pipeline::query_async`] on the async client instead).
///
/// ```no_run
/// use glide::sync::{PipelineExt, SyncGlideClient};
/// # fn demo(client: &SyncGlideClient) -> glide::RedisResult<()> {
/// let (a, b): (i64, i64) = glide::pipe()
///     .atomic()
///     .incr("c", 1)
///     .incr("c", 1)
///     .query_glide(client)?;
/// # let _ = (a, b); Ok(()) }
/// ```
pub trait PipelineExt {
    /// Execute this pipeline on a blocking GLIDE client, sending the built
    /// `Pipeline` directly to glide-core (native copy count), and decode the
    /// replies into `T` with the same `.ignore()`/transaction semantics as
    /// [`redis::Pipeline::query`].
    fn query_glide<C: SyncPipelineTarget, T: redis::FromRedisValue>(
        &self,
        con: &C,
    ) -> redis::RedisResult<T>;
}

impl PipelineExt for redis::Pipeline {
    fn query_glide<C: SyncPipelineTarget, T: redis::FromRedisValue>(
        &self,
        con: &C,
    ) -> redis::RedisResult<T> {
        let mut async_conn = con.async_conn();
        runtime().block_on(self.query_async(&mut async_conn))
    }
}

#[cfg(test)]
mod compat_tests {
    use super::*;

    #[test]
    fn unpack_single_command_roundtrip() {
        let mut cmd = redis::Cmd::new();
        cmd.arg("SET").arg("key").arg("value");
        let packed = cmd.get_packed_command();
        let unpacked = unpack_commands(&packed).unwrap();
        assert_eq!(unpacked.len(), 1);
        let args: Vec<_> = unpacked[0].args_iter().collect();
        assert_eq!(args.len(), 3);
    }

    #[test]
    fn unpack_pipeline_plain() {
        let mut p = redis::Pipeline::new();
        p.cmd("SET").arg("a").arg("1").cmd("GET").arg("a");
        // Plain pipelines are dispatched as (bytes, 0, len).
        let pipeline = unpack_pipeline(&p.get_packed_pipeline(), 0, 2).unwrap();
        assert!(!pipeline.is_atomic());
        assert_eq!(pipeline.len(), 2);
    }

    #[test]
    fn unpack_pipeline_transaction_strips_multi_exec() {
        let mut p = redis::Pipeline::new();
        p.atomic().cmd("INCR").arg("c").cmd("INCR").arg("c");
        // Transactions are dispatched as (bytes, len + 1, 1).
        let pipeline = unpack_pipeline(&p.get_packed_pipeline(), 3, 1).unwrap();
        assert!(pipeline.is_atomic());
        assert_eq!(pipeline.len(), 2, "MULTI/EXEC must be stripped");
    }

    #[test]
    fn unpack_pipeline_literal_multi_exec_stays_plain() {
        // A *non-atomic* pipeline containing literal MULTI/EXEC commands
        // (manual transaction management) must not be misdetected as atomic:
        // the decision comes from offset/count, not command sniffing.
        let mut p = redis::Pipeline::new();
        p.cmd("MULTI").cmd("INCR").arg("c").cmd("EXEC");
        let pipeline = unpack_pipeline(&p.get_packed_pipeline(), 0, 3).unwrap();
        assert!(!pipeline.is_atomic());
        assert_eq!(pipeline.len(), 3, "MULTI/EXEC must be preserved");
    }

    #[test]
    fn unpack_pipeline_transaction_without_wrapper_is_rejected() {
        let mut p = redis::Pipeline::new();
        p.cmd("INCR").arg("c");
        // Transaction offsets with no MULTI…EXEC wrapper: malformed.
        let err = unpack_pipeline(&p.get_packed_pipeline(), 2, 1).unwrap_err();
        assert_eq!(err.kind(), redis::ErrorKind::ClientError);
    }

    #[test]
    fn unpack_rejects_overflowing_bulk_length() {
        // `len + 2` on usize::MAX wraps in release builds; previously this
        // panicked on the slice range instead of returning an error.
        let input = b"*1\r\n$18446744073709551615\r\nX";
        let err = unpack_commands(input).unwrap_err();
        assert_eq!(err.kind(), redis::ErrorKind::ClientError);
    }

    #[test]
    fn unpack_rejects_malformed_input() {
        // Length larger than usize: parse failure, not panic.
        assert!(unpack_commands(b"*1\r\n$99999999999999999999\r\nX\r\n").is_err());
        // Truncated bulk payloads.
        assert!(unpack_commands(b"*1\r\n$5\r\nab").is_err());
        assert!(unpack_commands(b"*1\r\n$5\r\nabcde").is_err()); // missing CRLF
        // Bulk data not terminated by CRLF.
        assert!(unpack_commands(b"*1\r\n$3\r\nabcXX").is_err());
        // Wrong / missing type markers.
        assert!(unpack_commands(b"$3\r\nfoo\r\n").is_err());
        assert!(unpack_commands(b"*1\r\n+OK\r\n").is_err());
        // Non-digit and non-strict lengths (RESP lengths are bare digits).
        assert!(unpack_commands(b"*x\r\n").is_err());
        assert!(unpack_commands(b"*1\r\n$+5\r\nhello\r\n").is_err());
        assert!(unpack_commands(b"*1\r\n$\r\n\r\n").is_err());
        // Missing CRLF after length.
        assert!(unpack_commands(b"*1").is_err());
    }
}
