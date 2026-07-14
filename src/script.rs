// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Lua script helper with the redis-rs `Script` API shape.
//!
//! A clean-room implementation of the `redis::Script` convenience type
//! (removed from the vendored fork), provided for redis-rs migration parity:
//!
//! ```rust,no_run
//! use glide::Script;
//! # async fn demo(mut client: glide::GlideClient) -> glide::RedisResult<()> {
//! let script = Script::new("return tonumber(ARGV[1]) + tonumber(ARGV[2])");
//! let sum: i64 = script.arg(1).arg(2).invoke_async(&mut client).await?;
//! assert_eq!(sum, 3);
//! # Ok(()) }
//! ```
//!
//! Semantics match redis-rs: `invoke_async` first attempts `EVALSHA` (cheap,
//! cached) and transparently falls back to `EVAL` (which also loads the script)
//! when the server does not know the hash (`NOSCRIPT`). Works with any
//! [`redis::aio::ConnectionLike`], i.e. both [`crate::GlideClient`] and
//! [`crate::GlideClusterClient`].

use redis::aio::ConnectionLike;
use redis::{ErrorKind, FromRedisValue, RedisResult, ToRedisArgs, cmd};

/// A cached Lua script with its SHA-1 hash, mirroring redis-rs's `Script`.
///
/// Create once (computes the SHA-1), then [`Self::arg`]/[`Self::key`] to build
/// an invocation. See the [module docs](self) for an example.
#[derive(Debug, Clone)]
pub struct Script {
    code: String,
    hash: String,
}

impl Script {
    /// Create a new script object with a precomputed SHA-1 hash.
    pub fn new(code: &str) -> Script {
        let mut sha1 = sha1_smol::Sha1::new();
        sha1.update(code.as_bytes());
        Script {
            code: code.to_string(),
            hash: sha1.digest().to_string(),
        }
    }

    /// The SHA-1 hash of the script, as used by `EVALSHA`.
    pub fn get_hash(&self) -> &str {
        &self.hash
    }

    /// Create an invocation and add a regular argument (`ARGV[…]`).
    pub fn arg<'a, T: ToRedisArgs>(&'a self, arg: T) -> ScriptInvocation<'a> {
        let mut invocation = self.prepare_invoke();
        invocation.arg(arg);
        invocation
    }

    /// Create an invocation and add a key argument (`KEYS[…]`).
    pub fn key<'a, T: ToRedisArgs>(&'a self, key: T) -> ScriptInvocation<'a> {
        let mut invocation = self.prepare_invoke();
        invocation.key(key);
        invocation
    }

    /// Create an empty invocation (no keys, no args).
    pub fn prepare_invoke(&self) -> ScriptInvocation<'_> {
        ScriptInvocation {
            script: self,
            args: Vec::new(),
            keys: Vec::new(),
        }
    }

    /// Invoke the script without keys or args.
    pub async fn invoke_async<C: ConnectionLike, T: FromRedisValue>(
        &self,
        con: &mut C,
    ) -> RedisResult<T> {
        self.prepare_invoke().invoke_async(con).await
    }
}

/// A pending script invocation: keys + args bound to a [`Script`].
pub struct ScriptInvocation<'a> {
    script: &'a Script,
    args: Vec<Vec<u8>>,
    keys: Vec<Vec<u8>>,
}

impl ScriptInvocation<'_> {
    /// Add a regular argument (`ARGV[…]`). Builder form.
    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut Self {
        arg.write_redis_args(&mut self.args);
        self
    }

    /// Add a key argument (`KEYS[…]`). Builder form.
    pub fn key<T: ToRedisArgs>(&mut self, key: T) -> &mut Self {
        key.write_redis_args(&mut self.keys);
        self
    }

    /// Invoke the script: `EVALSHA` first, transparent `EVAL` fallback when the
    /// server does not have the script cached (`NOSCRIPT`).
    pub async fn invoke_async<C: ConnectionLike, T: FromRedisValue>(
        &self,
        con: &mut C,
    ) -> RedisResult<T> {
        let mut evalsha = cmd("EVALSHA");
        evalsha
            .arg(self.script.hash.as_bytes())
            .arg(self.keys.len())
            .arg(&self.keys)
            .arg(&self.args);
        match evalsha.query_async(con).await {
            Err(err) if err.kind() == ErrorKind::NoScriptError => {
                // Not cached on the server yet — EVAL both runs and caches it.
                let mut eval = cmd("EVAL");
                eval.arg(self.script.code.as_bytes())
                    .arg(self.keys.len())
                    .arg(&self.keys)
                    .arg(&self.args);
                eval.query_async(con).await
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_matches_server_semantics() {
        // SHA-1 of "return 1" (verified via `printf 'return 1' | sha1sum` and
        // matching the server's SCRIPT LOAD result).
        let script = Script::new("return 1");
        assert_eq!(
            script.get_hash(),
            "e0e1f9fabfc9d4800c877a703b823ac0578ff8db"
        );
    }

    #[test]
    fn invocation_collects_keys_and_args() {
        let script = Script::new("return KEYS[1]");
        let mut invocation = script.prepare_invoke();
        invocation.key("k1").key("k2").arg("a1").arg(2);
        assert_eq!(invocation.keys, vec![b"k1".to_vec(), b"k2".to_vec()]);
        assert_eq!(invocation.args, vec![b"a1".to_vec(), b"2".to_vec()]);
    }
}
