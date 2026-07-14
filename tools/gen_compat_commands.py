#!/usr/bin/env python3
"""Generate src/compat_commands.rs from the vendored redis-rs fork's
implement_commands! table.

Each generated trait method is signature-identical to the fork's
Commands/AsyncCommands method (same name, same generic order — turbofish
compatible) but delegates to the fork's generated `Cmd::<name>()` constructor
(identical wire encoding) and sends the built Cmd BY VALUE through
glide_send_owned / glide_send_owned_sync, avoiding the &Cmd -> clone tax of
the redis-rs ConnectionLike path.

Usage:
    python3 tools/gen_compat_commands.py            # resolve fork via cargo metadata
    python3 tools/gen_compat_commands.py /path/to/redis-rs/redis/src/commands/mod.rs

The output is formatted with `rustfmt` (edition 2024) so it lands
byte-identical to `cargo fmt`. See DEVELOPER.md "Regenerating compat_commands.rs".
"""
import json
import re
import shutil
import subprocess
import sys


def resolve_fork_mod_rs() -> str:
    """Locate the vendored redis fork's commands/mod.rs.

    Prefer an explicit argv[1]; otherwise derive the resolved `redis` package's
    manifest path from `cargo metadata` (no machine-specific hardcoding).
    """
    if len(sys.argv) > 1:
        return sys.argv[1]
    meta = json.loads(
        subprocess.check_output(["cargo", "metadata", "--format-version", "1"])
    )
    manifests = [p["manifest_path"] for p in meta["packages"] if p["name"] == "redis"]
    if not manifests:
        sys.exit("could not resolve the `redis` package via cargo metadata")
    # manifest_path is .../redis/Cargo.toml -> .../redis/src/commands/mod.rs
    import os

    return os.path.join(os.path.dirname(manifests[0]), "src", "commands", "mod.rs")


FORK = resolve_fork_mod_rs()

src = open(FORK).read()
start = src.index("implement_commands! {")
body = src[start:]
# The invocation ends at the first closing brace at column 0.
body = body[: re.search(r"^\}", body, re.M).start()]

# Parse entries: (doc_lines, attr_lines, name, [(G, Bound)...], [(arg, ty)...])
entries = []
i = 0
lines = body.split("\n")
pending_docs, pending_attrs = [], []
n = len(lines)
li = 0
while li < n:
    line = lines[li]
    stripped = line.strip()
    if stripped.startswith("///"):
        pending_docs.append(stripped)
        li += 1
        continue
    if stripped.startswith("#["):
        pending_attrs.append(stripped)
        li += 1
        continue
    m = re.match(r"\s*fn\s+([a-z_0-9]+)\s*(<[^>]*>)?\s*\(?", line)
    if not m or not stripped.startswith("fn "):
        if stripped and not stripped.startswith("//"):
            pending_docs, pending_attrs = [], []
        li += 1
        continue
    # Accumulate until the signature's closing ') {'
    sig = line
    while "{" not in sig:
        li += 1
        sig += " " + lines[li].strip()
    li += 1
    # Skip the body (balanced braces; body starts after the first '{' of sig)
    depth = sig.count("{") - sig.count("}")
    while depth > 0:
        depth += lines[li].count("{") - lines[li].count("}")
        li += 1
    sig = " ".join(sig.split())
    m = re.match(r"fn\s+([a-z_0-9]+)\s*(?:<([^>]*)>)?\s*\((.*?)\)\s*\{", sig)
    if not m:
        sys.exit(f"cannot parse signature: {sig}")
    name, gen, args = m.group(1), m.group(2) or "", m.group(3).strip()
    generics = []
    for g in filter(None, (p.strip() for p in gen.split(","))):
        gname, bound = (x.strip() for x in g.split(":", 1))
        generics.append((gname, bound))
    arglist = []
    if args:
        # split on top-level commas (arg types may contain (K, V) tuples)
        parts, depth_p, cur = [], 0, ""
        for ch in args:
            if ch in "(<[":
                depth_p += 1
            elif ch in ")>]":
                depth_p -= 1
            if ch == "," and depth_p == 0:
                parts.append(cur)
                cur = ""
            else:
                cur += ch
        parts.append(cur)
        for p in parts:
            aname, ty = (x.strip() for x in p.split(":", 1))
            arglist.append((aname, ty))
    entries.append((pending_docs, pending_attrs, name, generics, arglist))
    pending_docs, pending_attrs = [], []

assert len(entries) == 151, (
    f"expected 151 fork methods, parsed {len(entries)}. The fork's command table "
    "changed (likely a rev bump) — review the delta and update this count "
    "deliberately, along with the pinned rev and the copy-parity docs."
)

def method(docs, attrs, name, generics, args, is_async):
    out = []
    for d in docs:
        out.append(f"    {d}")
    for a in attrs:
        out.append(f"    {a}")
    out.append("    #[inline]")
    # Only add our own #[allow(deprecated)] when the fork entry did not already
    # carry one (deprecated methods copy the fork's attribute verbatim above).
    if not any("allow(deprecated)" in a for a in attrs):
        out.append("    #[allow(deprecated)]")
    out.append("    #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]")
    call_args = ", ".join(a for a, _ in args)
    params = "".join(f", {a}: {t}" for a, t in args)
    if is_async:
        gens = "".join(f"{g}: {b} + Send + Sync + 'a, " for g, b in generics)
        out.append(f"    fn {name}<'a, {gens}RV>(&'a mut self{params}) -> RedisFuture<'a, RV>")
        out.append("    where")
        out.append("        RV: FromRedisValue,")
        out.append("    {")
        out.append(f"        let cmd = Cmd::{name}({call_args});")
        out.append("        Box::pin(async move { from_owned_redis_value(self.glide_send_owned(cmd).await?) })")
        out.append("    }")
    else:
        gens = "".join(f"{g}: {b}, " for g, b in generics)
        out.append(f"    fn {name}<'a, {gens}RV: FromRedisValue>(&mut self{params}) -> RedisResult<RV> {{")
        out.append(f"        from_owned_redis_value(self.glide_send_owned_sync(Cmd::{name}({call_args}))?)")
        out.append("    }")
    return "\n".join(out)

header = '''\
// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//
// The method table (names, signatures, and doc comments) is derived from the
// vendored redis-rs fork's `implement_commands!` invocation
// (`redis` crate v0.25.2, BSD-3-Clause license, valkey-io/valkey-glide rev
// 052ae4e; see licenses/LICENSE.redis-rs and NOTICE).
// Each method delegates to the fork's generated `Cmd::<name>()` constructor,
// so the wire encoding is byte-identical to redis-rs.
//
// GENERATED by tools/gen_compat_commands.py — do not edit by hand.
//! GLIDE-owned drop-in replacements for the redis-rs `AsyncCommands` /
//! `Commands` traits, with **native copy behavior**.
//!
//! Signature-identical to the fork's traits (same names, same generic
//! parameter order — existing call sites and turbofish annotations compile
//! unchanged), but each method builds the command via the fork's own
//! `Cmd::<name>()` constructor and hands it **by value** to glide-core,
//! skipping the `&Cmd` -> clone tax of the `ConnectionLike` dispatch path.
//! A 10MB `SET` through these traits copies the payload exactly as many
//! times as the native GLIDE API.
//!
//! The fork's original traits remain implemented (via
//! [`redis::aio::ConnectionLike`]) and reachable as
//! [`crate::redis::AsyncCommands`] / [`crate::redis::Commands`] for code that
//! needs the literal redis-rs traits (e.g. generic functions bounded on
//! them). Do not import both flavors in one scope — method calls would be
//! ambiguous.

use redis::{
    Cmd, Direction, Expiry, FromRedisValue, LposOptions, RedisFuture, RedisResult, SetOptions,
    ToRedisArgs, Value, cmd, from_owned_redis_value,
};

/// The **async** redis-rs command surface with native copy behavior.
///
/// Drop-in for the fork's `redis::AsyncCommands`: same method names,
/// signatures, and semantics; commands are sent by value (no per-call `Cmd`
/// clone). Implemented by [`crate::GlideClient`] and
/// [`crate::GlideClusterClient`].
pub trait AsyncCommands: redis::aio::ConnectionLike + Send + Sized {
    /// Send an already-built command **by value** (no clone). This is the
    /// single required method; every typed command delegates to it. Also
    /// useful directly as a zero-extra-copy escape hatch for custom commands
    /// with large payloads.
    fn glide_send_owned<'a>(&'a mut self, cmd: Cmd) -> RedisFuture<'a, Value>;

'''

sync_header = '''\
}

/// The **blocking** redis-rs command surface with native copy behavior.
///
/// Drop-in for the fork's `redis::Commands` — see [`AsyncCommands`].
/// Implemented by [`crate::sync::SyncGlideClient`] and
/// [`crate::sync::SyncGlideClusterClient`].
///
/// These methods block on the internal runtime and **must not be called from
/// within an async context** (doing so panics with tokio's "cannot block the
/// current thread from within a runtime"); use [`AsyncCommands`] on the async
/// clients there instead.
#[cfg(feature = "sync")]
pub trait Commands: redis::ConnectionLike + Sized {
    /// Send an already-built command **by value** (no clone). This is the
    /// single required method; every typed command delegates to it.
    fn glide_send_owned_sync(&mut self, cmd: Cmd) -> RedisResult<Value>;

'''

scan_async = '''
    /// Incrementally iterate the keys space.
    #[inline]
    fn scan<RV: FromRedisValue>(
        &mut self,
    ) -> RedisFuture<'_, redis::AsyncIter<'_, RV>> {
        let mut c = cmd("SCAN");
        c.cursor_arg(0);
        Box::pin(async move { c.iter_async(self).await })
    }

    /// Incrementally iterate the keys space for keys matching a pattern.
    #[inline]
    fn scan_match<P: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        pattern: P,
    ) -> RedisFuture<'_, redis::AsyncIter<'_, RV>> {
        let mut c = cmd("SCAN");
        c.cursor_arg(0).arg("MATCH").arg(pattern);
        Box::pin(async move { c.iter_async(self).await })
    }

    /// Incrementally iterate hash fields and associated values.
    #[inline]
    fn hscan<K: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        key: K,
    ) -> RedisFuture<'_, redis::AsyncIter<'_, RV>> {
        let mut c = cmd("HSCAN");
        c.arg(key).cursor_arg(0);
        Box::pin(async move { c.iter_async(self).await })
    }

    /// Incrementally iterate hash fields and associated values for field
    /// names matching a pattern.
    #[inline]
    fn hscan_match<K: ToRedisArgsBound, P: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        key: K,
        pattern: P,
    ) -> RedisFuture<'_, redis::AsyncIter<'_, RV>> {
        let mut c = cmd("HSCAN");
        c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
        Box::pin(async move { c.iter_async(self).await })
    }

    /// Incrementally iterate set elements.
    #[inline]
    fn sscan<K: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        key: K,
    ) -> RedisFuture<'_, redis::AsyncIter<'_, RV>> {
        let mut c = cmd("SSCAN");
        c.arg(key).cursor_arg(0);
        Box::pin(async move { c.iter_async(self).await })
    }

    /// Incrementally iterate set elements for elements matching a pattern.
    #[inline]
    fn sscan_match<K: ToRedisArgsBound, P: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        key: K,
        pattern: P,
    ) -> RedisFuture<'_, redis::AsyncIter<'_, RV>> {
        let mut c = cmd("SSCAN");
        c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
        Box::pin(async move { c.iter_async(self).await })
    }

    /// Incrementally iterate sorted set elements.
    #[inline]
    fn zscan<K: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        key: K,
    ) -> RedisFuture<'_, redis::AsyncIter<'_, RV>> {
        let mut c = cmd("ZSCAN");
        c.arg(key).cursor_arg(0);
        Box::pin(async move { c.iter_async(self).await })
    }

    /// Incrementally iterate sorted set elements for elements matching a
    /// pattern.
    #[inline]
    fn zscan_match<K: ToRedisArgsBound, P: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        key: K,
        pattern: P,
    ) -> RedisFuture<'_, redis::AsyncIter<'_, RV>> {
        let mut c = cmd("ZSCAN");
        c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
        Box::pin(async move { c.iter_async(self).await })
    }
'''

scan_sync = '''
    /// Incrementally iterate the keys space.
    #[inline]
    fn scan<RV: FromRedisValue>(&mut self) -> RedisResult<redis::Iter<'_, RV>> {
        let mut c = cmd("SCAN");
        c.cursor_arg(0);
        c.iter(self)
    }

    /// Incrementally iterate the keys space for keys matching a pattern.
    #[inline]
    fn scan_match<P: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        pattern: P,
    ) -> RedisResult<redis::Iter<'_, RV>> {
        let mut c = cmd("SCAN");
        c.cursor_arg(0).arg("MATCH").arg(pattern);
        c.iter(self)
    }

    /// Incrementally iterate hash fields and associated values.
    #[inline]
    fn hscan<K: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        key: K,
    ) -> RedisResult<redis::Iter<'_, RV>> {
        let mut c = cmd("HSCAN");
        c.arg(key).cursor_arg(0);
        c.iter(self)
    }

    /// Incrementally iterate hash fields and associated values for field
    /// names matching a pattern.
    #[inline]
    fn hscan_match<K: ToRedisArgsBound, P: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        key: K,
        pattern: P,
    ) -> RedisResult<redis::Iter<'_, RV>> {
        let mut c = cmd("HSCAN");
        c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
        c.iter(self)
    }

    /// Incrementally iterate set elements.
    #[inline]
    fn sscan<K: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        key: K,
    ) -> RedisResult<redis::Iter<'_, RV>> {
        let mut c = cmd("SSCAN");
        c.arg(key).cursor_arg(0);
        c.iter(self)
    }

    /// Incrementally iterate set elements for elements matching a pattern.
    #[inline]
    fn sscan_match<K: ToRedisArgsBound, P: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        key: K,
        pattern: P,
    ) -> RedisResult<redis::Iter<'_, RV>> {
        let mut c = cmd("SSCAN");
        c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
        c.iter(self)
    }

    /// Incrementally iterate sorted set elements.
    #[inline]
    fn zscan<K: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        key: K,
    ) -> RedisResult<redis::Iter<'_, RV>> {
        let mut c = cmd("ZSCAN");
        c.arg(key).cursor_arg(0);
        c.iter(self)
    }

    /// Incrementally iterate sorted set elements for elements matching a
    /// pattern.
    #[inline]
    fn zscan_match<K: ToRedisArgsBound, P: ToRedisArgsBound, RV: FromRedisValue>(
        &mut self,
        key: K,
        pattern: P,
    ) -> RedisResult<redis::Iter<'_, RV>> {
        let mut c = cmd("ZSCAN");
        c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
        c.iter(self)
    }
'''

out = [header]
for docs, attrs, name, generics, args in entries:
    out.append(method(docs, attrs, name, generics, args, True))
    out.append("")
out.append(scan_async.replace("ToRedisArgsBound", "redis::ToRedisArgs"))
out.append(sync_header)
for docs, attrs, name, generics, args in entries:
    out.append(method(docs, attrs, name, generics, args, False))
    out.append("")
out.append(scan_sync.replace("ToRedisArgsBound", "redis::ToRedisArgs"))
out.append("}")
open("src/compat_commands.rs", "w").write("\n".join(out) + "\n")

# Format so the committed file is byte-identical to `cargo fmt` output.
if shutil.which("rustfmt"):
    subprocess.run(
        ["rustfmt", "--edition", "2024", "src/compat_commands.rs"], check=True
    )
else:
    print("WARNING: rustfmt not found — run `cargo fmt` before committing", file=sys.stderr)

print(f"generated src/compat_commands.rs with {len(entries)} methods per trait")
