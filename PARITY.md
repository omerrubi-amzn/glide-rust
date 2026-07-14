# redis-rs API Parity — Analysis & Plan

**Goal:** complete API parity with redis-rs so customers can migrate easily between the
two clients.

**Reference:** the **vendored redis-rs fork, version 0.25.2** (MIT-licensed,
*pre*-relicense) inside `valkey-io/valkey-glide` at the pinned rev this crate builds
against (`052ae4ef62b3ab055ce991d74ea27a5cf1762a20`), i.e. the same `redis` crate
instance we already link. We must **not** compare against (or copy from) upstream
redis-rs releases published after its licensing change.

> Source of truth used for this analysis:
> `~/.cargo/git/checkouts/valkey-glide-*/052ae4e/glide-core/redis-rs/redis/src/`

---

## 1. The pivotal architectural finding

Our wrapper links the **same `redis` crate instance** as `glide-core` (same repo+rev
git dependency), so `Cmd`, `Value`, `ToRedisArgs`, `FromRedisValue`, `RedisError`
unify across the boundary. In the fork:

```rust
// commands/mod.rs
impl<T> Commands for T where T: ConnectionLike {}
impl<T> AsyncCommands for T where T: crate::aio::ConnectionLike + Send + Sized {}
```

The **entire** redis-rs typed API — ~145 generated methods with the exact
`fn name<K: ToRedisArgs, ..., RV: FromRedisValue>(&mut self, ...) -> RedisResult<RV>`
signatures, plus the `scan` / `scan_match` / `hscan` / `sscan` / `zscan` iterator
methods — is blanket-implemented over **one trait**: `redis::aio::ConnectionLike`
(`req_packed_command`, `req_packed_commands`, `get_db`, `is_closed`).

`glide_core::client::Client` already exposes the matching primitives:
`send_command`, `send_pipeline`, `send_transaction` (all `RedisFuture<Value>`), and
is `Clone` (Arc inside).

**Conclusion:** true, exact-signature parity is achievable *without reimplementing
145 methods*: implement `aio::ConnectionLike` on a small adapter over our client and
`AsyncCommands`, `Pipeline::query_async`, and the scan iterators come for free —
with redis-rs's exact generics and `RedisResult` error type.

---

## 2. Command-surface audit (completed)

- redis-rs `Commands`/`AsyncCommands` (`implement_commands!` macro): ~145 typed
  methods → **112 distinct server commands**.
- Our wrapper: 369 methods across family traits (naming mirrors **Python GLIDE**),
  plus `json`/`ft` families the fork has no typed methods for.

### 2.1 Functional parity: already complete

Extracting the actual command strings both sides issue, only 4 fork commands had no
static reference in our source — all covered by modern equivalents:

| fork command | our coverage |
|---|---|
| `HMSET` (deprecated) | `hset(key, &[(f, v)])` (HSET is variadic since 4.0) |
| `RPOPLPUSH` (deprecated) | `lmove` (+ `brpoplpush` for blocking) |
| `ZREVRANGE` | `zrange_by_index(..., rev = true)` |
| `ZREVRANGEBYLEX` | `zrangebylex` / `zrangestore_by_lex` with `REV` |

`custom_command` / `custom_command_with_route` guarantee 100% functional coverage
regardless.

### 2.2 Naming/signature parity: 55 exact-name mismatches

Two buckets, neither missing functionality:

1. **Naming-convention diffs** (~20): `get_del`/`get_ex`/`hset_multiple`/`hincr`/
   `expire_at`/`pexpire_at`/`rename_nx`/`set_nx`/`mset_nx`/`set_ex`/`pset_ex`/
   `set_multiple`/`lpush_exists`/`rpush_exists`/`linsert_before`/`linsert_after`/
   `srandmember_multiple`/`zincr`/`zadd_multiple`/`zscore_multiple`/`zrembylex`/
   `zrembyscore`/`keys` vs our `getdel`/`getex`/`hset`/`hincrby`/`expireat`/…
2. **Variant-expansion** (~35): the fork splits one command into suffixed methods
   (`zinterstore{,_min,_max,_weights,_min_weights,_max_weights}`,
   `zunionstore{...}`, `zrangebyscore{,_limit,_withscores,_limit_withscores}`,
   `zrevrange{,_withscores}`, `zrevrangebylex{,_limit}`, `zmpop_min/max`,
   `bzmpop_min/max`, `bit_and/or/xor/not`, `bitcount_range`) — we collapse each
   into one method + options struct/enum (Python-GLIDE style).

Structural difference: fork methods are **generic over the return type**
(`-> RedisResult<RV>`); ours return **concrete typed results**
(`-> Result<Option<Bytes>>` etc.). These philosophies cannot merge in one method —
hence the adapter approach (§1) rather than rewriting the native API.

---

## 3. Client establishment (CMD/CME) & networking

Our wrapper deliberately builds on **glide-core's** connection stack (topology,
reconnection, routing); redis-rs's `Client::open` / `ConnectionManager` /
`MultiplexedConnection` / `ClusterClientBuilder` machinery is bypassed **by design**.
Parity here means "can the same connection intent be expressed", not cloning shapes.

**Covered by our config builders:** TLS mode, credentials, `read_from`/replica
strategy, request & connection timeouts, RESP2/3 protocol, `database_id`,
`client_name`, reconnect backoff, periodic topology checks. **Extras the fork's
public builder lacks:** `inflight_requests_limit`, `lazy_connect`, IAM auth,
AZ-affinity read strategies, declarative pub/sub subscriptions.

**Verified gaps:**

| # | Gap | Notes |
|---|---|---|
| N1 | **No `from_url` / `IntoConnectionInfo`** | fork: `Client::open("redis://user:pass@host:6379/0")`, `rediss://`. Biggest migration ergonomic gap. |
| N2 | **No mTLS client certificates** | our `TlsConfig` = `NoTls`/`SecureTls`/`InsecureTls` + CA cert only; fork's `TlsCertificates` carries a client identity. Needs a glide-core capability check (possible upstream dependency). |
| N3 | `tcp_nodelay` not exposed | core may set it internally (unverified). |
| N4 | `address_resolver` (custom DNS) not exposed | niche. |
| N5 | Client-side / server-assisted caching toggles | fork exposes; on GLIDE 2.4 roadmap. |
| N6 | Per-request retry knobs (`retries`, `min/max_retry_wait`, `retry_wait_formula`) | we expose reconnect backoff only — different semantics. |

---

## 4. Other surfaces

| Surface | Fork | Us | Verdict |
|---|---|---|---|
| Pipeline/transaction | `Pipeline`, `.atomic()`, `query_async`, per-method shortcuts | `Batch` (+`BatchOptions`), `add_cmd`, few shortcuts | Adapter (§1) bridges `Pipeline` → `send_pipeline`/`send_transaction`. |
| Scripting | **`Script` type removed in the fork** (upstream 0.25 has it) | `eval`/`evalsha`/`script_*`/`fcall`/`function_*` (broader) | Re-implement a clean-room `Script` convenience type (§5 B3). |
| Geo/streams typed modules | **removed in the fork** (upstream 0.25 has `redis::geo`, `redis::streams`) | native option structs exist | Covered natively; note fork ≠ upstream. |
| Pub/Sub | dedicated `PubSub` connection, `get_message()` | client-integrated `get_pubsub_message`, runtime (p/s)subscribe, auto-resubscribe | Structural, by design. Naming shims only. |
| Errors | `RedisError` + ~87 `ErrorKind`s, `RedisResult` | `GlideError` (7 variants), `Result` | Adapter path preserves `RedisError` exactly; native API keeps `GlideError`. |
| Type conversion | `ToRedisArgs`/`FromRedisValue`/`Value` | **identical — same crate instance** | Strongest parity point. |
| Sentinel | `sentinel.rs` | unsupported by glide-core | Out of scope. |
| Async runtime | tokio (+async-std upstream) | tokio only | Accepted. |

---

## 5. Blockers & recommendations

**B1 — Value normalization (the one real behavioral blocker).**
glide-core's send path applies `convert_to_expected_type()`
(`glide-core/src/client/mod.rs` ~1014) before returning, so replies are
GLIDE-normalized, not raw RESP. The fork's `FromRedisValue` impls expect raw shapes
(e.g. RESP2 `HGETALL` → flat array → `HashMap`); normalization may pre-convert to
`Value::Map` or typed doubles, changing what `RV` decoding sees. Some conversions
coincide, others won't.
*Recommendation:* differential test matrix (112 commands × RESP2/RESP3 × typical
`RV` targets) to find real mismatches; then either add a raw-passthrough/bypass to
the adapter's send path (minor glide-core change or a conversion-bypass list) or
document the deltas. **Do this first — it gates the adapter's drop-in claim.**

**B2 — `&mut self` + error types.** `ConnectionLike` takes `&mut self`; our clients
are `&self`. Not a blocker: the adapter holds a `Clone` of the core client. Adapter
returns `RedisResult`; native API keeps `GlideError` (which collapses ~87
`ErrorKind`s into 7 variants — users matching on `ErrorKind` should use the adapter).

**B3 — Fork ≠ upstream 0.25.** The fork removed `Script` and the `geo`/`streams`
typed modules. Perfect fork-parity still leaves upstream migrants missing
`redis::Script`.
*Recommendation:* treat the **fork surface as the parity contract**; additionally
re-implement `Script` clean-room (~100 lines: `new`/`prepare_invoke`/`invoke`
with SHA caching over our `evalsha`→`eval` fallback).

**B4 — Sync `Commands`.** Needs the *sync* `ConnectionLike` (different trait,
packed bytes). Our sync layer already wraps async via `block_on`; a sync adapter is
mechanical but separate. *Recommendation:* phase 2; async first.

**B5 — Establishment gaps (N1–N6).** *Recommendation:* implement `from_url()` (N1)
mapping `redis://`/`rediss://` URLs → our configs — high value, no blockers. mTLS
(N2) pending glide-core capability check. N3–N6 opt-in later.

**B6 — Pub/Sub model.** Keep our integrated model (core owns resubscription);
provide naming shims (`get_message` alias) only.

---

## 6. Plan

> **Design decision (2026-07-14):** no separate adapter/compat type. Our clients
> **are** redis-rs connection objects: `GlideClient` and `GlideClusterClient`
> implement `redis::aio::ConnectionLike` directly, so `AsyncCommands`,
> `Pipeline::query_async`, and the scan iterators work on them as-is. The
> needed redis-rs symbols (`AsyncCommands`, `Pipeline`, `pipe`, `cmd`,
> `RedisResult`, `RedisError`, `ErrorKind`, `FromRedisValue`, `ToRedisArgs`)
> are re-exported at the crate root since the vendored `redis` fork is a
> transitive git dependency downstream crates cannot name.

| Phase | Work | Status |
|---|---|---|
| **0** | B1 spike: normalized-value decoding vs `FromRedisValue` | ✅ **Done.** Static: the fork's `FromRedisValue` accepts `Boolean`/`Double`/`Map`/`Set`. Live: differential tests (maps, sets, doubles, booleans, pairs, streams, geo, CONFIG GET, LMPOP) pass on RESP2+RESP3 × standalone+cluster. |
| **1** | `redis::aio::ConnectionLike` **implemented directly on both clients**; unlocks `AsyncCommands`, `Pipeline` (bridged to `send_pipeline`/`send_transaction`), scan iterators | ✅ **Done** (`src/client.rs`, tests in `tests/it_redis_rs_api.rs`). |
| **2** | Establishment ergonomics: `from_url` / `from_connection_info` / `from_urls` (redis-rs URL semantics via the fork's `IntoConnectionInfo`); `client_identity()` **mutual TLS** on both configs (lowered to `ConnectionRequest::client_cert`/`client_key`) | ✅ **Done** (`src/config.rs`; unit + live tests). |
| **3** | ~~Native-API naming aliases for the 55 methods~~ | ❌ Dropped — direct trait impl provides exact parity. |
| **4** | Clean-room `Script`/`ScriptInvocation` (`src/script.rs`: SHA-1 caching, EVALSHA→EVAL `NOSCRIPT` fallback, works on both clients); **blocking `Commands`** via sync `redis::ConnectionLike` on `SyncGlideClient`/`SyncGlideClusterClient` (`src/sync/mod.rs`: `req_command` fast path, packed-byte pipeline decode, MULTI/EXEC → core transactions) | ✅ **Done.** |
| **5** | Verification: parity suites `tests/it_redis_rs_api.rs` + `tests/it_redis_rs_extra.rs` (87 live executions), doc examples | ✅ **Done** — full suite 1215 tests green, clippy + fmt clean. |

### Remaining known gaps (accepted / deferred)

- **N3–N6** (`tcp_nodelay` is in fact always enabled by our `base_request`;
  `address_resolver`, caching toggles, per-request retry knobs): deferred —
  niche, no customer ask.
- **Pub/Sub model**: intentionally kept client-integrated (core owns
  resubscription); no `PubSub` connection object.
- **Sentinel / async-std / unix sockets**: unsupported by glide-core; out of
  scope (`from_url` rejects unix-socket URLs with a clear error).

### Known limitations of the direct impl

- Our native traits (`StringCommands`, …) and `AsyncCommands` share method
  names; importing both in one scope requires fully-qualified disambiguation.
- `is_closed()` reports `false` (glide-core owns reconnection; there is no
  observable closed state).
- Scan iterators run against the connected node — for cluster-wide iteration
  use our native `cluster_scan`.

### Payload copy behavior (owned-send traits)

`glide::AsyncCommands` / `glide::Commands` are **GLIDE-owned drop-in
replacements** for the fork's traits (generated from the fork's own
`implement_commands!` table — see `tools/gen_compat_commands.py`): identical
names, generic order (turbofish-compatible), and wire encoding (methods
delegate to the fork's `Cmd::<name>()` constructors), but the built `Cmd` is
handed to glide-core **by value** via `glide_send_owned`, eliminating the
`&Cmd → clone` tax of the `ConnectionLike` dispatch path.

This is a deliberate softening of "hard" trait parity: the exported traits are
not literally `redis::AsyncCommands`/`redis::Commands` (those remain
implemented via `ConnectionLike` and reachable at `glide::redis::…` for
generic code bounded on them). Call sites compile unchanged either way.

Copies of an N-byte payload before wire serialization (glide-core internally
clones every single command once — `client/mod.rs:1138` at the pinned rev; an
upstream by-value `send_command` would remove that for all bindings):

| Path | Copies |
|---|---|
| Native `Batch` / compat `Pipeline::query_async` | 1 |
| Native typed methods (`StringCommands::set`, …) | 2 |
| **Compat typed methods (`glide::AsyncCommands` / `Commands`)** | **2 (= native)** |
| Fork traits via `ConnectionLike` (`glide::redis::AsyncCommands`) | 3 |
| Sync compat `Pipeline::query` (packed-byte round-trip) | 3 |
| `Script` invoke | 3 (interleaved key/arg buffers; redis-rs-inherent +1) |

Measured (10MB SET, loopback, release): native 25.18 ms ≈ owned-send compat
25.30 ms; fork `ConnectionLike` path 25.89 ms (+~0.7 ms = the extra copy).
Read paths have zero extra copies everywhere. For large values prefer the
typed traits or `Batch`; the escape hatch `glide_send_owned(cmd)` sends any
custom command with zero extra copies.

### Semver implication of re-exporting the fork

The crate re-exports the vendored `redis` fork wholesale (`glide::redis::…`,
plus curated flat re-exports), which makes the fork's public API part of
**this crate's** public API surface. Consequently, **bumping the pinned fork
rev is a potentially breaking change** of glide-rust and must be treated as
such under semver (major/minor accordingly), even when no wrapper code
changes.

### Open decisions

1. Is the **adapter** (exact redis-rs API, opt-in) the parity story, making the 55
   native aliases unnecessary? *(Recommended: yes.)*
2. Parity target = **fork surface** (recommended) vs upstream 0.25 surface
   (adds clean-room `Script`, geo/streams types).
3. Include the fork's deprecated methods (`hmset`, `rpoplpush`) in any native
   aliases? (The adapter provides them automatically.)

---

*Analysis performed 2026-07-14 against fork rev `052ae4e` (redis crate v0.25.2) and
wrapper repo `omerrubi-amzn/glide-rust`.*
