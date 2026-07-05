// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Shared integration-test harness.
//!
//! Provides:
//! * [`TestServer`] — boots an ephemeral standalone `valkey-server` on a free
//!   port and tears it down on drop, with RESP2/RESP3 client helpers.
//! * [`ClusterHarness`] — boots a real 3-primary cluster and connects a
//!   [`glide::GlideClusterClient`]. The canonical tool for this is
//!   `valkey-glide/utils/cluster_manager.py`
//!   (`python3 cluster_manager.py start --cluster-mode`), which we document and
//!   prefer; however it requires `valkey-cli` on `PATH`. When that is not
//!   available we build the cluster natively from the `valkey-server` binary
//!   (`CLUSTER ADDSLOTSRANGE` + `CLUSTER MEET`), so cluster tests still run.
//!   When neither is feasible the harness returns `None` and tests SKIP.
//! * The [`resp_test!`] macro — expands a single test body into two
//!   `#[tokio::test]`s, one per RESP protocol version (the ~2x multiplier;
//!   combined with standalone the effective coverage mirrors Python's
//!   RESP2/RESP3 parametrization).
//! * [`assert_request_error!`] — asserts a result is a server `RequestError`.

#![allow(dead_code)]

use glide::{
    GlideClient, GlideClientConfiguration, GlideClusterClient, GlideClusterClientConfiguration,
    ProtocolVersion, Route,
};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Per-test wall-clock timeout guard (G1)
// ---------------------------------------------------------------------------

/// Wall-clock budget for a single test. A wedged `await` — e.g. a pub/sub
/// receive that never arrives, or a cluster op that never returns — would
/// otherwise block the whole CI job indefinitely. This bounds every test the
/// way glide-core's `#[timeout(...)]` attribute does.
///
/// Overridable via `GLIDE_TEST_TIMEOUT_SECS` (default 120s — generous so it
/// never false-trips under `llvm-cov` instrumentation, yet still catches a
/// genuine hang, which is unbounded).
pub fn test_timeout() -> Duration {
    let secs = std::env::var("GLIDE_TEST_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(120);
    Duration::from_secs(secs)
}

/// Run a test future under [`test_timeout`], panicking (failing the test) if it
/// does not complete in time — so a hang surfaces as a fast, clear failure
/// instead of a stuck CI job. Used automatically by [`resp_test!`],
/// [`matrix_test!`] and [`timed_tokio_test!`].
pub async fn with_test_timeout<F: std::future::Future>(fut: F) -> F::Output {
    let budget = test_timeout();
    match tokio::time::timeout(budget, fut).await {
        Ok(v) => v,
        Err(_) => panic!(
            "test exceeded {budget:?} wall-clock timeout \
             (set GLIDE_TEST_TIMEOUT_SECS to adjust)"
        ),
    }
}

/// Define a `#[tokio::test]` whose body is bounded by [`with_test_timeout`].
/// Use for hand-written async tests; the `resp_test!` / `matrix_test!` macros
/// apply the same guard automatically.
///
/// ```ignore
/// timed_tokio_test!(async fn my_test() {
///     let srv = server_or_skip!();
///     // ...
/// });
/// ```
#[macro_export]
macro_rules! timed_tokio_test {
    (async fn $name:ident() $body:block) => {
        #[tokio::test]
        async fn $name() {
            $crate::common::with_test_timeout(async move $body).await;
        }
    };
}

/// Locate a usable `valkey-server`/`redis-server` binary. Set the
/// `VALKEY_SERVER_PATH` environment variable to point at a specific binary;
/// otherwise the first `valkey-server`/`redis-server` found on `PATH` is used.
/// Returns `None` (so tests SKIP) when no binary is available.
pub fn server_binary() -> Option<String> {
    if let Ok(p) = std::env::var("VALKEY_SERVER_PATH")
        && std::path::Path::new(&p).exists()
    {
        return Some(p);
    }
    for name in ["valkey-server", "redis-server"] {
        if let Ok(output) = Command::new("which").arg(name).output()
            && output.status.success()
        {
            let p = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !p.is_empty() {
                return Some(p);
            }
        }
    }
    None
}

/// Grab a currently-free TCP port on loopback.
pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().unwrap().port()
}

/// A process-and-thread-unique key with the given prefix, so tests never
/// collide even when running concurrently across RESP2/RESP3 variants.
pub fn key(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}:{t}:{n}")
}

/// A process-unique key wrapped in a Valkey **hash tag** so that all keys sharing
/// the same `tag` map to the same cluster slot. Required for multi-key commands
/// (MSET/MGET, RENAME, SINTERSTORE, …) to be valid in cluster mode; harmless in
/// standalone (the braces are just part of the key name). Example:
/// `tkey("grp", "a")` -> `{grp}:a:<ts>:<n>`.
pub fn tkey(tag: &str, name: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{{{tag}}}:{name}:{t}:{n}")
}

/// Parse a `"major.minor.patch"` version string into a tuple.
fn parse_version(s: &str) -> Option<(u32, u32, u32)> {
    let mut it = s.trim().split('.');
    let major = it.next()?.parse().ok()?;
    let minor = it.next().unwrap_or("0").parse().unwrap_or(0);
    // patch may carry a suffix (e.g. "3-rc1"); take leading digits only.
    let patch = it
        .next()
        .unwrap_or("0")
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .unwrap_or(0);
    Some((major, minor, patch))
}

/// Recursively collect all string-shaped content from a [`glide::Value`] into
/// `out`. Handles the flat bulk string a standalone `INFO` returns AND the
/// multi-node Map/Array a cluster client returns (so the version can be found in
/// either shape).
fn collect_value_text(v: &glide::Value, out: &mut String) {
    use glide::Value;
    match v {
        Value::BulkString(b) => {
            out.push_str(&String::from_utf8_lossy(b));
            out.push('\n');
        }
        Value::SimpleString(s) => {
            out.push_str(s);
            out.push('\n');
        }
        Value::VerbatimString { text, .. } => {
            out.push_str(text);
            out.push('\n');
        }
        Value::Array(items) | Value::Set(items) => {
            for it in items {
                collect_value_text(it, out);
            }
        }
        Value::Map(pairs) => {
            for (k, val) in pairs {
                collect_value_text(k, out);
                collect_value_text(val, out);
            }
        }
        _ => {}
    }
}

/// Query the connected server's version via `INFO server`. Works on standalone
/// AND cluster clients: `custom_command` returns the raw reply, which we walk to
/// find `valkey_version:` / `redis_version:` regardless of whether it is a flat
/// bulk string (standalone) or a per-node Map (cluster).
pub async fn server_version<C>(c: &C) -> Option<(u32, u32, u32)>
where
    C: glide::CustomCommand + Sync,
{
    let reply = c.custom_command(&["INFO", "server"]).await.ok()?;
    let mut text = String::new();
    collect_value_text(&reply, &mut text);
    // Prefer `valkey_version` — Valkey pins `redis_version` to a compat value
    // (7.2.4) on ALL releases, so redis_version is useless for gating on Valkey.
    // Fall back to redis_version only when there is no valkey_version (real Redis).
    for key in ["valkey_version:", "redis_version:"] {
        for line in text.lines() {
            if let Some(v) = line.trim().strip_prefix(key) {
                return parse_version(v);
            }
        }
    }
    None
}

/// Whether the server recognises `name` (via `COMMAND INFO`). This is a
/// version- and product-agnostic capability check — more robust than version
/// math for commands whose availability differs between Redis and Valkey
/// releases (e.g. hash-field TTL). Fails **closed** (returns `false`) if the
/// capability cannot be determined, so gated tests SKIP rather than error.
pub async fn command_exists<C>(c: &C, name: &str) -> bool
where
    C: glide::CustomCommand + Sync,
{
    match c.custom_command(&["COMMAND", "INFO", name]).await {
        Ok(v) => command_info_present(&v),
        Err(_) => false,
    }
}

/// `COMMAND INFO <name>` returns `[[ <details> ]]` when known and `[nil]` when
/// unknown. On cluster it may be a per-node Map. Present ⇔ a non-empty details
/// array exists somewhere in the reply.
fn command_info_present(v: &glide::Value) -> bool {
    use glide::Value;
    match v {
        Value::Array(items) => items
            .iter()
            .any(|it| matches!(it, Value::Array(inner) if !inner.is_empty())),
        Value::Map(pairs) => pairs.iter().any(|(_, val)| command_info_present(val)),
        _ => false,
    }
}

/// True when the server version is strictly below `min`. Returns `false` if the
/// version cannot be determined (fail-open: run the test rather than skip).
pub async fn version_below<C>(c: &C, min: (u32, u32, u32)) -> bool
where
    C: glide::CustomCommand + Sync,
{
    match server_version(c).await {
        Some(v) => v < min,
        None => false,
    }
}

/// Block until `port` accepts a TCP connection, or `deadline` elapses.
fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(30));
    }
    false
}

/// Extract a numeric `CLUSTER INFO` field value, e.g.
/// `cluster_info_field(info, "cluster_known_nodes") == Some(3)`.
fn cluster_info_field(info: &str, field: &str) -> Option<u64> {
    for line in info.lines() {
        if let Some(rest) = line.trim().strip_prefix(field)
            && let Some(v) = rest.strip_prefix(':')
        {
            return v.trim().parse().ok();
        }
    }
    None
}

/// Whether a native-cluster node reports FULL convergence — not merely
/// `cluster_state:ok`, which alone can precede a settled slot map / peer view
/// and let a routed command hit a `MOVED` to a not-yet-known node. We gate on
/// three `CLUSTER INFO` fields (mirroring upstream `cluster_manager.py`'s
/// slot-coverage + `cluster_state:ok` + topology-views checks):
///   * `cluster_state:ok`             — node considers the cluster usable;
///   * `cluster_slots_assigned:16384` — full slot coverage (G7);
///   * `cluster_known_nodes:<shards>` — node sees the whole topology (G2).
fn node_fully_converged(port: u16, shards: usize) -> bool {
    let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
    let Ok(info) = raw_cmd(&mut s, &["CLUSTER", "INFO"]) else {
        return false;
    };
    info.contains("cluster_state:ok")
        && cluster_info_field(&info, "cluster_slots_assigned") == Some(16384)
        && cluster_info_field(&info, "cluster_known_nodes") == Some(shards as u64)
}

// ---------------------------------------------------------------------------
// Standalone server
// ---------------------------------------------------------------------------

/// A running standalone server; killed on drop.
pub struct TestServer {
    child: Child,
    pub port: u16,
}

impl TestServer {
    /// Start a fresh standalone server, or `None` when no binary is available.
    pub fn start() -> Option<TestServer> {
        Self::start_with_args(&[])
    }

    /// Start a standalone server with extra CLI arguments (e.g. `--requirepass`).
    pub fn start_with_args(extra: &[&str]) -> Option<TestServer> {
        let bin = server_binary()?;
        let port = free_port();
        let mut args: Vec<String> = vec![
            "--port".into(),
            port.to_string(),
            "--bind".into(),
            "127.0.0.1".into(),
            "--save".into(),
            "".into(),
            "--appendonly".into(),
            "no".into(),
            "--daemonize".into(),
            "no".into(),
        ];
        args.extend(extra.iter().map(|s| s.to_string()));
        let child = Command::new(&bin)
            .args(&args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        let mut server = TestServer { child, port };
        if wait_for_port(port, Duration::from_secs(10)) {
            std::thread::sleep(Duration::from_millis(100));
            Some(server)
        } else {
            let _ = server.child.kill();
            None
        }
    }

    /// Connect an async client with the default protocol (RESP3).
    pub async fn client(&self) -> GlideClient {
        self.client_with_protocol(ProtocolVersion::RESP3).await
    }

    /// Connect an async client using the given RESP protocol version.
    pub async fn client_with_protocol(&self, protocol: ProtocolVersion) -> GlideClient {
        // Generous connect/request timeouts: under heavy load (e.g. the coverage
        // job runs the whole suite under llvm-cov instrumentation, with many
        // ephemeral servers spawned in parallel), a freshly-started server can be
        // slow to accept — the default connect timeout can then race and fail.
        let config = GlideClientConfiguration::with_address("127.0.0.1", self.port)
            .protocol(protocol)
            .connection_timeout(Duration::from_secs(10))
            .request_timeout(Duration::from_secs(10));
        connect_standalone_with_retry(config).await
    }

    /// Try to connect a client with the given configuration (for auth tests).
    pub async fn try_connect(
        &self,
        config: GlideClientConfiguration,
    ) -> glide::Result<GlideClient> {
        GlideClient::connect(config).await
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Connect a standalone client, retrying on transient connect failures. Under
/// heavy load (e.g. many `llvm-cov`-instrumented ephemeral servers spawned in
/// parallel), a freshly-started server can be briefly slow to accept or finish
/// the handshake; a single attempt can lose that race even with a generous
/// connection timeout. Mirrors glide-core's test connect-retry patch.
async fn connect_standalone_with_retry(config: GlideClientConfiguration) -> GlideClient {
    let mut last_err = None;
    for attempt in 0..10u32 {
        match GlideClient::connect(config.clone()).await {
            Ok(c) => return c,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(100 * (attempt + 1) as u64)).await;
            }
        }
    }
    panic!("connect to test server failed after retries: {last_err:?}");
}

// ---------------------------------------------------------------------------
// Cluster harness
// ---------------------------------------------------------------------------

/// Send a single RESP command over a raw socket and return the raw reply text.
/// Used only for cluster bootstrapping (slot assignment + meet + info polling),
/// which must bypass the standalone client handshake (SELECT is disabled in
/// cluster mode).
fn raw_cmd(stream: &mut TcpStream, args: &[&str]) -> std::io::Result<String> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    stream.write_all(&buf)?;
    let mut resp = [0u8; 65536];
    let n = stream.read(&mut resp)?;
    Ok(String::from_utf8_lossy(&resp[..n]).into_owned())
}

/// Extract the first unsigned integer value for `"field"` in a JSON fragment,
/// e.g. `field_u64(r#""port": 6379,"#, "\"port\"") == Some(6379)`. A tiny
/// dependency-free parser sufficient for cluster_manager.py's `SERVERS_JSON`.
fn field_u64(fragment: &str, field: &str) -> Option<u64> {
    let idx = fragment.find(field)?;
    let after = &fragment[idx + field.len()..];
    let digits: String = after
        .chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits.parse().ok()
}

/// Two backends:
/// * **cluster_manager.py** (preferred, opt-in): when the `GLIDE_CLUSTER_MANAGER`
///   env var points at the canonical `valkey-glide/utils/cluster_manager.py`,
///   the cluster is created with that tool — matching the Python suite and
///   yielding **replicas** (and, in future, TLS). Requires `valkey-cli` on PATH.
/// * **native** (fallback, self-contained): builds a primaries-only cluster
///   directly from `valkey-server` via `CLUSTER ADDSLOTSRANGE` + `MEET`, so the
///   standalone repo needs no external tooling.
pub struct ClusterHarness {
    children: Vec<Child>,
    pub ports: Vec<u16>,
    dir: std::path::PathBuf,
    /// When `Some`, the cluster was created by cluster_manager.py; the value is
    /// the `--cluster-folder` used to stop it.
    managed_folder: Option<String>,
    /// Primary node ports (the seed is `ports[0]`, always a primary).
    pub primary_ports: Vec<u16>,
    /// Replica node ports (empty for the native backend).
    pub replica_ports: Vec<u16>,
}

impl ClusterHarness {
    /// Start a 3-primary cluster (no replicas) and wait for it to converge.
    /// Returns `None` (so tests SKIP) if a server binary is unavailable or the
    /// cluster does not reach `cluster_state:ok` within the timeout.
    pub fn start() -> Option<ClusterHarness> {
        Self::start_shards(3)
    }

    /// Start a cluster with `shards` primaries. Tries cluster_manager.py first
    /// (adds replicas), then the native primaries-only backend.
    pub fn start_shards(shards: usize) -> Option<ClusterHarness> {
        if let Some(h) = Self::start_via_cluster_manager(shards, 1) {
            return Some(h);
        }
        Self::start_native(shards)
    }

    /// Preferred backend: shell out to the canonical `cluster_manager.py`
    /// (pointed to by `GLIDE_CLUSTER_MANAGER`). Returns `None` if the env var is
    /// unset/invalid or the tool fails, so the caller falls back to native.
    fn start_via_cluster_manager(shards: usize, replicas: usize) -> Option<ClusterHarness> {
        let script = std::env::var("GLIDE_CLUSTER_MANAGER").ok()?;
        if !std::path::Path::new(&script).exists() {
            return None;
        }
        let out = Command::new("python3")
            .args([
                &script,
                "start",
                "--cluster-mode",
                "-n",
                &shards.to_string(),
                "-r",
                &replicas.to_string(),
            ])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let stdout = String::from_utf8_lossy(&out.stdout);

        let mut folder: Option<String> = None;
        let mut nodes: Vec<(String, u16)> = Vec::new();
        for line in stdout.lines() {
            if let Some(rest) = line.strip_prefix("CLUSTER_FOLDER=") {
                folder = Some(rest.trim().to_string());
            } else if let Some(rest) = line.strip_prefix("CLUSTER_NODES=") {
                for addr in rest.trim().split(',') {
                    if let Some((h, p)) = addr.rsplit_once(':')
                        && let Ok(port) = p.parse::<u16>()
                    {
                        nodes.push((h.to_string(), port));
                    }
                }
            }
        }
        let folder = folder?;
        if nodes.is_empty() {
            return None;
        }

        // Parse SERVERS_JSON for the primary/replica split.
        let mut primary_ports: Vec<u16> = Vec::new();
        let mut replica_ports: Vec<u16> = Vec::new();
        if let Some(json_line) = stdout.lines().find(|l| l.starts_with("SERVERS_JSON=")) {
            let json = &json_line["SERVERS_JSON=".len()..];
            for obj in json.split('{').skip(1) {
                let port = field_u64(obj, "\"port\"").map(|v| v as u16);
                let is_primary = obj
                    .split_once("\"is_primary\"")
                    .map(|(_, r)| r.contains("true"))
                    .unwrap_or(false);
                if let Some(port) = port {
                    if is_primary {
                        primary_ports.push(port);
                    } else {
                        replica_ports.push(port);
                    }
                }
            }
        }

        // Seed on a primary if we identified one, else the first node.
        let seed = *primary_ports.first().unwrap_or(&nodes[0].1);
        let mut ports: Vec<u16> = vec![seed];
        ports.extend(nodes.iter().map(|(_, p)| *p).filter(|p| *p != seed));
        if primary_ports.is_empty() {
            primary_ports = nodes.iter().map(|(_, p)| *p).collect();
        }

        Some(ClusterHarness {
            children: Vec::new(),
            ports,
            dir: std::path::PathBuf::from(&folder),
            managed_folder: Some(folder),
            primary_ports,
            replica_ports,
        })
    }

    /// Native fallback: build a `shards`-primary cluster directly from
    /// `valkey-server`.
    fn start_native(shards: usize) -> Option<ClusterHarness> {
        let bin = server_binary()?;
        let dir = std::env::temp_dir().join(key("vgr_cluster"));
        std::fs::create_dir_all(&dir).ok()?;

        let ports: Vec<u16> = (0..shards).map(|_| free_port()).collect();
        let mut children = Vec::with_capacity(shards);
        for &port in &ports {
            let node_dir = dir.join(port.to_string());
            if std::fs::create_dir_all(&node_dir).is_err() {
                break;
            }
            let conf = format!("nodes-{port}.conf");
            let mut command = Command::new(&bin);
            command
                .args([
                    "--port",
                    &port.to_string(),
                    "--bind",
                    "127.0.0.1",
                    "--cluster-enabled",
                    "yes",
                    "--cluster-config-file",
                    &conf,
                    "--cluster-node-timeout",
                    "5000",
                    "--dir",
                    node_dir.to_str().unwrap(),
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            match command.spawn() {
                Ok(c) => {
                    children.push(c);
                }
                Err(_) => break,
            }
        }

        let harness = ClusterHarness {
            children,
            ports: ports.clone(),
            dir,
            managed_folder: None,
            primary_ports: ports.clone(),
            replica_ports: Vec::new(),
        };

        // All nodes must be up.
        if harness.children.len() != shards {
            return None;
        }
        for &port in &ports {
            if !wait_for_port(port, Duration::from_secs(10)) {
                return None;
            }
        }

        // Assign slot ranges evenly across primaries, then MEET into node 0.
        let total_slots = 16384u32;
        let per = total_slots / shards as u32;
        for (i, &port) in ports.iter().enumerate() {
            let lo = i as u32 * per;
            let hi = if i == shards - 1 {
                total_slots - 1
            } else {
                (i as u32 + 1) * per - 1
            };
            let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
            s.set_read_timeout(Some(Duration::from_secs(3))).ok()?;
            let reply = raw_cmd(
                &mut s,
                &["CLUSTER", "ADDSLOTSRANGE", &lo.to_string(), &hi.to_string()],
            )
            .ok()?;
            if !reply.contains("OK") {
                return None;
            }
        }
        for &port in &ports[1..] {
            let mut s = TcpStream::connect(("127.0.0.1", ports[0])).ok()?;
            s.set_read_timeout(Some(Duration::from_secs(3))).ok()?;
            let reply =
                raw_cmd(&mut s, &["CLUSTER", "MEET", "127.0.0.1", &port.to_string()]).ok()?;
            if !reply.contains("OK") {
                return None;
            }
        }

        // Wait for FULL convergence before returning the cluster. A node
        // reporting `cluster_state:ok` alone is insufficient: right after
        // formation its slot map / peer view can still be settling, so the first
        // routed op may hit a `MOVED` to a not-yet-known node. We therefore gate
        // on every node reaching full slot coverage AND full topology awareness
        // (see [`node_fully_converged`]) — the source-level fix for the
        // cluster-scan startup race that `warm_up_cluster`/`retry_transient!`
        // otherwise paper over.
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut all_ok = false;
        while Instant::now() < deadline {
            let converged = ports
                .iter()
                .filter(|&&port| node_fully_converged(port, shards))
                .count();
            if converged == ports.len() {
                all_ok = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        if !all_ok {
            return None;
        }
        Some(harness)
    }

    /// The seed `host:port` used to connect a cluster client.
    pub fn seed_port(&self) -> u16 {
        self.ports[0]
    }

    /// Connect a cluster client to this cluster with the given protocol.
    pub async fn client_with_protocol(
        &self,
        protocol: ProtocolVersion,
    ) -> Option<GlideClusterClient> {
        let config = GlideClusterClientConfiguration::with_address("127.0.0.1", self.ports[0])
            .protocol(protocol)
            .request_timeout(Duration::from_secs(5));
        // Bounded connect-retry: under load a freshly-formed cluster can briefly
        // refuse or time out the initial connection; a single attempt shouldn't
        // fail the whole test.
        let mut client = None;
        for attempt in 0..10u32 {
            match GlideClusterClient::connect(config.clone()).await {
                Ok(c) => {
                    client = Some(c);
                    break;
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100 * (attempt + 1) as u64)).await;
                }
            }
        }
        let client = client?;
        // Converge the connection map before handing the client to a test: right
        // after a cluster forms, the client's topology snapshot can lag, so the
        // first routed op may hit a `MOVED` to a not-yet-connected node
        // (`ConnectionNotFoundForRoute`). A retried broadcast PING forces the
        // client to connect to every primary, eliminating that startup race.
        warm_up_cluster(&client).await;
        Some(client)
    }

    /// Connect a cluster client with the default protocol (RESP3).
    pub async fn client(&self) -> Option<GlideClusterClient> {
        self.client_with_protocol(ProtocolVersion::RESP3).await
    }
}

impl Drop for ClusterHarness {
    fn drop(&mut self) {
        if let Some(folder) = &self.managed_folder {
            // Managed backend: stop via cluster_manager.py (best effort).
            if let Ok(script) = std::env::var("GLIDE_CLUSTER_MANAGER") {
                let _ = Command::new("python3")
                    .args([&script, "stop", "--cluster-folder", folder])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
            return;
        }
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Whether an error is a *transient* cluster-topology error — the kind that can
/// occur while a freshly-formed cluster client's slot→node connection map is
/// still converging. Right after a cluster forms, a command can hit a `MOVED`
/// redirect to a node not yet in the connection map, surfacing as
/// `ConnectionNotFoundForRoute`; a topology refresh is triggered but the
/// in-flight op fails. `TRYAGAIN` (multi-key op spanning a slot mid-migration)
/// and `LOADING` (a node still reading its dataset into memory) are likewise
/// transient right after startup. These are all safe to retry.
pub fn is_transient_cluster_error(e: &glide::GlideError) -> bool {
    let m = e.to_string();
    m.contains("ConnectionNotFoundForRoute")
        || m.contains("Requested connection not found")
        || m.contains("connection map")
        || m.contains("MOVED")
        || m.contains("Moved")
        || m.contains("TRYAGAIN")
        || m.contains("TryAgain")
        || m.contains("LOADING")
        || m.contains("Loading")
        || m.contains("loading the dataset")
}

/// Force a freshly-connected cluster client to connect to every primary so its
/// slot→node connection map is fully populated before a test issues routed or
/// scan commands. Best-effort: retries a broadcast `PING` on transient topology
/// errors, and returns once it succeeds (or after a bounded number of attempts /
/// on a non-transient error, leaving the test to surface any real problem).
async fn warm_up_cluster(client: &GlideClusterClient) {
    for attempt in 0..20u32 {
        let mut ping = redis::Cmd::new();
        ping.arg("PING");
        match client.route_command(ping, Route::AllPrimaries).await {
            Ok(_) => return,
            Err(e) if is_transient_cluster_error(&e) => {
                tokio::time::sleep(Duration::from_millis(50 * (attempt + 1) as u64)).await;
            }
            Err(_) => return,
        }
    }
}

/// Extract the subscriber count for `channel` from a `PUBSUB NUMSUB` reply
/// (`[chan, count, ...]` in RESP2, or a map in RESP3).
fn numsub_count(v: &glide::Value, channel: &str) -> Option<i64> {
    use glide::Value;
    let is_chan = |k: &Value| match k {
        Value::BulkString(b) => b.as_slice() == channel.as_bytes(),
        Value::SimpleString(s) => s == channel,
        _ => false,
    };
    match v {
        Value::Array(items) => {
            let mut it = items.iter();
            while let (Some(k), Some(val)) = (it.next(), it.next()) {
                if is_chan(k) {
                    return glide::value::to_i64(val.clone()).ok();
                }
            }
            None
        }
        Value::Map(pairs) => pairs
            .iter()
            .find(|(k, _)| is_chan(k))
            .and_then(|(_, val)| glide::value::to_i64(val.clone()).ok()),
        _ => None,
    }
}

/// Poll `PUBSUB NUMSUB <channel>` on `c` until the subscriber count for
/// `channel` satisfies `pred`, or `timeout` elapses; returns whether the
/// predicate was met. Use instead of a fixed sleep after (un)subscribe so a test
/// proceeds the instant the server has registered the change and never races a
/// slow registration (the Rust analogue of Python's `wait_for_subscription_state`).
pub async fn wait_for_numsub<C, F>(c: &C, channel: &str, mut pred: F, timeout: Duration) -> bool
where
    C: glide::CustomCommand + Sync,
    F: FnMut(i64) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(v) = c.custom_command(&["PUBSUB", "NUMSUB", channel]).await
            && let Some(n) = numsub_count(&v, channel)
            && pred(n)
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll `PUBSUB NUMPAT` on `c` until the total pattern-subscription count
/// satisfies `pred`, or `timeout` elapses.
pub async fn wait_for_numpat<C, F>(c: &C, mut pred: F, timeout: Duration) -> bool
where
    C: glide::CustomCommand + Sync,
    F: FnMut(i64) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(v) = c.custom_command(&["PUBSUB", "NUMPAT"]).await
            && let Ok(n) = glide::value::to_i64(v)
            && pred(n)
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Retry a cluster operation on transient topology errors (see
/// [`is_transient_cluster_error`]) with bounded exponential-ish backoff.
///
/// Usage (pass the future *without* `.await` — the macro awaits internally):
/// ```ignore
/// let (next, keys) = retry_transient!(client.cluster_scan(&cursor, None, Some(100), None)).unwrap();
/// ```
#[macro_export]
macro_rules! retry_transient {
    ($op:expr) => {{
        let mut __attempt: u32 = 0;
        loop {
            match $op.await {
                Ok(v) => break Ok::<_, glide::GlideError>(v),
                Err(e) if __attempt < 15 && $crate::common::is_transient_cluster_error(&e) => {
                    __attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(50 * __attempt as u64))
                        .await;
                }
                Err(e) => break Err(e),
            }
        }
    }};
}

/// Start a standalone server, or `return` from the test (printing SKIP) when no
/// server binary is available.
#[macro_export]
macro_rules! server_or_skip {
    () => {{
        match $crate::common::TestServer::start() {
            Some(s) => s,
            None => {
                eprintln!("SKIP: no valkey-server binary available");
                return;
            }
        }
    }};
}

/// Start a cluster, or `return` from the test (printing SKIP) when a cluster is
/// not feasible in this environment.
#[macro_export]
macro_rules! cluster_or_skip {
    () => {{
        match $crate::common::ClusterHarness::start() {
            Some(h) => h,
            None => {
                eprintln!("SKIP: cluster harness not feasible in this environment");
                return;
            }
        }
    }};
}

/// Expand one test body into two `#[tokio::test]`s — one for RESP2, one for
/// RESP3 — each with its own fresh standalone server bound to `$c`.
///
/// ```ignore
/// resp_test!(get_missing, c, {
///     assert_eq!(c.get(common::key("k")).await.unwrap(), None);
/// });
/// ```
#[macro_export]
macro_rules! resp_test {
    ($name:ident, $c:ident, $body:block) => {
        mod $name {
            use super::*;
            #[allow(unused_imports)]
            use $crate::common;

            #[tokio::test]
            async fn resp2() {
                let __srv = $crate::server_or_skip!();
                let $c = __srv
                    .client_with_protocol(glide::ProtocolVersion::RESP2)
                    .await;
                $crate::common::with_test_timeout(async { $body }).await;
            }

            #[tokio::test]
            async fn resp3() {
                let __srv = $crate::server_or_skip!();
                let $c = __srv
                    .client_with_protocol(glide::ProtocolVersion::RESP3)
                    .await;
                $crate::common::with_test_timeout(async { $body }).await;
            }
        }
    };
}

/// Expand one test body into **four** `#[tokio::test]`s: the cartesian product of
/// {standalone, cluster} × {RESP2, RESP3} — mirroring Python's
/// `cluster_mode × protocol` parametrization (the ~4× multiplier).
///
/// * standalone arms get a fresh [`TestServer`] each (cheap, fully isolated);
/// * cluster arms form a fresh per-test [`ClusterHarness`] each (fully isolated;
///   SKIP if a cluster cannot be formed in this environment).
///
/// The body must be **cluster-safe**: use [`common::key`] for single-key work and
/// [`common::tkey`] (hash-tagged) so multi-key commands land in one slot. The
/// same body compiles against both client types because every typed command
/// method is implemented for both `GlideClient` and `GlideClusterClient`.
///
/// ```ignore
/// matrix_test!(set_and_get, c, {
///     let k = common::key("str");
///     c.set(&k, "v").await.unwrap();
///     assert_eq!(c.get(&k).await.unwrap().as_deref(), Some(&b"v"[..]));
/// });
/// ```
#[macro_export]
macro_rules! matrix_test {
    ($name:ident, $c:ident, $body:block) => {
        mod $name {
            use super::*;
            #[allow(unused_imports)]
            use $crate::common;

            #[tokio::test]
            async fn standalone_resp2() {
                let __srv = $crate::server_or_skip!();
                let $c = __srv
                    .client_with_protocol(glide::ProtocolVersion::RESP2)
                    .await;
                $crate::common::with_test_timeout(async { $body }).await;
            }

            #[tokio::test]
            async fn standalone_resp3() {
                let __srv = $crate::server_or_skip!();
                let $c = __srv
                    .client_with_protocol(glide::ProtocolVersion::RESP3)
                    .await;
                $crate::common::with_test_timeout(async { $body }).await;
            }

            #[tokio::test]
            async fn cluster_resp2() {
                let __h = match $crate::common::ClusterHarness::start() {
                    Some(h) => h,
                    None => {
                        eprintln!("SKIP: cluster harness not feasible in this environment");
                        return;
                    }
                };
                let $c = match __h
                    .client_with_protocol(glide::ProtocolVersion::RESP2)
                    .await
                {
                    Some(c) => c,
                    None => {
                        eprintln!("SKIP: could not connect cluster client (RESP2)");
                        return;
                    }
                };
                $crate::common::with_test_timeout(async { $body }).await;
            }

            #[tokio::test]
            async fn cluster_resp3() {
                let __h = match $crate::common::ClusterHarness::start() {
                    Some(h) => h,
                    None => {
                        eprintln!("SKIP: cluster harness not feasible in this environment");
                        return;
                    }
                };
                let $c = match __h
                    .client_with_protocol(glide::ProtocolVersion::RESP3)
                    .await
                {
                    Some(c) => c,
                    None => {
                        eprintln!("SKIP: could not connect cluster client (RESP3)");
                        return;
                    }
                };
                $crate::common::with_test_timeout(async { $body }).await;
            }
        }
    };
}

/// Skip the current test (printing SKIP) when the server version is below
/// `major.minor.patch` — the Rust analogue of Python's
/// `@pytest.mark.skip_if_version_below`. Requires a connected client `$c` that
/// implements `ServerManagementCommands`.
///
/// ```ignore
/// matrix_test!(hexpire_sets_ttl, c, {
///     skip_if_version_below!(c, 7, 4, 0);
///     // ... newer-command assertions ...
/// });
/// ```
#[macro_export]
macro_rules! skip_if_version_below {
    ($c:expr, $major:expr, $minor:expr, $patch:expr) => {{
        if $crate::common::version_below(&$c, ($major, $minor, $patch)).await {
            eprintln!("SKIP: requires server >= {}.{}.{}", $major, $minor, $patch);
            return;
        }
    }};
}

/// Skip the current test (printing SKIP) unless the server recognises `$cmd` —
/// a robust, version-agnostic capability gate (preferred over version math for
/// commands whose availability differs across Redis/Valkey releases).
///
/// ```ignore
/// matrix_test!(hexpire_sets_ttl, c, {
///     skip_unless_command!(c, "HEXPIRE");
///     // ...
/// });
/// ```
#[macro_export]
macro_rules! skip_unless_command {
    ($c:expr, $cmd:expr) => {{
        if !$crate::common::command_exists(&$c, $cmd).await {
            eprintln!("SKIP: server does not support {}", $cmd);
            return;
        }
    }};
}

/// Assert that an expression evaluated to `Err(GlideError::Request(_))` — the
/// mapped form of a server-side error (e.g. `WRONGTYPE`).
#[macro_export]
macro_rules! assert_request_error {
    ($expr:expr) => {{
        match $expr {
            Err(glide::GlideError::Request(_)) => {}
            Err(other) => panic!("expected RequestError, got {:?}", other),
            Ok(v) => panic!("expected RequestError, got Ok({:?})", v),
        }
    }};
}
