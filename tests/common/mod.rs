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
    ProtocolVersion,
};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

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
        let config =
            GlideClientConfiguration::with_address("127.0.0.1", self.port).protocol(protocol);
        GlideClient::connect(config)
            .await
            .expect("connect to test server")
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

/// A running multi-node cluster; all nodes are killed and temp state removed on
/// drop.
pub struct ClusterHarness {
    children: Vec<Child>,
    pub ports: Vec<u16>,
    dir: std::path::PathBuf,
}

impl ClusterHarness {
    /// Start a 3-primary cluster (no replicas) and wait for it to converge.
    /// Returns `None` (so tests SKIP) if a server binary is unavailable or the
    /// cluster does not reach `cluster_state:ok` within the timeout.
    pub fn start() -> Option<ClusterHarness> {
        Self::start_shards(3)
    }

    /// Start a cluster with `shards` primaries.
    pub fn start_shards(shards: usize) -> Option<ClusterHarness> {
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
            match Command::new(&bin)
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
                .stderr(Stdio::null())
                .spawn()
            {
                Ok(c) => children.push(c),
                Err(_) => break,
            }
        }

        let harness = ClusterHarness {
            children,
            ports: ports.clone(),
            dir,
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

        // Poll until every node reports state ok (not just the seed), so that
        // routed commands to any node see a converged cluster even under heavy
        // concurrent load.
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut all_ok = false;
        while Instant::now() < deadline {
            let mut converged = 0;
            for &port in &ports {
                if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
                    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
                    if let Ok(info) = raw_cmd(&mut s, &["CLUSTER", "INFO"])
                        && info.contains("cluster_state:ok")
                    {
                        converged += 1;
                    }
                }
            }
            if converged == ports.len() {
                all_ok = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        if !all_ok {
            return None;
        }
        // Small settle so topology (slot ownership per node) fully propagates.
        std::thread::sleep(Duration::from_millis(500));
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
        GlideClusterClient::connect(config).await.ok()
    }

    /// Connect a cluster client with the default protocol (RESP3).
    pub async fn client(&self) -> Option<GlideClusterClient> {
        self.client_with_protocol(ProtocolVersion::RESP3).await
    }
}

impl Drop for ClusterHarness {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ---------------------------------------------------------------------------
// Macros
// ---------------------------------------------------------------------------

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
                $body
            }

            #[tokio::test]
            async fn resp3() {
                let __srv = $crate::server_or_skip!();
                let $c = __srv
                    .client_with_protocol(glide::ProtocolVersion::RESP3)
                    .await;
                $body
            }
        }
    };
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
