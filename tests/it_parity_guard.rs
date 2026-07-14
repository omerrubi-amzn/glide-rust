// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Parity guards for the unified command table (`src/commands/core.rs`):
//!  * `command_table_matches_fork` — runs `tools/verify_command_table.py`,
//!    which compares every method signature in our hand-maintained table
//!    against the vendored redis-rs fork's `implement_commands!` table
//!    (names, generic order, argument lists). Skips gracefully when Python
//!    or the fork checkout is unavailable (same posture as the live tests).
//!  * `fork_trait_escape_path_*` — locks the compatibility promise that the
//!    *literal* fork traits (`glide::redis::AsyncCommands` / `Commands`)
//!    still work on the clients, including generic code bounded on them.

mod common;

use std::path::Path;
use std::process::Command;

/// Signature-parity guard: our table must match the fork's, method for method.
#[test]
fn command_table_matches_fork() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let verifier = Path::new(manifest_dir).join("tools/verify_command_table.py");
    if !verifier.exists() {
        eprintln!("SKIP: verifier not found");
        return;
    }
    let python = ["python3", "python"]
        .into_iter()
        .find(|p| Command::new(p).arg("--version").output().is_ok());
    let Some(python) = python else {
        eprintln!("SKIP: no python interpreter available");
        return;
    };
    let out = Command::new(python)
        .arg(&verifier)
        .current_dir(manifest_dir)
        .output()
        .expect("run verifier");
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("could not resolve") {
            eprintln!("SKIP: fork checkout unavailable:\n{stderr}");
            return;
        }
        panic!(
            "command table diverges from the fork:\n{}\n{stderr}",
            String::from_utf8_lossy(&out.stdout)
        );
    }
}

// ---- generic-code locks --------------------------------------------------------
//
// The GLIDE clients are deliberately NOT `redis` connection objects (the
// `ConnectionLike` interop cost a payload copy per command). What IS
// guaranteed is that generic code can bound on GLIDE's own traits — these
// tests compile-lock that contract and exercise it live.

/// Generic over GLIDE's async trait — proves downstream code can write
/// client-agnostic helpers against `glide::AsyncCommands`.
async fn via_glide_async_trait<C: glide::AsyncCommands>(
    con: &C,
    key: &str,
) -> glide::RedisResult<i64> {
    con.set::<_, _, ()>(key, 7).await?;
    con.get(key).await
}

matrix_test!(generic_code_on_glide_async_trait, c, {
    let k = common::key("rrs_glide_generic");
    let v = via_glide_async_trait(&c, &k).await.unwrap();
    assert_eq!(v, 7);
});

#[test]
fn generic_code_on_glide_sync_trait() {
    let srv = match common::TestServer::start() {
        Some(s) => s,
        None => {
            eprintln!("SKIP: no valkey-server");
            return;
        }
    };
    use glide::Commands;
    use glide::sync::SyncGlideClient;
    let c = SyncGlideClient::connect(glide::GlideClientConfiguration::with_address(
        "127.0.0.1",
        srv.port,
    ))
    .unwrap();
    let k = common::key("rrs_glide_generic_sync");
    // Generic bound on GLIDE's blocking trait.
    fn via_glide_sync_trait<C: Commands>(con: &C, key: &str) -> glide::RedisResult<i64> {
        con.set::<_, _, ()>(key, 9)?;
        con.get(key)
    }
    let v = via_glide_sync_trait(&c, &k).unwrap();
    assert_eq!(v, 9);
}
