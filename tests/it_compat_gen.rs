// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Guards for the generated `src/compat_commands.rs`:
//!  * `compat_commands_matches_fork` — re-runs the generator and diffs its
//!    output against the committed file, catching drift after a fork-rev bump.
//!    Skips gracefully when Python or the fork checkout is unavailable (same
//!    posture the live server tests use).
//!  * `fork_trait_escape_path_*` — locks the `feat!` compatibility promise that
//!    the *literal* fork traits (`glide::redis::AsyncCommands` / `Commands`)
//!    still work on the clients, including generic code bounded on them.

mod common;

use std::path::Path;
use std::process::Command;

/// Drift guard: the committed generated file must equal a fresh generation.
#[test]
fn compat_commands_matches_fork() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let generator = Path::new(manifest_dir).join("tools/gen_compat_commands.py");
    if !generator.exists() {
        eprintln!("SKIP: generator not found");
        return;
    }
    let python = ["python3", "python"]
        .into_iter()
        .find(|p| Command::new(p).arg("--version").output().is_ok());
    let Some(python) = python else {
        eprintln!("SKIP: no python interpreter available");
        return;
    };

    let committed = Path::new(manifest_dir).join("src/compat_commands.rs");
    let committed_src = std::fs::read_to_string(&committed).expect("read committed file");

    // Generate into a temp copy so we never disturb the tree, then restore.
    let backup = std::fs::read_to_string(&committed).expect("backup");
    let out = Command::new(python)
        .arg(&generator)
        .current_dir(manifest_dir)
        .output()
        .expect("run generator");
    let regenerated = std::fs::read_to_string(&committed).expect("read regenerated");
    // Restore the committed content regardless of outcome.
    std::fs::write(&committed, &backup).expect("restore committed file");

    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        // Fork checkout not resolvable (e.g. cargo cache evicted) — skip, don't fail.
        if stderr.contains("could not resolve") || stderr.contains("cargo metadata") {
            eprintln!("SKIP: fork checkout unavailable:\n{stderr}");
            return;
        }
        panic!("generator failed:\n{stderr}");
    }

    assert_eq!(
        committed_src, regenerated,
        "src/compat_commands.rs is stale — run `python3 tools/gen_compat_commands.py` \
         (see DEVELOPER.md 'Regenerating compat_commands.rs')"
    );
}

// ---- fork-trait escape path (P2-R2-1) ----------------------------------------
//
// The `feat!` commit swapped `glide::AsyncCommands`/`Commands` to GLIDE-owned
// drop-ins, but promised the *literal* fork traits still work via
// `glide::redis::*`. These tests compile-lock that promise (a generic function
// bounded on the fork trait) and exercise it live.

/// Generic over the literal fork async trait — proves downstream code bounded
/// on `redis::aio::ConnectionLike`-derived `AsyncCommands` still compiles.
async fn via_fork_async_trait<C: glide::redis::AsyncCommands>(
    con: &mut C,
    key: &str,
) -> glide::RedisResult<i64> {
    con.set::<_, _, ()>(key, 7).await?;
    con.get(key).await
}

matrix_test!(fork_trait_escape_path_async, c, {
    let mut c = c;
    let k = common::key("rrs_fork_escape");
    let v = via_fork_async_trait(&mut c, &k).await.unwrap();
    assert_eq!(v, 7);
});

#[test]
fn fork_trait_escape_path_sync() {
    let srv = match common::TestServer::start() {
        Some(s) => s,
        None => {
            eprintln!("SKIP: no valkey-server");
            return;
        }
    };
    use glide::redis::Commands as ForkCommands;
    use glide::sync::SyncGlideClient;
    let mut c = SyncGlideClient::connect(glide::GlideClientConfiguration::with_address(
        "127.0.0.1",
        srv.port,
    ))
    .unwrap();
    let k = common::key("rrs_fork_escape_sync");
    // Generic bound on the literal fork blocking trait.
    fn via_fork_sync_trait<C: ForkCommands>(con: &mut C, key: &str) -> glide::RedisResult<i64> {
        con.set::<_, _, ()>(key, 9)?;
        con.get(key)
    }
    let v = via_fork_sync_trait(&mut c, &k).unwrap();
    assert_eq!(v, 9);
}
