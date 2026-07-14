// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! redis-rs parity: Script, `from_url`, blocking `Commands`, and decode checks
//! for reply shapes glide-core restructures (streams, geo, CONFIG GET).
//!
//! Companion to `it_redis_rs_api.rs`.

mod common;

use glide::sync::{SyncGlideClient, SyncGlideClusterClient};
use glide::{
    AsyncCommands, Commands, GlideClientConfiguration, GlideClusterClientConfiguration, Script, cmd,
};
use std::collections::HashMap;

// ---- Script (clean-room redis-rs Script type) ---------------------------------

matrix_test!(script_invoke_with_keys_and_args, c, {
    let c = c;
    let script = Script::new("return redis.call('SET', KEYS[1], ARGV[1])");
    let k = common::key("rrs_script");
    let _: () = script.key(&k).arg("stored").invoke_async(&c).await.unwrap();
    let v: String = c.get(&k).await.unwrap();
    assert_eq!(v, "stored");
});

matrix_test!(script_computes_values, c, {
    let c = c;
    let script = Script::new("return tonumber(ARGV[1]) + tonumber(ARGV[2])");
    let sum: i64 = script.arg(1).arg(2).invoke_async(&c).await.unwrap();
    assert_eq!(sum, 3);
});

resp_test!(script_noscript_fallback_after_flush, c, {
    let mut c = c;
    // Flush the script cache so EVALSHA is guaranteed to miss, exercising the
    // transparent EVAL fallback.
    let _: () = cmd("SCRIPT")
        .arg("FLUSH")
        .arg("SYNC")
        .query_async(&mut c)
        .await
        .unwrap();
    let script = Script::new("return 41 + 1");
    let v: i64 = script.invoke_async(&c).await.unwrap();
    assert_eq!(v, 42);
    // Second invocation hits the now-cached EVALSHA path.
    let v: i64 = script.invoke_async(&c).await.unwrap();
    assert_eq!(v, 42);
});

// ---- from_url (live) -----------------------------------------------------------

timed_tokio_test!(
    async fn from_url_connects_and_selects_db() {
        let srv = server_or_skip!();
        let url = format!("redis://127.0.0.1:{}/1", srv.port);
        let cfg = GlideClientConfiguration::from_url(&url).unwrap();
        assert_eq!(cfg.database_id, 1);
        let c1 = glide::GlideClient::connect(cfg).await.unwrap();

        let k = common::key("rrs_url_db");
        c1.set::<_, _, ()>(&k, "in-db-1").await.unwrap();

        // A db-0 client must not see the key; a second db-1 client must.
        let c0 = glide::GlideClient::connect(
            GlideClientConfiguration::from_url(&format!("redis://127.0.0.1:{}", srv.port)).unwrap(),
        )
        .await
        .unwrap();
        let miss: Option<String> = c0.get(&k).await.unwrap();
        assert_eq!(miss, None);
        let hit: Option<String> = c1.get(&k).await.unwrap();
        assert_eq!(hit.as_deref(), Some("in-db-1"));

        // get_db (ConnectionLike) reports the URL-selected database.
        assert_eq!(redis::aio::ConnectionLike::get_db(&c1), 1);
        assert_eq!(redis::aio::ConnectionLike::get_db(&c0), 0);
    }
);

// ---- blocking Commands trait (sync layer) --------------------------------------

#[test]
fn sync_commands_trait_typed_api() {
    let srv = server_or_skip!();
    let c = SyncGlideClient::connect(GlideClientConfiguration::with_address(
        "127.0.0.1",
        srv.port,
    ))
    .unwrap();

    let k = common::key("rrs_sync");
    // Typed redis-rs blocking API (Commands trait), exact signatures.
    Commands::set::<_, _, ()>(&c, &k, 42).unwrap();
    let v: i64 = Commands::get(&c, &k).unwrap();
    assert_eq!(v, 42);
    let v: i64 = Commands::incr(&c, &k, 8).unwrap();
    assert_eq!(v, 50);

    let h = common::key("rrs_sync_h");
    Commands::hset_multiple::<_, _, _, ()>(&c, &h, &[("a", "1"), ("b", "2")]).unwrap();
    let all: HashMap<String, String> = Commands::hgetall(&c, &h).unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn sync_pipeline_and_transaction() {
    let srv = server_or_skip!();
    let mut c = SyncGlideClient::connect(GlideClientConfiguration::with_address(
        "127.0.0.1",
        srv.port,
    ))
    .unwrap();

    let k1 = common::tkey("rrs_sp", "k1");
    let k2 = common::tkey("rrs_sp", "k2");
    let (v1, v2): (String, i64) = glide::pipe()
        .set(&k1, "x")
        .ignore()
        .set(&k2, 9)
        .ignore()
        .get(&k1)
        .get(&k2)
        .query(&mut c)
        .unwrap();
    assert_eq!((v1.as_str(), v2), ("x", 9));

    let ctr = common::tkey("rrs_sp", "ctr");
    let (a, b): (i64, i64) = glide::pipe()
        .atomic()
        .incr(&ctr, 1)
        .incr(&ctr, 1)
        .query(&mut c)
        .unwrap();
    assert_eq!((a, b), (1, 2));

    // Native-copy path: PipelineExt::query_glide (borrows &client, sends the
    // built Pipeline directly — no packed-byte round-trip) must produce
    // identical results to redis-rs's .query(), including .ignore() handling
    // and atomic transactions.
    use glide::sync::PipelineExt;
    let k3 = common::tkey("rrs_sp", "k3");
    let (v3, cnt): (String, i64) = glide::pipe()
        .set(&k3, "y")
        .ignore()
        .get(&k3)
        .incr(&ctr, 5)
        .query_glide(&c)
        .unwrap();
    assert_eq!((v3.as_str(), cnt), ("y", 7));

    let ctr2 = common::tkey("rrs_sp", "ctr2");
    let (x, y): (i64, i64) = glide::pipe()
        .atomic()
        .incr(&ctr2, 3)
        .incr(&ctr2, 4)
        .query_glide(&c)
        .unwrap();
    assert_eq!((x, y), (3, 7));
}

#[test]
fn sync_pipeline_with_literal_multi_exec_is_not_atomic() {
    // A plain (non-atomic) pipeline containing literal MULTI/EXEC commands —
    // manual transaction management, a real redis-rs pattern. Transaction
    // detection is driven by the trait call's offset/count, so this must NOT
    // be collapsed into a glide-core transaction: each command gets a reply.
    let srv = server_or_skip!();
    let mut c = SyncGlideClient::connect(GlideClientConfiguration::with_address(
        "127.0.0.1",
        srv.port,
    ))
    .unwrap();

    let ctr = common::tkey("rrs_literal_tx", "ctr");
    let (multi_ok, queued, exec_replies): (String, String, Vec<i64>) = glide::pipe()
        .cmd("MULTI")
        .cmd("INCR")
        .arg(&ctr)
        .cmd("EXEC")
        .query(&mut c)
        .unwrap();
    assert_eq!(multi_ok, "OK");
    assert_eq!(queued, "QUEUED");
    assert_eq!(exec_replies, vec![1]);
}

#[test]
fn sync_script_invoke_and_load() {
    // Blocking Script API (P1 parity gap): invoke() + load() on the sync client.
    let srv = server_or_skip!();
    let c = SyncGlideClient::connect(GlideClientConfiguration::with_address(
        "127.0.0.1",
        srv.port,
    ))
    .unwrap();

    let script = Script::new("return redis.call('SET', KEYS[1], ARGV[1])");
    let k = common::key("rrs_sync_script");
    let _: () = script.key(&k).arg("stored-sync").invoke(&c).unwrap();
    let v: String = Commands::get(&c, &k).unwrap();
    assert_eq!(v, "stored-sync");

    // Typed return through the sync path.
    let sum_script = Script::new("return tonumber(ARGV[1]) + tonumber(ARGV[2])");
    let sum: i64 = sum_script.arg(20).arg(22).invoke(&c).unwrap();
    assert_eq!(sum, 42);

    // load() returns the script's SHA-1 and populates the server cache.
    let hash = sum_script.load(&c).unwrap();
    assert_eq!(hash, sum_script.get_hash());
}

resp_test!(script_load_async_returns_hash, c, {
    let mut c = c;
    let script = Script::new("return 7");
    let hash = script.load_async(&c).await.unwrap();
    assert_eq!(hash, script.get_hash());
    // Loaded: EVALSHA now succeeds without fallback.
    let v: i64 = cmd("EVALSHA")
        .arg(script.get_hash())
        .arg(0)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(v, 7);
});

resp_test!(noscript_errorkind_passthrough, c, {
    // redis-rs users `match err.kind()`; NOSCRIPT must surface as
    // ErrorKind::NoScriptError outside the Script type's internal fallback.
    let mut c = c;
    let _: () = cmd("SCRIPT")
        .arg("FLUSH")
        .arg("SYNC")
        .query_async(&mut c)
        .await
        .unwrap();
    let err = cmd("EVALSHA")
        .arg("0000000000000000000000000000000000000000")
        .arg(0)
        .query_async::<_, i64>(&mut c)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), glide::ErrorKind::NoScriptError, "got: {err}");
});

// ---- normalized reply shapes: streams / geo / CONFIG GET ------------------------

matrix_test!(config_get_decodes_to_map, c, {
    let mut c = c;
    let cfg: HashMap<String, String> = cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory")
        .query_async(&mut c)
        .await
        .unwrap();
    assert!(cfg.contains_key("maxmemory"), "got: {cfg:?}");
});

matrix_test!(xadd_xlen_via_cmd, c, {
    // The fork has no typed stream methods — redis-rs users drive streams via
    // cmd(); verify typed decoding of the replies.
    let mut c = c;
    let k = common::key("rrs_stream");
    let id1: String = cmd("XADD")
        .arg(&k)
        .arg("*")
        .arg("f")
        .arg("v1")
        .query_async(&mut c)
        .await
        .unwrap();
    let _: String = cmd("XADD")
        .arg(&k)
        .arg("*")
        .arg("f")
        .arg("v2")
        .query_async(&mut c)
        .await
        .unwrap();
    assert!(id1.contains('-'));
    let len: i64 = cmd("XLEN").arg(&k).query_async(&mut c).await.unwrap();
    assert_eq!(len, 2);
});

matrix_test!(xrange_decode_shape, c, {
    // glide-core normalizes stream-entry replies to a map of id -> flat
    // field-value array; verify it decodes into standard containers.
    let mut c = c;
    let k = common::key("rrs_xr");
    let _: String = cmd("XADD")
        .arg(&k)
        .arg("1-1")
        .arg("a")
        .arg("1")
        .query_async(&mut c)
        .await
        .unwrap();
    let _: String = cmd("XADD")
        .arg(&k)
        .arg("2-2")
        .arg("b")
        .arg("2")
        .query_async(&mut c)
        .await
        .unwrap();
    let entries: HashMap<String, Vec<(String, String)>> = cmd("XRANGE")
        .arg(&k)
        .arg("-")
        .arg("+")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries["1-1"], vec![("a".to_string(), "1".to_string())]);
    assert_eq!(entries["2-2"], vec![("b".to_string(), "2".to_string())]);
});

matrix_test!(geo_decode_shapes, c, {
    // The fork has no typed geo methods either — cmd()-driven, typed decode.
    let mut c = c;
    let k = common::key("rrs_geo");
    let added: i64 = cmd("GEOADD")
        .arg(&k)
        .arg(13.361389)
        .arg(38.115556)
        .arg("Palermo")
        .arg(15.087269)
        .arg(37.502669)
        .arg("Catania")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(added, 2);

    // GEODIST is normalized to a double.
    let dist: f64 = cmd("GEODIST")
        .arg(&k)
        .arg("Palermo")
        .arg("Catania")
        .arg("km")
        .query_async(&mut c)
        .await
        .unwrap();
    assert!((dist - 166.27).abs() < 1.0, "got {dist}");

    // GEOPOS is normalized to arrays of double pairs.
    let pos: Vec<Vec<(f64, f64)>> = cmd("GEOPOS")
        .arg(&k)
        .arg("Palermo")
        .query_async(&mut c)
        .await
        .unwrap();
    assert!((pos[0][0].0 - 13.361389).abs() < 0.001);
});

matrix_test!(lmpop_typed_method, c, {
    let c = c;
    if common::version_below(&c, (7, 0, 0)).await {
        return;
    }
    let k = common::tkey("rrs_lmpop", "l1");
    let _: () = c.rpush(&k, &["a", "b"]).await.unwrap();
    // Fork signature: lmpop(numkeys, key, dir, count); normalized reply:
    // (key, [elements]).
    let popped: (String, Vec<String>) = c.lmpop(1, &k, glide::Direction::Left, 2).await.unwrap();
    assert_eq!(popped.0, k);
    assert_eq!(popped.1, vec!["a".to_string(), "b".to_string()]);
});

// ---- cluster: from_urls, sync Commands, NOSCRIPT fallback (P2-R2-6) -----------

#[tokio::test]
async fn cluster_from_urls_connects_and_routes() {
    let cluster = cluster_or_skip!();
    // Build seed-node URLs from the real cluster's primaries and connect via
    // the redis-rs-style URL constructor.
    let urls: Vec<String> = cluster
        .primary_ports
        .iter()
        .map(|p| format!("redis://127.0.0.1:{p}"))
        .collect();
    let cfg = GlideClusterClientConfiguration::from_urls(urls.iter().map(String::as_str)).unwrap();
    assert_eq!(cfg.addresses.len(), cluster.primary_ports.len());
    let c = match glide::GlideClusterClient::connect(cfg).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("SKIP: cluster connect failed: {e}");
            return;
        }
    };
    // Keys hash to different slots; the compat typed API routes each.
    for i in 0..20 {
        let k = format!("rrs_cluster_url:{i}");
        AsyncCommands::set::<_, _, ()>(&c, &k, i).await.unwrap();
        let v: i64 = AsyncCommands::get(&c, &k).await.unwrap();
        assert_eq!(v, i);
    }
}

#[test]
fn sync_cluster_commands_trait() {
    let cluster = cluster_or_skip!();
    let c = match SyncGlideClusterClient::connect(GlideClusterClientConfiguration::with_address(
        "127.0.0.1",
        cluster.seed_port(),
    )) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("SKIP: sync cluster connect failed: {e}");
            return;
        }
    };
    // Blocking redis-rs typed API on the cluster client.
    let k = format!("rrs_sync_cluster:{}", common::key("k"));
    Commands::set::<_, _, ()>(&c, &k, 123).unwrap();
    let v: i64 = Commands::get(&c, &k).unwrap();
    assert_eq!(v, 123);
    let v: i64 = Commands::incr(&c, &k, 7).unwrap();
    assert_eq!(v, 130);
}

#[tokio::test]
async fn cluster_script_noscript_fallback() {
    // Keyless scripts route to a random node, so EVALSHA can miss on whichever
    // node it lands on — exercising the transparent EVAL fallback in cluster
    // mode. Flush all nodes first to guarantee the miss, then invoke enough
    // times to hit multiple nodes.
    let cluster = cluster_or_skip!();
    let mut c = match cluster.client().await {
        Some(c) => c,
        None => {
            eprintln!("SKIP: cluster client connect failed");
            return;
        }
    };
    let _: () = cmd("SCRIPT")
        .arg("FLUSH")
        .arg("SYNC")
        .query_async(&mut c)
        .await
        .unwrap_or(());
    let script = Script::new("return 40 + 2");
    for _ in 0..10 {
        let v: i64 = script.invoke_async(&c).await.unwrap();
        assert_eq!(v, 42);
    }
}
