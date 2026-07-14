// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! redis-rs API parity tests.
//!
//! `GlideClient` / `GlideClusterClient` implement `redis::aio::ConnectionLike`,
//! so the redis-rs `AsyncCommands` trait, `Pipeline`, and scan iterators work on
//! them directly. These tests exercise that surface end-to-end against a live
//! server on RESP2 and RESP3 — including commands whose replies glide-core
//! normalizes (maps, sets, doubles, booleans), to prove redis-rs typed decoding
//! (`FromRedisValue`) still behaves as redis-rs users expect.

mod common;

use glide::{AsyncCommands, RedisResult, pipe};
use std::collections::{HashMap, HashSet};

// ---- typed AsyncCommands methods (exact redis-rs signatures) ----------------

matrix_test!(set_get_typed, c, {
    let mut c = c;
    let k = common::key("rrs");
    c.set::<_, _, ()>(&k, 42).await.unwrap();
    let as_int: i64 = c.get(&k).await.unwrap();
    assert_eq!(as_int, 42);
    let as_string: String = c.get(&k).await.unwrap();
    assert_eq!(as_string, "42");
});

matrix_test!(get_missing_option_none, c, {
    let mut c = c;
    let v: Option<String> = c.get(common::key("rrs_missing")).await.unwrap();
    assert_eq!(v, None);
});

matrix_test!(incr_decr_typed, c, {
    let mut c = c;
    let k = common::key("rrs_ctr");
    let v: i64 = c.incr(&k, 5).await.unwrap();
    assert_eq!(v, 5);
    let v: i64 = c.decr(&k, 2).await.unwrap();
    assert_eq!(v, 3);
});

matrix_test!(redis_rs_names_work, c, {
    // Methods whose redis-rs names differ from our native trait names.
    let mut c = c;
    let k = common::key("rrs_names");
    c.set_ex::<_, _, ()>(&k, "v", 100).await.unwrap();
    let ttl: i64 = c.ttl(&k).await.unwrap();
    assert!(ttl > 0 && ttl <= 100);
    let old: String = c.getset(&k, "new").await.unwrap();
    assert_eq!(old, "v");
    let deleted: String = c.get_del(&k).await.unwrap();
    assert_eq!(deleted, "new");
    let exists: bool = c.exists(&k).await.unwrap();
    assert!(!exists);
});

matrix_test!(deprecated_commands_still_work, c, {
    // The fork keeps deprecated commands (HMSET, RPOPLPUSH); same-slot keys.
    let mut c = c;
    let src = common::tkey("rrs_dep", "src");
    let dst = common::tkey("rrs_dep", "dst");
    c.rpush::<_, _, ()>(&src, &["a", "b"]).await.unwrap();
    let moved: String = c.rpoplpush(&src, &dst).await.unwrap();
    assert_eq!(moved, "b");
});

// ---- normalized-value decoding (the Phase-0 behavioral question) -------------

matrix_test!(hgetall_decodes_to_hashmap, c, {
    // glide-core normalizes HGETALL to a map on both RESP2 and RESP3;
    // redis-rs HashMap decoding must accept it.
    let mut c = c;
    let k = common::key("rrs_hash");
    c.hset_multiple::<_, _, _, ()>(&k, &[("f1", "v1"), ("f2", "v2")])
        .await
        .unwrap();
    let all: HashMap<String, String> = c.hgetall(&k).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all["f1"], "v1");
    assert_eq!(all["f2"], "v2");
});

matrix_test!(bool_normalization_decodes, c, {
    let mut c = c;
    let k = common::key("rrs_set");
    c.sadd::<_, _, ()>(&k, "member").await.unwrap();
    let yes: bool = c.sismember(&k, "member").await.unwrap();
    let no: bool = c.sismember(&k, "nope").await.unwrap();
    assert!(yes);
    assert!(!no);
    let expired: bool = c.expire(&k, 1000).await.unwrap();
    assert!(expired);
});

matrix_test!(smembers_decodes_to_hashset, c, {
    let mut c = c;
    let k = common::key("rrs_sm");
    c.sadd::<_, _, ()>(&k, &["a", "b", "c"]).await.unwrap();
    let members: HashSet<String> = c.smembers(&k).await.unwrap();
    assert_eq!(
        members,
        HashSet::from(["a".to_string(), "b".to_string(), "c".to_string()])
    );
});

matrix_test!(zset_double_normalization_decodes, c, {
    let mut c = c;
    let k = common::key("rrs_z");
    let added: i64 = c
        .zadd_multiple(&k, &[(1.5, "one"), (2.5, "two")])
        .await
        .unwrap();
    assert_eq!(added, 2);
    let score: f64 = c.zscore(&k, "one").await.unwrap();
    assert_eq!(score, 1.5);
    let incremented: f64 = c.zincr(&k, "one", 1.0).await.unwrap();
    assert_eq!(incremented, 2.5);
    // ZPOPMIN reply is normalized; redis-rs decodes member/score pairs.
    let popped: Vec<(String, f64)> = c.zpopmin(&k, 1).await.unwrap();
    assert_eq!(popped, vec![("two".to_string(), 2.5)]);
});

matrix_test!(zrange_withscores_decodes, c, {
    let mut c = c;
    let k = common::key("rrs_zr");
    c.zadd_multiple::<_, _, _, ()>(&k, &[(1.0, "a"), (2.0, "b")])
        .await
        .unwrap();
    let pairs: Vec<(String, f64)> = c.zrange_withscores(&k, 0, -1).await.unwrap();
    assert_eq!(pairs, vec![("a".to_string(), 1.0), ("b".to_string(), 2.0)]);
});

// ---- pipelines & transactions ------------------------------------------------

matrix_test!(pipeline_query_async, c, {
    let mut c = c;
    let k1 = common::tkey("rrs_pipe", "k1");
    let k2 = common::tkey("rrs_pipe", "k2");
    let (v1, v2): (String, i64) = pipe()
        .set(&k1, "hello")
        .ignore()
        .set(&k2, 7)
        .ignore()
        .get(&k1)
        .get(&k2)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(v1, "hello");
    assert_eq!(v2, 7);
});

matrix_test!(atomic_transaction_query_async, c, {
    let mut c = c;
    let k = common::tkey("rrs_tx", "ctr");
    let (a, b): (i64, i64) = pipe()
        .atomic()
        .incr(&k, 1)
        .incr(&k, 1)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!((a, b), (1, 2));
});

// ---- error surface -----------------------------------------------------------

matrix_test!(wrong_type_returns_redis_error, c, {
    let mut c = c;
    let k = common::key("rrs_err");
    c.set::<_, _, ()>(&k, "text").await.unwrap();
    let res: RedisResult<Vec<String>> = c.lrange(&k, 0, -1).await;
    let err = res.unwrap_err();
    // WRONGTYPE server error surfaces as a RedisError, exactly as in redis-rs.
    assert!(err.to_string().contains("WRONGTYPE"), "got: {err}");
});

// ---- scan iterators (standalone only: cursor iteration is per-node) -----------

resp_test!(scan_match_iterator, c, {
    let mut c = c;
    let prefix = common::key("rrs_scan");
    for i in 0..10 {
        c.set::<_, _, ()>(format!("{prefix}:{i}"), i).await.unwrap();
    }
    let mut found: Vec<String> = Vec::new();
    {
        let mut iter = c.scan_match(format!("{prefix}:*")).await.unwrap();
        while let Some(k) = iter.next_item().await {
            found.push(k);
        }
    }
    assert_eq!(found.len(), 10);
});
