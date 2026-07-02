// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Per-command hash integration tests (RESP2 + RESP3).

mod common;

use glide::commands::options::ExpireOptions;
use glide::{HashCommands, ListCommands};

matrix_test!(hset_hget, c, {
    let k = common::key("h");
    assert_eq!(c.hset(&k, &[("f1", "v1"), ("f2", "v2")]).await.unwrap(), 2);
    assert_eq!(c.hget(&k, "f1").await.unwrap().as_deref(), Some(&b"v1"[..]));
});

matrix_test!(hget_missing_field, c, {
    let k = common::key("h");
    c.hset(&k, &[("f", "v")]).await.unwrap();
    assert_eq!(c.hget(&k, "nope").await.unwrap(), None);
});

matrix_test!(hget_missing_key, c, {
    assert_eq!(c.hget(common::key("h"), "f").await.unwrap(), None);
});

matrix_test!(hset_updates_existing_returns_zero, c, {
    let k = common::key("h");
    c.hset(&k, &[("f", "v1")]).await.unwrap();
    // Updating an existing field returns 0 new fields.
    assert_eq!(c.hset(&k, &[("f", "v2")]).await.unwrap(), 0);
    assert_eq!(c.hget(&k, "f").await.unwrap().as_deref(), Some(&b"v2"[..]));
});

matrix_test!(hsetnx, c, {
    let k = common::key("h");
    assert!(c.hsetnx(&k, "f", "v1").await.unwrap());
    assert!(!c.hsetnx(&k, "f", "v2").await.unwrap());
    assert_eq!(c.hget(&k, "f").await.unwrap().as_deref(), Some(&b"v1"[..]));
});

matrix_test!(hdel, c, {
    let k = common::key("h");
    c.hset(&k, &[("a", "1"), ("b", "2"), ("d", "3")])
        .await
        .unwrap();
    assert_eq!(c.hdel(&k, &["a", "b", "missing"]).await.unwrap(), 2);
    assert_eq!(c.hlen(&k).await.unwrap(), 1);
});

matrix_test!(hgetall, c, {
    let k = common::key("h");
    c.hset(&k, &[("f1", "v1"), ("f2", "v2")]).await.unwrap();
    let all = c.hgetall(&k).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all.get("f1").map(|b| b.as_ref()), Some(&b"v1"[..]));
    assert_eq!(all.get("f2").map(|b| b.as_ref()), Some(&b"v2"[..]));
});

matrix_test!(hgetall_missing_is_empty, c, {
    assert!(c.hgetall(common::key("h")).await.unwrap().is_empty());
});

matrix_test!(hmget, c, {
    let k = common::key("h");
    c.hset(&k, &[("f1", "v1"), ("f2", "v2")]).await.unwrap();
    let vals = c.hmget(&k, &["f1", "missing", "f2"]).await.unwrap();
    assert_eq!(vals[0].as_deref(), Some(&b"v1"[..]));
    assert_eq!(vals[1], None);
    assert_eq!(vals[2].as_deref(), Some(&b"v2"[..]));
});

matrix_test!(hexists, c, {
    let k = common::key("h");
    c.hset(&k, &[("f", "v")]).await.unwrap();
    assert!(c.hexists(&k, "f").await.unwrap());
    assert!(!c.hexists(&k, "nope").await.unwrap());
});

matrix_test!(hlen, c, {
    let k = common::key("h");
    assert_eq!(c.hlen(&k).await.unwrap(), 0);
    c.hset(&k, &[("a", "1"), ("b", "2")]).await.unwrap();
    assert_eq!(c.hlen(&k).await.unwrap(), 2);
});

matrix_test!(hkeys_hvals, c, {
    let k = common::key("h");
    c.hset(&k, &[("f1", "v1"), ("f2", "v2")]).await.unwrap();
    let mut keys: Vec<_> = c
        .hkeys(&k)
        .await
        .unwrap()
        .iter()
        .map(|b| b.to_vec())
        .collect();
    keys.sort();
    assert_eq!(keys, vec![b"f1".to_vec(), b"f2".to_vec()]);
    let mut vals: Vec<_> = c
        .hvals(&k)
        .await
        .unwrap()
        .iter()
        .map(|b| b.to_vec())
        .collect();
    vals.sort();
    assert_eq!(vals, vec![b"v1".to_vec(), b"v2".to_vec()]);
});

matrix_test!(hincr_by, c, {
    let k = common::key("h");
    assert_eq!(c.hincr_by(&k, "n", 5).await.unwrap(), 5);
    assert_eq!(c.hincr_by(&k, "n", -2).await.unwrap(), 3);
});

matrix_test!(hincr_by_float, c, {
    let k = common::key("h");
    let v = c.hincr_by_float(&k, "n", 1.5).await.unwrap();
    assert!((v - 1.5).abs() < 1e-9);
});

matrix_test!(hstrlen, c, {
    let k = common::key("h");
    c.hset(&k, &[("f", "hello")]).await.unwrap();
    assert_eq!(c.hstrlen(&k, "f").await.unwrap(), 5);
    assert_eq!(c.hstrlen(&k, "missing").await.unwrap(), 0);
});

matrix_test!(hrandfield, c, {
    let k = common::key("h");
    c.hset(&k, &[("only", "v")]).await.unwrap();
    assert_eq!(
        c.hrandfield(&k).await.unwrap().as_deref(),
        Some(&b"only"[..])
    );
    // Missing key -> None.
    assert_eq!(c.hrandfield(common::key("x")).await.unwrap(), None);
});

matrix_test!(hrandfield_count, c, {
    let k = common::key("h");
    c.hset(&k, &[("a", "1"), ("b", "2"), ("d", "3")])
        .await
        .unwrap();
    let fields = c.hrandfield_count(&k, 2).await.unwrap();
    assert_eq!(fields.len(), 2);
});

matrix_test!(hset_wrong_type_errors, c, {
    let k = common::key("wt");
    c.rpush(&k, &["x"]).await.unwrap();
    assert_request_error!(c.hget(&k, "f").await);
});

// ---------------------------------------------------------------------------
// Hash-field TTL (Valkey/Redis 7.4+). Version-gated so CI against older servers
// skips gracefully — mirrors Python's @skip_if_version_below("7.4.0").
// ---------------------------------------------------------------------------

matrix_test!(hexpire_and_httl, c, {
    skip_if_version_below!(c, 7, 4, 0);
    let k = common::key("h_ttl");
    c.hset(&k, &[("f1", "v1"), ("f2", "v2")]).await.unwrap();
    // Set a 100s TTL on f1 only.
    let res = c.hexpire(&k, 100, &["f1"], None).await.unwrap();
    assert_eq!(res, vec![1]); // 1 = expiry set
    // HTTL: f1 has a positive TTL, f2 has none (-1), missing field is -2.
    let ttls = c.httl(&k, &["f1", "f2", "missing"]).await.unwrap();
    assert!(ttls[0] > 0 && ttls[0] <= 100);
    assert_eq!(ttls[1], -1);
    assert_eq!(ttls[2], -2);
});

matrix_test!(hexpire_conditions, c, {
    skip_if_version_below!(c, 7, 4, 0);
    let k = common::key("h_ttlc");
    c.hset(&k, &[("f", "v")]).await.unwrap();
    // NX: set only when no TTL exists -> succeeds.
    assert_eq!(
        c.hexpire(&k, 100, &["f"], Some(ExpireOptions::HasNoExpiry))
            .await
            .unwrap(),
        vec![1]
    );
    // NX again -> 0 (a TTL already exists).
    assert_eq!(
        c.hexpire(&k, 200, &["f"], Some(ExpireOptions::HasNoExpiry))
            .await
            .unwrap(),
        vec![0]
    );
    // XX: set only when a TTL exists -> succeeds.
    assert_eq!(
        c.hexpire(&k, 200, &["f"], Some(ExpireOptions::HasExistingExpiry))
            .await
            .unwrap(),
        vec![1]
    );
});

matrix_test!(hpexpire_and_hpttl, c, {
    skip_if_version_below!(c, 7, 4, 0);
    let k = common::key("h_pttl");
    c.hset(&k, &[("f", "v")]).await.unwrap();
    assert_eq!(
        c.hpexpire(&k, 100_000, &["f"], None).await.unwrap(),
        vec![1]
    );
    let pttls = c.hpttl(&k, &["f"]).await.unwrap();
    assert!(pttls[0] > 0 && pttls[0] <= 100_000);
});

matrix_test!(hexpiretime_absolute, c, {
    skip_if_version_below!(c, 7, 4, 0);
    let k = common::key("h_et");
    c.hset(&k, &[("f", "v")]).await.unwrap();
    let future = 4_102_444_800; // year 2100 (seconds)
    assert_eq!(
        c.hexpireat(&k, future, &["f"], None).await.unwrap(),
        vec![1]
    );
    let et = c.hexpiretime(&k, &["f"]).await.unwrap();
    assert_eq!(et[0], future);
});
