// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Mock-executor unit tests for the hash command family.
use super::Mock;
use crate::commands::hash::HashCommands;
use crate::commands::options::{ExpireOptions, ExpirySet};
use bytes::Bytes;
use redis::Value;

#[tokio::test]
async fn hset_flattens_field_values() {
    let m = Mock::int(2);
    let n = m.hset("h", &[("f1", "v1"), ("f2", "v2")]).await.unwrap();
    m.assert_args(&["HSET", "h", "f1", "v1", "f2", "v2"]);
    assert_eq!(n, 2);
}

#[tokio::test]
async fn hget_hit_and_miss() {
    let m = Mock::bulk("v");
    assert_eq!(
        m.hget("h", "f").await.unwrap(),
        Some(Bytes::from_static(b"v"))
    );
    m.assert_args(&["HGET", "h", "f"]);

    let m = Mock::nil();
    assert_eq!(m.hget("h", "f").await.unwrap(), None);
}

#[tokio::test]
async fn hsetnx_bool() {
    let m = Mock::int(1);
    assert!(m.hsetnx("h", "f", "v").await.unwrap());
    m.assert_args(&["HSETNX", "h", "f", "v"]);
}

#[tokio::test]
async fn hdel_encoding() {
    let m = Mock::int(2);
    assert_eq!(m.hdel("h", &["f1", "f2"]).await.unwrap(), 2);
    m.assert_args(&["HDEL", "h", "f1", "f2"]);
}

#[tokio::test]
async fn hgetall_map() {
    let m = Mock::array(vec![
        Value::BulkString(b"f1".to_vec()),
        Value::BulkString(b"v1".to_vec()),
        Value::BulkString(b"f2".to_vec()),
        Value::BulkString(b"v2".to_vec()),
    ]);
    let map = m.hgetall("h").await.unwrap();
    m.assert_args(&["HGETALL", "h"]);
    assert_eq!(map.get("f1"), Some(&Bytes::from_static(b"v1")));
    assert_eq!(map.get("f2"), Some(&Bytes::from_static(b"v2")));
}

#[tokio::test]
async fn hmget_vec() {
    let m = Mock::array(vec![Value::BulkString(b"v1".to_vec()), Value::Nil]);
    let v = m.hmget("h", &["f1", "f2"]).await.unwrap();
    m.assert_args(&["HMGET", "h", "f1", "f2"]);
    assert_eq!(v, vec![Some(Bytes::from_static(b"v1")), None]);
}

#[tokio::test]
async fn hexists_hlen() {
    let m = Mock::int(1);
    assert!(m.hexists("h", "f").await.unwrap());
    m.assert_args(&["HEXISTS", "h", "f"]);

    let m = Mock::int(3);
    assert_eq!(m.hlen("h").await.unwrap(), 3);
    m.assert_args(&["HLEN", "h"]);
}

#[tokio::test]
async fn hkeys_hvals() {
    let m = Mock::array(vec![Value::BulkString(b"f1".to_vec())]);
    assert_eq!(m.hkeys("h").await.unwrap(), vec![Bytes::from_static(b"f1")]);
    m.assert_args(&["HKEYS", "h"]);

    let m = Mock::array(vec![Value::BulkString(b"v1".to_vec())]);
    assert_eq!(m.hvals("h").await.unwrap(), vec![Bytes::from_static(b"v1")]);
    m.assert_args(&["HVALS", "h"]);
}

#[tokio::test]
async fn hincr_variants() {
    let m = Mock::int(6);
    assert_eq!(m.hincr_by("h", "f", 5).await.unwrap(), 6);
    m.assert_args(&["HINCRBY", "h", "f", "5"]);

    let m = Mock::bulk("3.5");
    assert_eq!(m.hincr_by_float("h", "f", 1.5).await.unwrap(), 3.5);
    m.assert_args(&["HINCRBYFLOAT", "h", "f", "1.5"]);
}

#[tokio::test]
async fn hstrlen_encoding() {
    let m = Mock::int(4);
    assert_eq!(m.hstrlen("h", "f").await.unwrap(), 4);
    m.assert_args(&["HSTRLEN", "h", "f"]);
}

#[tokio::test]
async fn hrandfield_variants() {
    let m = Mock::bulk("f1");
    assert_eq!(
        m.hrandfield("h").await.unwrap(),
        Some(Bytes::from_static(b"f1"))
    );
    m.assert_args(&["HRANDFIELD", "h"]);

    let m = Mock::array(vec![Value::BulkString(b"f1".to_vec())]);
    m.hrandfield_count("h", 2).await.unwrap();
    m.assert_args(&["HRANDFIELD", "h", "2"]);

    let m = Mock::array(vec![
        Value::BulkString(b"f1".to_vec()),
        Value::BulkString(b"v1".to_vec()),
    ]);
    let pairs = m.hrandfield_withvalues("h", 1).await.unwrap();
    m.assert_args(&["HRANDFIELD", "h", "1", "WITHVALUES"]);
    assert_eq!(
        pairs,
        vec![(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"))]
    );
}

#[tokio::test]
async fn hscan_and_novalues() {
    let m = Mock::array(vec![
        Value::BulkString(b"0".to_vec()),
        Value::Array(vec![
            Value::BulkString(b"f1".to_vec()),
            Value::BulkString(b"v1".to_vec()),
        ]),
    ]);
    let (cursor, pairs) = m.hscan("h", "0", None, None).await.unwrap();
    m.assert_args(&["HSCAN", "h", "0"]);
    assert_eq!(cursor, "0");
    assert_eq!(
        pairs,
        vec![(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"))]
    );

    let m = Mock::array(vec![
        Value::BulkString(b"0".to_vec()),
        Value::Array(vec![Value::BulkString(b"f1".to_vec())]),
    ]);
    let (cursor, fields) = m
        .hscan_novalues("h", "0", Some(b"f*"), Some(10))
        .await
        .unwrap();
    m.assert_args(&["HSCAN", "h", "0", "MATCH", "f*", "COUNT", "10", "NOVALUES"]);
    assert_eq!(cursor, "0");
    assert_eq!(fields, vec![Bytes::from_static(b"f1")]);
}

#[tokio::test]
async fn hexpire_family() {
    let m = Mock::array(vec![Value::Int(1), Value::Int(1)]);
    let r = m.hexpire("h", 60, &["f1", "f2"], None).await.unwrap();
    m.assert_args(&["HEXPIRE", "h", "60", "FIELDS", "2", "f1", "f2"]);
    assert_eq!(r, vec![1, 1]);

    let m = Mock::array(vec![Value::Int(1)]);
    m.hexpire("h", 60, &["f1"], Some(ExpireOptions::HasNoExpiry))
        .await
        .unwrap();
    m.assert_args(&["HEXPIRE", "h", "60", "NX", "FIELDS", "1", "f1"]);
}

#[tokio::test]
async fn httl_and_hpersist() {
    let m = Mock::array(vec![Value::Int(100)]);
    assert_eq!(m.httl("h", &["f1"]).await.unwrap(), vec![100]);
    m.assert_args(&["HTTL", "h", "FIELDS", "1", "f1"]);

    let m = Mock::array(vec![Value::Int(1)]);
    m.hpersist("h", &["f1"]).await.unwrap();
    m.assert_args(&["HPERSIST", "h", "FIELDS", "1", "f1"]);
}

#[tokio::test]
async fn hgetex_and_hsetex() {
    let m = Mock::array(vec![Value::BulkString(b"v1".to_vec())]);
    let v = m
        .hgetex("h", &["f1"], Some(ExpirySet::Seconds(60)))
        .await
        .unwrap();
    m.assert_args(&["HGETEX", "h", "EX", "60", "FIELDS", "1", "f1"]);
    assert_eq!(v, vec![Some(Bytes::from_static(b"v1"))]);

    let m = Mock::int(1);
    let n = m.hsetex("h", &[("f1", "v1")], None, None).await.unwrap();
    m.assert_args(&["HSETEX", "h", "FIELDS", "1", "f1", "v1"]);
    assert_eq!(n, 1);
}

// ---- Hash-field TTL family (HEXPIRE/HGETEX/HSETEX, …) ----

#[tokio::test]
async fn hexpire_encoding_and_status() {
    let m = Mock::array(vec![Value::Int(1), Value::Int(1)]);
    let r = m.hexpire("h", 100, &["f1", "f2"], None).await.unwrap();
    m.assert_args(&["HEXPIRE", "h", "100", "FIELDS", "2", "f1", "f2"]);
    assert_eq!(r, vec![1, 1]);
}

#[tokio::test]
async fn hexpire_with_condition() {
    let m = Mock::array(vec![Value::Int(0)]);
    m.hexpire(
        "h",
        100,
        &["f1"],
        Some(crate::commands::options::ExpireOptions::HasNoExpiry),
    )
    .await
    .unwrap();
    m.assert_args(&["HEXPIRE", "h", "100", "NX", "FIELDS", "1", "f1"]);
}

#[tokio::test]
async fn hexpireat_and_pexpire_family() {
    let m = Mock::array(vec![Value::Int(1)]);
    m.hexpireat("h", 1700000000, &["f1"], None).await.unwrap();
    m.assert_args(&["HEXPIREAT", "h", "1700000000", "FIELDS", "1", "f1"]);

    let m = Mock::array(vec![Value::Int(1)]);
    m.hpexpire("h", 5000, &["f1"], None).await.unwrap();
    m.assert_args(&["HPEXPIRE", "h", "5000", "FIELDS", "1", "f1"]);

    let m = Mock::array(vec![Value::Int(1)]);
    m.hpexpireat("h", 1700000000000, &["f1"], None)
        .await
        .unwrap();
    m.assert_args(&["HPEXPIREAT", "h", "1700000000000", "FIELDS", "1", "f1"]);
}

#[tokio::test]
async fn httl_pttl_persist_expiretime() {
    let m = Mock::array(vec![Value::Int(90)]);
    assert_eq!(m.httl("h", &["f1"]).await.unwrap(), vec![90]);
    m.assert_args(&["HTTL", "h", "FIELDS", "1", "f1"]);

    let m = Mock::array(vec![Value::Int(90000)]);
    m.hpttl("h", &["f1"]).await.unwrap();
    m.assert_args(&["HPTTL", "h", "FIELDS", "1", "f1"]);

    let m = Mock::array(vec![Value::Int(1)]);
    m.hpersist("h", &["f1"]).await.unwrap();
    m.assert_args(&["HPERSIST", "h", "FIELDS", "1", "f1"]);

    let m = Mock::array(vec![Value::Int(1700000000)]);
    m.hexpiretime("h", &["f1"]).await.unwrap();
    m.assert_args(&["HEXPIRETIME", "h", "FIELDS", "1", "f1"]);

    let m = Mock::array(vec![Value::Int(1700000000000)]);
    m.hpexpiretime("h", &["f1"]).await.unwrap();
    m.assert_args(&["HPEXPIRETIME", "h", "FIELDS", "1", "f1"]);
}

#[tokio::test]
async fn hgetex_encoding() {
    let m = Mock::array(vec![Value::BulkString(b"v1".to_vec()), Value::Nil]);
    let r = m.hgetex("h", &["f1", "f2"], None).await.unwrap();
    m.assert_args(&["HGETEX", "h", "FIELDS", "2", "f1", "f2"]);
    assert_eq!(r[0].as_deref(), Some(&b"v1"[..]));
    assert_eq!(r[1], None);

    let m = Mock::array(vec![Value::BulkString(b"v1".to_vec())]);
    m.hgetex(
        "h",
        &["f1"],
        Some(crate::commands::options::ExpirySet::Seconds(60)),
    )
    .await
    .unwrap();
    m.assert_args(&["HGETEX", "h", "EX", "60", "FIELDS", "1", "f1"]);
}

#[tokio::test]
async fn hsetex_encoding() {
    let m = Mock::int(1);
    let r = m.hsetex("h", &[("f1", "v1")], None, None).await.unwrap();
    m.assert_args(&["HSETEX", "h", "FIELDS", "1", "f1", "v1"]);
    assert_eq!(r, 1);

    let m = Mock::int(1);
    m.hsetex(
        "h",
        &[("f1", "v1")],
        Some(crate::commands::options::HashFieldConditionalChange::OnlyIfNoneExist),
        Some(crate::commands::options::ExpirySet::Seconds(60)),
    )
    .await
    .unwrap();
    m.assert_args(&["HSETEX", "h", "FNX", "EX", "60", "FIELDS", "1", "f1", "v1"]);
}
