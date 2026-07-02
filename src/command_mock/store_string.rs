// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Mock-executor unit tests for the string command family.
use super::Mock;
use crate::commands::options::{ExpirySet, SetOptions};
use crate::commands::string::StringCommands;
use bytes::Bytes;
use redis::Value;

#[tokio::test]
async fn get_hit() {
    let m = Mock::bulk("hello");
    let v = m.get("k").await.unwrap();
    m.assert_args(&["GET", "k"]);
    assert_eq!(v, Some(Bytes::from_static(b"hello")));
}

#[tokio::test]
async fn get_miss_is_none() {
    let m = Mock::nil();
    assert_eq!(m.get("absent").await.unwrap(), None);
    m.assert_args(&["GET", "absent"]);
}

#[tokio::test]
async fn getdel_encoding() {
    let m = Mock::bulk("v");
    m.getdel("k").await.unwrap();
    m.assert_args(&["GETDEL", "k"]);
}

#[tokio::test]
async fn getex_no_expiry() {
    let m = Mock::bulk("v");
    m.getex("k", None).await.unwrap();
    m.assert_args(&["GETEX", "k"]);
}

#[tokio::test]
async fn getex_with_expiry_seconds() {
    let m = Mock::bulk("v");
    m.getex("k", Some(ExpirySet::Seconds(60))).await.unwrap();
    m.assert_args(&["GETEX", "k", "EX", "60"]);
}

#[tokio::test]
async fn set_basic() {
    let m = Mock::ok();
    m.set("k", "v").await.unwrap();
    m.assert_args(&["SET", "k", "v"]);
}

#[tokio::test]
async fn set_options_with_get_returns_old() {
    let m = Mock::bulk("old");
    let opts = SetOptions {
        return_old_value: true,
        ..Default::default()
    };
    let old = m.set_options("k", "new", opts).await.unwrap();
    assert_eq!(m.keyword(), "SET");
    let args = m.args();
    assert_eq!(&args[0..3], &["SET", "k", "new"]);
    assert!(args.contains(&"GET".to_string()));
    assert_eq!(old, Some(Bytes::from_static(b"old")));
}

#[tokio::test]
async fn append_returns_len() {
    let m = Mock::int(5);
    assert_eq!(m.append("k", "v").await.unwrap(), 5);
    m.assert_args(&["APPEND", "k", "v"]);
}

#[tokio::test]
async fn strlen_encoding() {
    let m = Mock::int(11);
    assert_eq!(m.strlen("k").await.unwrap(), 11);
    m.assert_args(&["STRLEN", "k"]);
}

#[tokio::test]
async fn getrange_encoding() {
    let m = Mock::bulk("ell");
    m.getrange("k", 1, 3).await.unwrap();
    m.assert_args(&["GETRANGE", "k", "1", "3"]);
}

#[tokio::test]
async fn setrange_encoding() {
    let m = Mock::int(10);
    m.setrange("k", 5, "xyz").await.unwrap();
    m.assert_args(&["SETRANGE", "k", "5", "xyz"]);
}

#[tokio::test]
async fn mget_multi() {
    let m = Mock::array(vec![
        Value::BulkString(b"a".to_vec()),
        Value::Nil,
        Value::BulkString(b"c".to_vec()),
    ]);
    let v = m.mget(&["k1", "k2", "k3"]).await.unwrap();
    m.assert_args(&["MGET", "k1", "k2", "k3"]);
    assert_eq!(
        v,
        vec![
            Some(Bytes::from_static(b"a")),
            None,
            Some(Bytes::from_static(b"c"))
        ]
    );
}

#[tokio::test]
async fn mset_flattens_pairs() {
    let m = Mock::ok();
    m.mset(&[("k1", "v1"), ("k2", "v2")]).await.unwrap();
    m.assert_args(&["MSET", "k1", "v1", "k2", "v2"]);
}

#[tokio::test]
async fn msetnx_returns_bool() {
    let m = Mock::int(1);
    assert!(m.msetnx(&[("k1", "v1")]).await.unwrap());
    m.assert_args(&["MSETNX", "k1", "v1"]);
}

#[tokio::test]
async fn incr_family() {
    let m = Mock::int(1);
    m.incr("k").await.unwrap();
    m.assert_args(&["INCR", "k"]);

    let m = Mock::int(6);
    assert_eq!(m.incr_by("k", 5).await.unwrap(), 6);
    m.assert_args(&["INCRBY", "k", "5"]);

    let m = Mock::bulk("3.5");
    assert_eq!(m.incr_by_float("k", 1.5).await.unwrap(), 3.5);
    m.assert_args(&["INCRBYFLOAT", "k", "1.5"]);
}

#[tokio::test]
async fn decr_family() {
    let m = Mock::int(-1);
    m.decr("k").await.unwrap();
    m.assert_args(&["DECR", "k"]);

    let m = Mock::int(-5);
    m.decr_by("k", 5).await.unwrap();
    m.assert_args(&["DECRBY", "k", "5"]);
}

#[tokio::test]
async fn lcs_len_encoding() {
    let m = Mock::int(3);
    assert_eq!(m.lcs_len("k1", "k2").await.unwrap(), 3);
    m.assert_args(&["LCS", "k1", "k2", "LEN"]);
}

#[tokio::test]
async fn getset_encoding() {
    let m = Mock::bulk("old");
    m.getset("k", "new").await.unwrap();
    m.assert_args(&["GETSET", "k", "new"]);
}

#[tokio::test]
async fn setex_and_psetex() {
    let m = Mock::ok();
    m.setex("k", 60, "v").await.unwrap();
    m.assert_args(&["SETEX", "k", "60", "v"]);

    let m = Mock::ok();
    m.psetex("k", 6000, "v").await.unwrap();
    m.assert_args(&["PSETEX", "k", "6000", "v"]);
}

#[tokio::test]
async fn setnx_returns_bool() {
    let m = Mock::int(1);
    assert!(m.setnx("k", "v").await.unwrap());
    m.assert_args(&["SETNX", "k", "v"]);
}

#[tokio::test]
async fn substr_encoding() {
    let m = Mock::bulk("ell");
    m.substr("k", 1, 3).await.unwrap();
    m.assert_args(&["SUBSTR", "k", "1", "3"]);
}
