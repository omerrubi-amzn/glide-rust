// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Mock-executor unit tests for the set command family.
use super::Mock;
use crate::commands::set::SetCommands;
use bytes::Bytes;
use redis::Value;

#[tokio::test]
async fn sadd_srem() {
    let m = Mock::int(2);
    assert_eq!(m.sadd("s", &["a", "b"]).await.unwrap(), 2);
    m.assert_args(&["SADD", "s", "a", "b"]);

    let m = Mock::int(1);
    assert_eq!(m.srem("s", &["a"]).await.unwrap(), 1);
    m.assert_args(&["SREM", "s", "a"]);
}

#[tokio::test]
async fn smembers_set() {
    let m = Mock::array(vec![
        Value::BulkString(b"a".to_vec()),
        Value::BulkString(b"b".to_vec()),
    ]);
    let members = m.smembers("s").await.unwrap();
    m.assert_args(&["SMEMBERS", "s"]);
    assert!(members.contains(&Bytes::from_static(b"a")));
    assert!(members.contains(&Bytes::from_static(b"b")));
}

#[tokio::test]
async fn scard_sismember() {
    let m = Mock::int(3);
    assert_eq!(m.scard("s").await.unwrap(), 3);
    m.assert_args(&["SCARD", "s"]);

    let m = Mock::int(1);
    assert!(m.sismember("s", "a").await.unwrap());
    m.assert_args(&["SISMEMBER", "s", "a"]);
}

#[tokio::test]
async fn smismember_vec() {
    let m = Mock::array(vec![Value::Int(1), Value::Int(0)]);
    assert_eq!(
        m.smismember("s", &["a", "b"]).await.unwrap(),
        vec![true, false]
    );
    m.assert_args(&["SMISMEMBER", "s", "a", "b"]);
}

#[tokio::test]
async fn spop_variants() {
    let m = Mock::bulk("a");
    assert_eq!(m.spop("s").await.unwrap(), Some(Bytes::from_static(b"a")));
    m.assert_args(&["SPOP", "s"]);

    let m = Mock::array(vec![Value::BulkString(b"a".to_vec())]);
    m.spop_count("s", 2).await.unwrap();
    m.assert_args(&["SPOP", "s", "2"]);
}

#[tokio::test]
async fn srandmember_variants() {
    let m = Mock::bulk("a");
    m.srandmember("s").await.unwrap();
    m.assert_args(&["SRANDMEMBER", "s"]);

    let m = Mock::array(vec![Value::BulkString(b"a".to_vec())]);
    m.srandmember_count("s", -5).await.unwrap();
    m.assert_args(&["SRANDMEMBER", "s", "-5"]);
}

#[tokio::test]
async fn set_ops() {
    let m = Mock::array(vec![Value::BulkString(b"a".to_vec())]);
    m.sunion(&["s1", "s2"]).await.unwrap();
    m.assert_args(&["SUNION", "s1", "s2"]);

    let m = Mock::array(vec![Value::BulkString(b"a".to_vec())]);
    m.sinter(&["s1", "s2"]).await.unwrap();
    m.assert_args(&["SINTER", "s1", "s2"]);

    let m = Mock::array(vec![Value::BulkString(b"a".to_vec())]);
    m.sdiff(&["s1", "s2"]).await.unwrap();
    m.assert_args(&["SDIFF", "s1", "s2"]);
}

#[tokio::test]
async fn sintercard_variants() {
    let m = Mock::int(2);
    assert_eq!(m.sintercard(&["s1", "s2"]).await.unwrap(), 2);
    m.assert_args(&["SINTERCARD", "2", "s1", "s2"]);

    let m = Mock::int(1);
    m.sintercard_limit(&["s1", "s2"], 1).await.unwrap();
    m.assert_args(&["SINTERCARD", "2", "s1", "s2", "LIMIT", "1"]);
}

#[tokio::test]
async fn set_op_stores() {
    let m = Mock::int(3);
    m.sunionstore("dest", &["s1", "s2"]).await.unwrap();
    m.assert_args(&["SUNIONSTORE", "dest", "s1", "s2"]);

    let m = Mock::int(1);
    m.sinterstore("dest", &["s1", "s2"]).await.unwrap();
    m.assert_args(&["SINTERSTORE", "dest", "s1", "s2"]);

    let m = Mock::int(1);
    m.sdiffstore("dest", &["s1", "s2"]).await.unwrap();
    m.assert_args(&["SDIFFSTORE", "dest", "s1", "s2"]);
}

#[tokio::test]
async fn smove_encoding() {
    let m = Mock::int(1);
    assert!(m.smove("src", "dst", "m").await.unwrap());
    m.assert_args(&["SMOVE", "src", "dst", "m"]);
}

#[tokio::test]
async fn sscan_encoding() {
    let m = Mock::array(vec![
        Value::BulkString(b"0".to_vec()),
        Value::Array(vec![Value::BulkString(b"a".to_vec())]),
    ]);
    let (cursor, members) = m.sscan("s", "0", Some(b"a*"), Some(10)).await.unwrap();
    m.assert_args(&["SSCAN", "s", "0", "MATCH", "a*", "COUNT", "10"]);
    assert_eq!(cursor, "0");
    assert_eq!(members, vec![Bytes::from_static(b"a")]);
}
