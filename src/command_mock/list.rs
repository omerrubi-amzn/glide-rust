// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Mock-executor unit tests for the list command family.
use super::Mock;
use crate::commands::list::ListCommands;
use crate::commands::options::{InsertPosition, ListDirection};
use bytes::Bytes;
use redis::Value;

#[tokio::test]
async fn push_variants() {
    let m = Mock::int(2);
    assert_eq!(m.lpush("l", &["a", "b"]).await.unwrap(), 2);
    m.assert_args(&["LPUSH", "l", "a", "b"]);

    let m = Mock::int(2);
    m.rpush("l", &["a", "b"]).await.unwrap();
    m.assert_args(&["RPUSH", "l", "a", "b"]);

    let m = Mock::int(0);
    m.lpushx("l", &["a"]).await.unwrap();
    m.assert_args(&["LPUSHX", "l", "a"]);

    let m = Mock::int(0);
    m.rpushx("l", &["a"]).await.unwrap();
    m.assert_args(&["RPUSHX", "l", "a"]);
}

#[tokio::test]
async fn pop_variants() {
    let m = Mock::bulk("a");
    assert_eq!(m.lpop("l").await.unwrap(), Some(Bytes::from_static(b"a")));
    m.assert_args(&["LPOP", "l"]);

    let m = Mock::array(vec![Value::BulkString(b"a".to_vec())]);
    m.lpop_count("l", 2).await.unwrap();
    m.assert_args(&["LPOP", "l", "2"]);

    let m = Mock::bulk("z");
    m.rpop("l").await.unwrap();
    m.assert_args(&["RPOP", "l"]);

    let m = Mock::array(vec![Value::BulkString(b"z".to_vec())]);
    m.rpop_count("l", 3).await.unwrap();
    m.assert_args(&["RPOP", "l", "3"]);
}

#[tokio::test]
async fn lrange_llen_lindex() {
    let m = Mock::array(vec![
        Value::BulkString(b"a".to_vec()),
        Value::BulkString(b"b".to_vec()),
    ]);
    let v = m.lrange("l", 0, -1).await.unwrap();
    m.assert_args(&["LRANGE", "l", "0", "-1"]);
    assert_eq!(v, vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]);

    let m = Mock::int(2);
    assert_eq!(m.llen("l").await.unwrap(), 2);
    m.assert_args(&["LLEN", "l"]);

    let m = Mock::bulk("a");
    assert_eq!(
        m.lindex("l", 0).await.unwrap(),
        Some(Bytes::from_static(b"a"))
    );
    m.assert_args(&["LINDEX", "l", "0"]);
}

#[tokio::test]
async fn lset_ltrim() {
    let m = Mock::ok();
    m.lset("l", 0, "x").await.unwrap();
    m.assert_args(&["LSET", "l", "0", "x"]);

    let m = Mock::ok();
    m.ltrim("l", 1, 3).await.unwrap();
    m.assert_args(&["LTRIM", "l", "1", "3"]);
}

#[tokio::test]
async fn lrem_linsert() {
    let m = Mock::int(1);
    assert_eq!(m.lrem("l", -1, "a").await.unwrap(), 1);
    m.assert_args(&["LREM", "l", "-1", "a"]);

    let m = Mock::int(3);
    m.linsert("l", InsertPosition::Before, "pivot", "x")
        .await
        .unwrap();
    m.assert_args(&["LINSERT", "l", "BEFORE", "pivot", "x"]);

    let m = Mock::int(3);
    m.linsert("l", InsertPosition::After, "pivot", "x")
        .await
        .unwrap();
    m.assert_args(&["LINSERT", "l", "AFTER", "pivot", "x"]);
}

#[tokio::test]
async fn lmove_encoding() {
    let m = Mock::bulk("a");
    m.lmove("src", "dst", ListDirection::Left, ListDirection::Right)
        .await
        .unwrap();
    m.assert_args(&["LMOVE", "src", "dst", "LEFT", "RIGHT"]);
}

#[tokio::test]
async fn lpos_present_and_absent() {
    let m = Mock::int(4);
    assert_eq!(m.lpos("l", "a").await.unwrap(), Some(4));
    m.assert_args(&["LPOS", "l", "a"]);

    let m = Mock::nil();
    assert_eq!(m.lpos("l", "a").await.unwrap(), None);
}

#[tokio::test]
async fn blpop_brpop() {
    let m = Mock::array(vec![
        Value::BulkString(b"l1".to_vec()),
        Value::BulkString(b"a".to_vec()),
    ]);
    let r = m.blpop(&["l1", "l2"], 1.0).await.unwrap();
    let args = m.args();
    assert_eq!(&args[0..3], &["BLPOP", "l1", "l2"]);
    assert_eq!(
        r,
        Some((Bytes::from_static(b"l1"), Bytes::from_static(b"a")))
    );

    let m = Mock::nil();
    assert_eq!(m.brpop(&["l1"], 0.5).await.unwrap(), None);
    assert_eq!(m.args()[0], "BRPOP");
}

#[tokio::test]
async fn blmove_brpoplpush() {
    let m = Mock::bulk("a");
    m.blmove("src", "dst", ListDirection::Left, ListDirection::Right, 1.0)
        .await
        .unwrap();
    let args = m.args();
    assert_eq!(&args[0..5], &["BLMOVE", "src", "dst", "LEFT", "RIGHT"]);

    let m = Mock::bulk("a");
    m.brpoplpush("src", "dst", 1.0).await.unwrap();
    let args = m.args();
    assert_eq!(&args[0..3], &["BRPOPLPUSH", "src", "dst"]);
}

#[tokio::test]
async fn lmpop_and_blmpop() {
    let m = Mock::array(vec![
        Value::BulkString(b"l1".to_vec()),
        Value::Array(vec![Value::BulkString(b"a".to_vec())]),
    ]);
    let r = m
        .lmpop(&["l1", "l2"], ListDirection::Left, Some(2))
        .await
        .unwrap();
    m.assert_args(&["LMPOP", "2", "l1", "l2", "LEFT", "COUNT", "2"]);
    assert_eq!(
        r,
        Some((Bytes::from_static(b"l1"), vec![Bytes::from_static(b"a")]))
    );

    let m = Mock::nil();
    m.blmpop(&["l1"], ListDirection::Right, None, 1.0)
        .await
        .unwrap();
    let args = m.args();
    assert_eq!(args[0], "BLMPOP");
    // numkeys/keys/direction follow the timeout token
    assert!(args.contains(&"RIGHT".to_string()));
    assert!(args.contains(&"l1".to_string()));
}
