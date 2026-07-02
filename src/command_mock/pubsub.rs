// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Mock-executor unit tests for the Pub/Sub command family.
use super::Mock;
use crate::commands::pubsub::PubSubCommands;
use bytes::Bytes;
use redis::Value;

#[tokio::test]
async fn publish_encoding_and_count() {
    let m = Mock::int(3);
    assert_eq!(m.publish("ch", "hello").await.unwrap(), 3);
    m.assert_args(&["PUBLISH", "ch", "hello"]);
}

#[tokio::test]
async fn spublish_encoding() {
    let m = Mock::int(1);
    assert_eq!(m.spublish("sch", "hi").await.unwrap(), 1);
    m.assert_args(&["SPUBLISH", "sch", "hi"]);
}

#[tokio::test]
async fn pubsub_channels_with_and_without_pattern() {
    let m = Mock::array(vec![Value::BulkString(b"news".to_vec())]);
    let chans = m.pubsub_channels(Some(b"n*")).await.unwrap();
    m.assert_args(&["PUBSUB", "CHANNELS", "n*"]);
    assert_eq!(chans, vec![Bytes::from_static(b"news")]);

    let m = Mock::array(vec![]);
    let _ = m.pubsub_channels(None).await.unwrap();
    m.assert_args(&["PUBSUB", "CHANNELS"]);
}

#[tokio::test]
async fn pubsub_numpat_int() {
    let m = Mock::int(5);
    assert_eq!(m.pubsub_numpat().await.unwrap(), 5);
    m.assert_args(&["PUBSUB", "NUMPAT"]);
}

#[tokio::test]
async fn pubsub_numsub_pairs() {
    let m = Mock::array(vec![
        Value::BulkString(b"a".to_vec()),
        Value::Int(2),
        Value::BulkString(b"b".to_vec()),
        Value::Int(0),
    ]);
    let subs = m.pubsub_numsub(&["a", "b"]).await.unwrap();
    m.assert_args(&["PUBSUB", "NUMSUB", "a", "b"]);
    assert_eq!(
        subs,
        vec![(Bytes::from_static(b"a"), 2), (Bytes::from_static(b"b"), 0)]
    );
}

#[tokio::test]
async fn pubsub_shard_variants() {
    let m = Mock::array(vec![Value::BulkString(b"s".to_vec())]);
    let _ = m.pubsub_shardchannels(None).await.unwrap();
    m.assert_args(&["PUBSUB", "SHARDCHANNELS"]);

    let m = Mock::array(vec![Value::BulkString(b"s".to_vec()), Value::Int(1)]);
    let subs = m.pubsub_shardnumsub(&["s"]).await.unwrap();
    m.assert_args(&["PUBSUB", "SHARDNUMSUB", "s"]);
    assert_eq!(subs, vec![(Bytes::from_static(b"s"), 1)]);
}
