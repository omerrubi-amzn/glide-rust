// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Mock-executor unit tests for the HyperLogLog command family.
use super::Mock;
use crate::commands::hyperloglog::HyperLogLogCommands;

#[tokio::test]
async fn pfadd_encoding_and_bool() {
    let m = Mock::int(1);
    assert!(m.pfadd("hll", &["a", "b", "c"]).await.unwrap());
    m.assert_args(&["PFADD", "hll", "a", "b", "c"]);
}

#[tokio::test]
async fn pfadd_no_change_returns_false() {
    let m = Mock::int(0);
    assert!(!m.pfadd("hll", &["a"]).await.unwrap());
    m.assert_args(&["PFADD", "hll", "a"]);
}

#[tokio::test]
async fn pfcount_single_and_multi() {
    let m = Mock::int(42);
    assert_eq!(m.pfcount(&["h1"]).await.unwrap(), 42);
    m.assert_args(&["PFCOUNT", "h1"]);

    let m = Mock::int(100);
    assert_eq!(m.pfcount(&["h1", "h2", "h3"]).await.unwrap(), 100);
    m.assert_args(&["PFCOUNT", "h1", "h2", "h3"]);
}

#[tokio::test]
async fn pfmerge_encoding() {
    let m = Mock::ok();
    m.pfmerge("dest", &["s1", "s2"]).await.unwrap();
    m.assert_args(&["PFMERGE", "dest", "s1", "s2"]);
}
