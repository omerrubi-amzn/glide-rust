// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Mock-executor unit tests for the sorted-set command family.
use super::Mock;
use crate::commands::options::ConditionalChange;
use crate::commands::sorted_set::{
    AggregationType, LexBound, ScoreBound, ScoreFilter, SortedSetCommands, ZAddOptions,
};
use bytes::Bytes;
use redis::Value;

#[tokio::test]
async fn zadd_and_incr() {
    let m = Mock::int(2);
    let n = m.zadd("z", &[("m1", 1.5), ("m2", 2.5)]).await.unwrap();
    m.assert_args(&["ZADD", "z", "1.5", "m1", "2.5", "m2"]);
    assert_eq!(n, 2);

    let m = Mock::double(3.5);
    let v = m.zadd_incr("z", "m1", 1.5).await.unwrap();
    m.assert_args(&["ZADD", "z", "INCR", "1.5", "m1"]);
    assert_eq!(v, Some(3.5));
}

#[tokio::test]
async fn zadd_options_encoding() {
    let m = Mock::int(1);
    let opts = ZAddOptions {
        conditional_change: Some(ConditionalChange::OnlyIfExists),
        changed: true,
        ..Default::default()
    };
    m.zadd_options("z", &[("m1", 1.5)], opts).await.unwrap();
    m.assert_args(&["ZADD", "z", "XX", "CH", "1.5", "m1"]);
}

#[tokio::test]
async fn zrem_zcard() {
    let m = Mock::int(1);
    m.zrem("z", &["m1"]).await.unwrap();
    m.assert_args(&["ZREM", "z", "m1"]);

    let m = Mock::int(5);
    assert_eq!(m.zcard("z").await.unwrap(), 5);
    m.assert_args(&["ZCARD", "z"]);
}

#[tokio::test]
async fn zscore_and_zmscore() {
    let m = Mock::double(1.5);
    assert_eq!(m.zscore("z", "m1").await.unwrap(), Some(1.5));
    m.assert_args(&["ZSCORE", "z", "m1"]);

    let m = Mock::array(vec![Value::Double(1.5), Value::Nil]);
    let v = m.zmscore("z", &["m1", "m2"]).await.unwrap();
    m.assert_args(&["ZMSCORE", "z", "m1", "m2"]);
    assert_eq!(v, vec![Some(1.5), None]);
}

#[tokio::test]
async fn zcount_and_zlexcount() {
    let m = Mock::int(3);
    m.zcount(
        "z",
        ScoreBound::NegativeInfinity,
        ScoreBound::PositiveInfinity,
    )
    .await
    .unwrap();
    m.assert_args(&["ZCOUNT", "z", "-inf", "+inf"]);

    let m = Mock::int(2);
    m.zcount("z", ScoreBound::Inclusive(1.0), ScoreBound::Exclusive(5.0))
        .await
        .unwrap();
    m.assert_args(&["ZCOUNT", "z", "1", "(5"]);

    let m = Mock::int(2);
    m.zlexcount(
        "z",
        &LexBound::NegativeInfinity,
        &LexBound::PositiveInfinity,
    )
    .await
    .unwrap();
    m.assert_args(&["ZLEXCOUNT", "z", "-", "+"]);
}

#[tokio::test]
async fn zrange_variants() {
    let m = Mock::array(vec![Value::BulkString(b"m1".to_vec())]);
    m.zrange_by_index("z", 0, -1, false).await.unwrap();
    m.assert_args(&["ZRANGE", "z", "0", "-1"]);

    let m = Mock::array(vec![Value::BulkString(b"m1".to_vec())]);
    m.zrange_by_index("z", 0, -1, true).await.unwrap();
    m.assert_args(&["ZRANGE", "z", "0", "-1", "REV"]);

    let m = Mock::array(vec![
        Value::BulkString(b"m1".to_vec()),
        Value::BulkString(b"1.5".to_vec()),
    ]);
    let v = m.zrange_withscores("z", 0, -1, false).await.unwrap();
    m.assert_args(&["ZRANGE", "z", "0", "-1", "WITHSCORES"]);
    assert_eq!(v, vec![(Bytes::from_static(b"m1"), 1.5)]);
}

#[tokio::test]
async fn zrangebyscore_and_bylex_and_rev() {
    let m = Mock::array(vec![Value::BulkString(b"m1".to_vec())]);
    m.zrangebyscore("z", ScoreBound::Inclusive(1.0), ScoreBound::Inclusive(5.0))
        .await
        .unwrap();
    m.assert_args(&["ZRANGEBYSCORE", "z", "1", "5"]);

    let m = Mock::array(vec![Value::BulkString(b"m1".to_vec())]);
    m.zrangebylex(
        "z",
        &LexBound::Inclusive(b"a".to_vec()),
        &LexBound::PositiveInfinity,
    )
    .await
    .unwrap();
    m.assert_args(&["ZRANGEBYLEX", "z", "[a", "+"]);

    let m = Mock::array(vec![Value::BulkString(b"m1".to_vec())]);
    m.zrevrangebyscore(
        "z",
        ScoreBound::PositiveInfinity,
        ScoreBound::NegativeInfinity,
    )
    .await
    .unwrap();
    m.assert_args(&["ZREVRANGEBYSCORE", "z", "+inf", "-inf"]);
}

#[tokio::test]
async fn zrank_family() {
    let m = Mock::int(0);
    assert_eq!(m.zrank("z", "m1").await.unwrap(), Some(0));
    m.assert_args(&["ZRANK", "z", "m1"]);

    let m = Mock::nil();
    assert_eq!(m.zrank("z", "missing").await.unwrap(), None);

    let m = Mock::int(2);
    m.zrevrank("z", "m1").await.unwrap();
    m.assert_args(&["ZREVRANK", "z", "m1"]);

    let m = Mock::array(vec![Value::Int(0), Value::Double(1.5)]);
    let r = m.zrank_withscore("z", "m1").await.unwrap();
    m.assert_args(&["ZRANK", "z", "m1", "WITHSCORE"]);
    assert_eq!(r, Some((0, 1.5)));
}

#[tokio::test]
async fn zincrby_and_pops() {
    let m = Mock::double(3.5);
    assert_eq!(m.zincrby("z", 2.5, "m1").await.unwrap(), 3.5);
    m.assert_args(&["ZINCRBY", "z", "2.5", "m1"]);

    let m = Mock::array(vec![
        Value::BulkString(b"m1".to_vec()),
        Value::BulkString(b"1.5".to_vec()),
    ]);
    let v = m.zpopmin("z").await.unwrap();
    m.assert_args(&["ZPOPMIN", "z"]);
    assert_eq!(v, vec![(Bytes::from_static(b"m1"), 1.5)]);

    let m = Mock::array(vec![
        Value::BulkString(b"m2".to_vec()),
        Value::BulkString(b"9.5".to_vec()),
    ]);
    m.zpopmax("z").await.unwrap();
    m.assert_args(&["ZPOPMAX", "z"]);
}

#[tokio::test]
async fn zrandmember_variants() {
    let m = Mock::bulk("m1");
    assert_eq!(
        m.zrandmember("z").await.unwrap(),
        Some(Bytes::from_static(b"m1"))
    );
    m.assert_args(&["ZRANDMEMBER", "z"]);

    let m = Mock::array(vec![Value::BulkString(b"m1".to_vec())]);
    m.zrandmember_count("z", -5).await.unwrap();
    m.assert_args(&["ZRANDMEMBER", "z", "-5"]);

    let m = Mock::array(vec![
        Value::BulkString(b"m1".to_vec()),
        Value::BulkString(b"1.5".to_vec()),
    ]);
    let v = m.zrandmember_withscores("z", 2).await.unwrap();
    m.assert_args(&["ZRANDMEMBER", "z", "2", "WITHSCORES"]);
    assert_eq!(v, vec![(Bytes::from_static(b"m1"), 1.5)]);
}

#[tokio::test]
async fn zstore_ops() {
    let m = Mock::int(3);
    m.zinterstore("dest", &["z1", "z2"], Some(AggregationType::Max))
        .await
        .unwrap();
    m.assert_args(&["ZINTERSTORE", "dest", "2", "z1", "z2", "AGGREGATE", "MAX"]);

    let m = Mock::int(3);
    m.zunionstore("dest", &["z1", "z2"], None).await.unwrap();
    m.assert_args(&["ZUNIONSTORE", "dest", "2", "z1", "z2"]);

    let m = Mock::int(2);
    m.zdiffstore("dest", &["z1", "z2"]).await.unwrap();
    m.assert_args(&["ZDIFFSTORE", "dest", "2", "z1", "z2"]);
}

#[tokio::test]
async fn zdiff_zunion_zinter() {
    let m = Mock::array(vec![Value::BulkString(b"m1".to_vec())]);
    m.zdiff(&["z1", "z2"]).await.unwrap();
    m.assert_args(&["ZDIFF", "2", "z1", "z2"]);

    let m = Mock::array(vec![
        Value::BulkString(b"m1".to_vec()),
        Value::BulkString(b"1.5".to_vec()),
    ]);
    m.zdiff_withscores(&["z1", "z2"]).await.unwrap();
    m.assert_args(&["ZDIFF", "2", "z1", "z2", "WITHSCORES"]);

    let m = Mock::array(vec![Value::BulkString(b"m1".to_vec())]);
    m.zunion(&["z1", "z2"], Some(AggregationType::Sum))
        .await
        .unwrap();
    m.assert_args(&["ZUNION", "2", "z1", "z2", "AGGREGATE", "SUM"]);

    let m = Mock::array(vec![Value::BulkString(b"m1".to_vec())]);
    m.zinter(&["z1", "z2"], None).await.unwrap();
    m.assert_args(&["ZINTER", "2", "z1", "z2"]);
}

#[tokio::test]
async fn zintercard_and_rangestore() {
    let m = Mock::int(1);
    m.zintercard(&["z1", "z2"], Some(10)).await.unwrap();
    m.assert_args(&["ZINTERCARD", "2", "z1", "z2", "LIMIT", "10"]);

    let m = Mock::int(1);
    m.zintercard(&["z1", "z2"], None).await.unwrap();
    m.assert_args(&["ZINTERCARD", "2", "z1", "z2"]);

    let m = Mock::int(3);
    m.zrangestore_by_index("dest", "src", 0, -1, true)
        .await
        .unwrap();
    m.assert_args(&["ZRANGESTORE", "dest", "src", "0", "-1", "REV"]);
}

#[tokio::test]
async fn zremrange_family() {
    let m = Mock::int(1);
    m.zremrangebyrank("z", 0, 1).await.unwrap();
    m.assert_args(&["ZREMRANGEBYRANK", "z", "0", "1"]);

    let m = Mock::int(1);
    m.zremrangebyscore(
        "z",
        ScoreBound::NegativeInfinity,
        ScoreBound::PositiveInfinity,
    )
    .await
    .unwrap();
    m.assert_args(&["ZREMRANGEBYSCORE", "z", "-inf", "+inf"]);

    let m = Mock::int(1);
    m.zremrangebylex(
        "z",
        &LexBound::NegativeInfinity,
        &LexBound::PositiveInfinity,
    )
    .await
    .unwrap();
    m.assert_args(&["ZREMRANGEBYLEX", "z", "-", "+"]);
}

#[tokio::test]
async fn zscan_encoding() {
    let m = Mock::array(vec![
        Value::BulkString(b"0".to_vec()),
        Value::Array(vec![
            Value::BulkString(b"m1".to_vec()),
            Value::BulkString(b"1.5".to_vec()),
        ]),
    ]);
    let (cursor, pairs) = m.zscan("z", "0", Some(b"m*"), Some(10)).await.unwrap();
    m.assert_args(&["ZSCAN", "z", "0", "MATCH", "m*", "COUNT", "10"]);
    assert_eq!(cursor, "0");
    assert_eq!(pairs, vec![(Bytes::from_static(b"m1"), 1.5)]);
}

#[tokio::test]
async fn bzpop_and_zmpop() {
    let m = Mock::array(vec![
        Value::BulkString(b"z1".to_vec()),
        Value::BulkString(b"m1".to_vec()),
        Value::Double(1.5),
    ]);
    let r = m.bzpopmin(&["z1", "z2"], 1.0).await.unwrap();
    let args = m.args();
    assert_eq!(&args[0..3], &["BZPOPMIN", "z1", "z2"]);
    assert_eq!(
        r,
        Some((Bytes::from_static(b"z1"), Bytes::from_static(b"m1"), 1.5))
    );

    let m = Mock::nil();
    assert_eq!(m.bzpopmax(&["z1"], 0.5).await.unwrap(), None);
    assert_eq!(m.args()[0], "BZPOPMAX");

    let m = Mock::array(vec![
        Value::BulkString(b"z1".to_vec()),
        Value::Array(vec![Value::Array(vec![
            Value::BulkString(b"m1".to_vec()),
            Value::BulkString(b"1.5".to_vec()),
        ])]),
    ]);
    let r = m
        .zmpop(&["z1", "z2"], ScoreFilter::Min, Some(2))
        .await
        .unwrap();
    m.assert_args(&["ZMPOP", "2", "z1", "z2", "MIN", "COUNT", "2"]);
    assert_eq!(
        r,
        Some((
            Bytes::from_static(b"z1"),
            vec![(Bytes::from_static(b"m1"), 1.5)]
        ))
    );
}

#[tokio::test]
async fn zrangestore_by_score_forward_no_limit() {
    let m = Mock::int(3);
    let n = m
        .zrangestore_by_score(
            "d",
            "s",
            ScoreBound::Inclusive(1.0),
            ScoreBound::Inclusive(5.0),
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(n, 3);
    m.assert_args(&["ZRANGESTORE", "d", "s", "1", "5", "BYSCORE"]);
}

#[tokio::test]
async fn zrangestore_by_score_rev_swaps_bounds_and_limit() {
    let m = Mock::int(2);
    m.zrangestore_by_score(
        "d",
        "s",
        ScoreBound::Inclusive(1.0),
        ScoreBound::Inclusive(5.0),
        true,
        Some(crate::commands::options::Limit {
            offset: 0,
            count: 10,
        }),
    )
    .await
    .unwrap();
    // REV => high bound emitted first, plus REV + LIMIT.
    m.assert_args(&[
        "ZRANGESTORE",
        "d",
        "s",
        "5",
        "1",
        "BYSCORE",
        "REV",
        "LIMIT",
        "0",
        "10",
    ]);
}

#[tokio::test]
async fn zrangestore_by_lex_forward() {
    let m = Mock::int(1);
    m.zrangestore_by_lex(
        "d",
        "s",
        &LexBound::Inclusive(b"a".to_vec()),
        &LexBound::PositiveInfinity,
        false,
        None,
    )
    .await
    .unwrap();
    m.assert_args(&["ZRANGESTORE", "d", "s", "[a", "+", "BYLEX"]);
}

#[tokio::test]
async fn zrangestore_by_lex_rev_swaps_bounds() {
    let m = Mock::int(1);
    m.zrangestore_by_lex(
        "d",
        "s",
        &LexBound::NegativeInfinity,
        &LexBound::Inclusive(b"z".to_vec()),
        true,
        None,
    )
    .await
    .unwrap();
    // REV => (max, min) order.
    m.assert_args(&["ZRANGESTORE", "d", "s", "[z", "-", "BYLEX", "REV"]);
}
