// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Per-command sorted-set integration tests (RESP2 + RESP3).

mod common;

use glide::commands::sorted_set::{AggregationType, LexBound, ScoreBound};
use glide::{SortedSetCommands, StringCommands};

matrix_test!(zadd_zcard, c, {
    let k = common::key("z");
    assert_eq!(
        c.zadd(&k, &[("a", 1.0), ("b", 2.0), ("d", 3.0)])
            .await
            .unwrap(),
        3
    );
    // Re-adding updates score, returns 0 new.
    assert_eq!(c.zadd(&k, &[("a", 10.0)]).await.unwrap(), 0);
    assert_eq!(c.zcard(&k).await.unwrap(), 3);
});

matrix_test!(zcard_missing_zero, c, {
    assert_eq!(c.zcard(common::key("z")).await.unwrap(), 0);
});

matrix_test!(zadd_incr, c, {
    let k = common::key("z");
    c.zadd(&k, &[("a", 1.0)]).await.unwrap();
    let v = c.zadd_incr(&k, "a", 4.0).await.unwrap();
    assert_eq!(v, Some(5.0));
});

matrix_test!(zrem, c, {
    let k = common::key("z");
    c.zadd(&k, &[("a", 1.0), ("b", 2.0)]).await.unwrap();
    assert_eq!(c.zrem(&k, &["a", "missing"]).await.unwrap(), 1);
    assert_eq!(c.zcard(&k).await.unwrap(), 1);
});

matrix_test!(zscore, c, {
    let k = common::key("z");
    c.zadd(&k, &[("a", 1.5)]).await.unwrap();
    assert_eq!(c.zscore(&k, "a").await.unwrap(), Some(1.5));
    assert_eq!(c.zscore(&k, "missing").await.unwrap(), None);
});

matrix_test!(zmscore, c, {
    let k = common::key("z");
    c.zadd(&k, &[("a", 1.0), ("b", 2.0)]).await.unwrap();
    assert_eq!(
        c.zmscore(&k, &["a", "x", "b"]).await.unwrap(),
        vec![Some(1.0), None, Some(2.0)]
    );
});

matrix_test!(zcount, c, {
    let k = common::key("z");
    c.zadd(&k, &[("a", 1.0), ("b", 2.0), ("d", 3.0)])
        .await
        .unwrap();
    assert_eq!(
        c.zcount(
            &k,
            ScoreBound::NegativeInfinity,
            ScoreBound::PositiveInfinity
        )
        .await
        .unwrap(),
        3
    );
    assert_eq!(
        c.zcount(&k, ScoreBound::Inclusive(2.0), ScoreBound::PositiveInfinity)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        c.zcount(&k, ScoreBound::Exclusive(2.0), ScoreBound::PositiveInfinity)
            .await
            .unwrap(),
        1
    );
});

matrix_test!(zlexcount, c, {
    let k = common::key("z");
    c.zadd(&k, &[("a", 0.0), ("b", 0.0), ("d", 0.0)])
        .await
        .unwrap();
    assert_eq!(
        c.zlexcount(&k, &LexBound::NegativeInfinity, &LexBound::PositiveInfinity)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        c.zlexcount(
            &k,
            &LexBound::Inclusive(b"b".to_vec()),
            &LexBound::PositiveInfinity
        )
        .await
        .unwrap(),
        2
    );
});

matrix_test!(zrange_by_index, c, {
    let k = common::key("z");
    c.zadd(&k, &[("a", 1.0), ("b", 2.0), ("d", 3.0)])
        .await
        .unwrap();
    let asc = c.zrange_by_index(&k, 0, -1, false).await.unwrap();
    let asc: Vec<_> = asc.iter().map(|b| b.as_ref()).collect();
    assert_eq!(asc, vec![&b"a"[..], &b"b"[..], &b"d"[..]]);
    let rev = c.zrange_by_index(&k, 0, -1, true).await.unwrap();
    let rev: Vec<_> = rev.iter().map(|b| b.as_ref()).collect();
    assert_eq!(rev, vec![&b"d"[..], &b"b"[..], &b"a"[..]]);
});

matrix_test!(zrange_withscores, c, {
    let k = common::key("z");
    c.zadd(&k, &[("a", 1.0), ("b", 2.0)]).await.unwrap();
    let ws = c.zrange_withscores(&k, 0, -1, false).await.unwrap();
    assert_eq!(ws.len(), 2);
    assert_eq!(ws[0].0.as_ref(), b"a");
    assert_eq!(ws[0].1, 1.0);
    assert_eq!(ws[1].1, 2.0);
});

matrix_test!(zrangebyscore, c, {
    let k = common::key("z");
    c.zadd(&k, &[("a", 1.0), ("b", 2.0), ("d", 3.0)])
        .await
        .unwrap();
    let r = c
        .zrangebyscore(&k, ScoreBound::Inclusive(2.0), ScoreBound::PositiveInfinity)
        .await
        .unwrap();
    let r: Vec<_> = r.iter().map(|b| b.as_ref()).collect();
    assert_eq!(r, vec![&b"b"[..], &b"d"[..]]);
});

matrix_test!(zrank_zrevrank, c, {
    let k = common::key("z");
    c.zadd(&k, &[("a", 1.0), ("b", 2.0), ("d", 3.0)])
        .await
        .unwrap();
    assert_eq!(c.zrank(&k, "a").await.unwrap(), Some(0));
    assert_eq!(c.zrank(&k, "d").await.unwrap(), Some(2));
    assert_eq!(c.zrevrank(&k, "d").await.unwrap(), Some(0));
    assert_eq!(c.zrank(&k, "missing").await.unwrap(), None);
});

matrix_test!(zincrby, c, {
    let k = common::key("z");
    c.zadd(&k, &[("a", 1.0)]).await.unwrap();
    assert!((c.zincrby(&k, 5.0, "a").await.unwrap() - 6.0).abs() < 1e-9);
    // ZINCRBY on missing member creates it.
    assert!((c.zincrby(&k, 2.0, "new").await.unwrap() - 2.0).abs() < 1e-9);
});

matrix_test!(zpopmin_zpopmax, c, {
    let k = common::key("z");
    c.zadd(&k, &[("a", 1.0), ("b", 2.0), ("d", 3.0)])
        .await
        .unwrap();
    let min = c.zpopmin(&k).await.unwrap();
    assert_eq!(min[0].0.as_ref(), b"a");
    assert_eq!(min[0].1, 1.0);
    let max = c.zpopmax(&k).await.unwrap();
    assert_eq!(max[0].0.as_ref(), b"d");
    assert_eq!(max[0].1, 3.0);
});

matrix_test!(zpopmin_empty, c, {
    assert!(c.zpopmin(common::key("z")).await.unwrap().is_empty());
});

matrix_test!(zrandmember, c, {
    let k = common::key("z");
    c.zadd(&k, &[("only", 1.0)]).await.unwrap();
    assert_eq!(
        c.zrandmember(&k).await.unwrap().as_deref(),
        Some(&b"only"[..])
    );
    assert_eq!(c.zrandmember(common::key("x")).await.unwrap(), None);
});

matrix_test!(zunionstore, c, {
    let z1 = common::tkey("zs", "z1");
    let z2 = common::tkey("zs", "z2");
    let dst = common::tkey("zs", "dst");
    c.zadd(&z1, &[("a", 1.0), ("b", 2.0)]).await.unwrap();
    c.zadd(&z2, &[("b", 10.0), ("d", 3.0)]).await.unwrap();
    assert_eq!(
        c.zunionstore(&dst, &[&z1, &z2], Some(AggregationType::Max))
            .await
            .unwrap(),
        3
    );
    assert_eq!(c.zscore(&dst, "b").await.unwrap(), Some(10.0));
});

matrix_test!(zinterstore, c, {
    let z1 = common::tkey("zs", "z1");
    let z2 = common::tkey("zs", "z2");
    let dst = common::tkey("zs", "dst");
    c.zadd(&z1, &[("a", 1.0), ("b", 2.0)]).await.unwrap();
    c.zadd(&z2, &[("b", 10.0), ("d", 3.0)]).await.unwrap();
    assert_eq!(
        c.zinterstore(&dst, &[&z1, &z2], Some(AggregationType::Sum))
            .await
            .unwrap(),
        1
    );
    assert_eq!(c.zscore(&dst, "b").await.unwrap(), Some(12.0));
});

matrix_test!(zset_wrong_type_errors, c, {
    let k = common::key("wt");
    c.set(&k, "notazset").await.unwrap();
    assert_request_error!(c.zadd(&k, &[("a", 1.0)]).await);
});
