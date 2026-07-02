// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Per-command list integration tests (RESP2 + RESP3).

mod common;

use glide::commands::options::{InsertPosition, ListDirection};
use glide::{ListCommands, StringCommands};

matrix_test!(rpush_lpush_llen, c, {
    let k = common::key("l");
    assert_eq!(c.rpush(&k, &["a", "b", "c"]).await.unwrap(), 3);
    assert_eq!(c.lpush(&k, &["z"]).await.unwrap(), 4);
    assert_eq!(c.llen(&k).await.unwrap(), 4);
});

matrix_test!(llen_missing_is_zero, c, {
    assert_eq!(c.llen(common::key("l")).await.unwrap(), 0);
});

matrix_test!(lpushx_rpushx_require_existing, c, {
    let k = common::key("l");
    // X variants no-op on a missing key.
    assert_eq!(c.lpushx(&k, &["a"]).await.unwrap(), 0);
    assert_eq!(c.rpushx(&k, &["a"]).await.unwrap(), 0);
    c.rpush(&k, &["x"]).await.unwrap();
    assert_eq!(c.lpushx(&k, &["y"]).await.unwrap(), 2);
    assert_eq!(c.rpushx(&k, &["z"]).await.unwrap(), 3);
});

matrix_test!(lrange_full_and_negative, c, {
    let k = common::key("l");
    c.rpush(&k, &["a", "b", "c", "d"]).await.unwrap();
    let all = c.lrange(&k, 0, -1).await.unwrap();
    assert_eq!(all.len(), 4);
    let tail = c.lrange(&k, -2, -1).await.unwrap();
    let tail: Vec<_> = tail.iter().map(|b| b.as_ref()).collect();
    assert_eq!(tail, vec![&b"c"[..], &b"d"[..]]);
});

matrix_test!(lrange_missing_empty, c, {
    assert!(c.lrange(common::key("l"), 0, -1).await.unwrap().is_empty());
});

matrix_test!(lindex, c, {
    let k = common::key("l");
    c.rpush(&k, &["a", "b", "c"]).await.unwrap();
    assert_eq!(c.lindex(&k, 0).await.unwrap().as_deref(), Some(&b"a"[..]));
    assert_eq!(c.lindex(&k, -1).await.unwrap().as_deref(), Some(&b"c"[..]));
    // Out-of-range index -> None.
    assert_eq!(c.lindex(&k, 99).await.unwrap(), None);
});

matrix_test!(lpop_rpop, c, {
    let k = common::key("l");
    c.rpush(&k, &["a", "b", "c"]).await.unwrap();
    assert_eq!(c.lpop(&k).await.unwrap().as_deref(), Some(&b"a"[..]));
    assert_eq!(c.rpop(&k).await.unwrap().as_deref(), Some(&b"c"[..]));
});

matrix_test!(lpop_rpop_missing_none, c, {
    assert_eq!(c.lpop(common::key("l")).await.unwrap(), None);
    assert_eq!(c.rpop(common::key("l2")).await.unwrap(), None);
});

matrix_test!(lpop_rpop_count, c, {
    let k = common::key("l");
    c.rpush(&k, &["a", "b", "c", "d"]).await.unwrap();
    let front = c.lpop_count(&k, 2).await.unwrap();
    let front: Vec<_> = front.iter().map(|b| b.as_ref()).collect();
    assert_eq!(front, vec![&b"a"[..], &b"b"[..]]);
    let back = c.rpop_count(&k, 2).await.unwrap();
    let back: Vec<_> = back.iter().map(|b| b.as_ref()).collect();
    assert_eq!(back, vec![&b"d"[..], &b"c"[..]]);
});

matrix_test!(lset, c, {
    let k = common::key("l");
    c.rpush(&k, &["a", "b", "c"]).await.unwrap();
    c.lset(&k, 1, "B").await.unwrap();
    assert_eq!(c.lindex(&k, 1).await.unwrap().as_deref(), Some(&b"B"[..]));
});

matrix_test!(lset_out_of_range_errors, c, {
    let k = common::key("l");
    c.rpush(&k, &["a"]).await.unwrap();
    assert_request_error!(c.lset(&k, 5, "x").await);
});

matrix_test!(ltrim, c, {
    let k = common::key("l");
    c.rpush(&k, &["a", "b", "c", "d", "e"]).await.unwrap();
    c.ltrim(&k, 1, 3).await.unwrap();
    let remaining = c.lrange(&k, 0, -1).await.unwrap();
    let remaining: Vec<_> = remaining.iter().map(|b| b.as_ref()).collect();
    assert_eq!(remaining, vec![&b"b"[..], &b"c"[..], &b"d"[..]]);
});

matrix_test!(lrem, c, {
    let k = common::key("l");
    c.rpush(&k, &["a", "b", "a", "c", "a"]).await.unwrap();
    // Remove 2 occurrences from the head.
    assert_eq!(c.lrem(&k, 2, "a").await.unwrap(), 2);
    assert_eq!(c.llen(&k).await.unwrap(), 3);
});

matrix_test!(linsert, c, {
    let k = common::key("l");
    c.rpush(&k, &["a", "c"]).await.unwrap();
    c.linsert(&k, InsertPosition::Before, "c", "b")
        .await
        .unwrap();
    let all = c.lrange(&k, 0, -1).await.unwrap();
    let all: Vec<_> = all.iter().map(|b| b.as_ref()).collect();
    assert_eq!(all, vec![&b"a"[..], &b"b"[..], &b"c"[..]]);
});

matrix_test!(linsert_missing_pivot_returns_neg1, c, {
    let k = common::key("l");
    c.rpush(&k, &["a"]).await.unwrap();
    assert_eq!(
        c.linsert(&k, InsertPosition::After, "zzz", "x")
            .await
            .unwrap(),
        -1
    );
});

matrix_test!(lmove, c, {
    let src = common::tkey("ls", "src");
    let dst = common::tkey("ls", "dst");
    c.rpush(&src, &["a", "b", "c"]).await.unwrap();
    let moved = c
        .lmove(&src, &dst, ListDirection::Left, ListDirection::Right)
        .await
        .unwrap();
    assert_eq!(moved.as_deref(), Some(&b"a"[..]));
    assert_eq!(c.lrange(&dst, 0, -1).await.unwrap()[0].as_ref(), b"a");
});

matrix_test!(lpos, c, {
    let k = common::key("l");
    c.rpush(&k, &["a", "b", "c", "b"]).await.unwrap();
    assert_eq!(c.lpos(&k, "b").await.unwrap(), Some(1));
    assert_eq!(c.lpos(&k, "missing").await.unwrap(), None);
});

matrix_test!(list_wrong_type_errors, c, {
    let k = common::key("wt");
    c.set(&k, "notalist").await.unwrap();
    assert_request_error!(c.lpush(&k, &["x"]).await);
});
