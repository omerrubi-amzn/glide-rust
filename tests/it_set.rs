// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Per-command set integration tests (RESP2 + RESP3).

mod common;

use glide::{Bytes, SetCommands, StringCommands};

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

matrix_test!(sadd_scard, c, {
    let k = common::key("s");
    assert_eq!(c.sadd(&k, &["a", "b", "c"]).await.unwrap(), 3);
    // Re-adding existing members returns 0 new.
    assert_eq!(c.sadd(&k, &["a"]).await.unwrap(), 0);
    assert_eq!(c.scard(&k).await.unwrap(), 3);
});

matrix_test!(scard_missing_zero, c, {
    assert_eq!(c.scard(common::key("s")).await.unwrap(), 0);
});

matrix_test!(srem, c, {
    let k = common::key("s");
    c.sadd(&k, &["a", "b", "c"]).await.unwrap();
    assert_eq!(c.srem(&k, &["a", "missing"]).await.unwrap(), 1);
    assert_eq!(c.scard(&k).await.unwrap(), 2);
});

matrix_test!(smembers, c, {
    let k = common::key("s");
    c.sadd(&k, &["a", "b", "c"]).await.unwrap();
    let members = c.smembers(&k).await.unwrap();
    assert_eq!(members.len(), 3);
    assert!(members.contains(&b("a")));
});

matrix_test!(smembers_missing_empty, c, {
    assert!(c.smembers(common::key("s")).await.unwrap().is_empty());
});

matrix_test!(sismember, c, {
    let k = common::key("s");
    c.sadd(&k, &["a"]).await.unwrap();
    assert!(c.sismember(&k, "a").await.unwrap());
    assert!(!c.sismember(&k, "b").await.unwrap());
});

matrix_test!(smismember, c, {
    let k = common::key("s");
    c.sadd(&k, &["a", "b"]).await.unwrap();
    assert_eq!(
        c.smismember(&k, &["a", "x", "b"]).await.unwrap(),
        vec![true, false, true]
    );
});

matrix_test!(spop, c, {
    let k = common::key("s");
    c.sadd(&k, &["only"]).await.unwrap();
    assert_eq!(c.spop(&k).await.unwrap().as_deref(), Some(&b"only"[..]));
    // Set now empty -> None.
    assert_eq!(c.spop(&k).await.unwrap(), None);
});

matrix_test!(spop_count, c, {
    let k = common::key("s");
    c.sadd(&k, &["a", "b", "c"]).await.unwrap();
    let popped = c.spop_count(&k, 2).await.unwrap();
    assert_eq!(popped.len(), 2);
    assert_eq!(c.scard(&k).await.unwrap(), 1);
});

matrix_test!(srandmember, c, {
    let k = common::key("s");
    c.sadd(&k, &["only"]).await.unwrap();
    assert_eq!(
        c.srandmember(&k).await.unwrap().as_deref(),
        Some(&b"only"[..])
    );
    // Not removed.
    assert_eq!(c.scard(&k).await.unwrap(), 1);
    assert_eq!(c.srandmember(common::key("x")).await.unwrap(), None);
});

matrix_test!(sunion, c, {
    let s1 = common::tkey("st", "s1");
    let s2 = common::tkey("st", "s2");
    c.sadd(&s1, &["a", "b"]).await.unwrap();
    c.sadd(&s2, &["b", "c"]).await.unwrap();
    let u = c.sunion(&[&s1, &s2]).await.unwrap();
    assert_eq!(u.len(), 3);
});

matrix_test!(sinter, c, {
    let s1 = common::tkey("st", "s1");
    let s2 = common::tkey("st", "s2");
    c.sadd(&s1, &["a", "b"]).await.unwrap();
    c.sadd(&s2, &["b", "c"]).await.unwrap();
    let i = c.sinter(&[&s1, &s2]).await.unwrap();
    assert_eq!(i.len(), 1);
    assert!(i.contains(&b("b")));
});

matrix_test!(sdiff, c, {
    let s1 = common::tkey("st", "s1");
    let s2 = common::tkey("st", "s2");
    c.sadd(&s1, &["a", "b", "c"]).await.unwrap();
    c.sadd(&s2, &["b"]).await.unwrap();
    let d = c.sdiff(&[&s1, &s2]).await.unwrap();
    assert_eq!(d.len(), 2);
    assert!(!d.contains(&b("b")));
});

matrix_test!(sintercard, c, {
    let s1 = common::tkey("st", "s1");
    let s2 = common::tkey("st", "s2");
    c.sadd(&s1, &["a", "b", "c"]).await.unwrap();
    c.sadd(&s2, &["b", "c", "d"]).await.unwrap();
    assert_eq!(c.sintercard(&[&s1, &s2]).await.unwrap(), 2);
});

matrix_test!(sunionstore, c, {
    let s1 = common::tkey("st", "s1");
    let s2 = common::tkey("st", "s2");
    let dst = common::tkey("st", "dst");
    c.sadd(&s1, &["a", "b"]).await.unwrap();
    c.sadd(&s2, &["b", "c"]).await.unwrap();
    assert_eq!(c.sunionstore(&dst, &[&s1, &s2]).await.unwrap(), 3);
    assert_eq!(c.scard(&dst).await.unwrap(), 3);
});

matrix_test!(sinterstore, c, {
    let s1 = common::tkey("st", "s1");
    let s2 = common::tkey("st", "s2");
    let dst = common::tkey("st", "dst");
    c.sadd(&s1, &["a", "b"]).await.unwrap();
    c.sadd(&s2, &["b", "c"]).await.unwrap();
    assert_eq!(c.sinterstore(&dst, &[&s1, &s2]).await.unwrap(), 1);
});

matrix_test!(sdiffstore, c, {
    let s1 = common::tkey("st", "s1");
    let s2 = common::tkey("st", "s2");
    let dst = common::tkey("st", "dst");
    c.sadd(&s1, &["a", "b", "c"]).await.unwrap();
    c.sadd(&s2, &["b"]).await.unwrap();
    assert_eq!(c.sdiffstore(&dst, &[&s1, &s2]).await.unwrap(), 2);
});

matrix_test!(smove, c, {
    let src = common::tkey("st", "src");
    let dst = common::tkey("st", "dst");
    c.sadd(&src, &["a", "b"]).await.unwrap();
    assert!(c.smove(&src, &dst, "a").await.unwrap());
    assert!(!c.smove(&src, &dst, "missing").await.unwrap());
    assert!(c.sismember(&dst, "a").await.unwrap());
});

matrix_test!(set_wrong_type_errors, c, {
    let k = common::key("wt");
    c.set(&k, "notaset").await.unwrap();
    assert_request_error!(c.sadd(&k, &["x"]).await);
});
