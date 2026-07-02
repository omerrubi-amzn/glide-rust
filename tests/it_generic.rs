// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Per-command generic (key-space) integration tests (RESP2 + RESP3).

mod common;

use glide::commands::options::{ExpireOptions, Limit, OrderBy};
use glide::{GenericCommands, ListCommands, StringCommands};

resp_test!(del_and_exists, c, {
    let a = common::key("a");
    let b = common::key("b");
    c.set(&a, "1").await.unwrap();
    c.set(&b, "2").await.unwrap();
    assert_eq!(c.exists(&[&a, &b, &common::key("m")]).await.unwrap(), 2);
    assert_eq!(c.del(&[&a, &b]).await.unwrap(), 2);
    assert_eq!(c.exists(&[&a]).await.unwrap(), 0);
});

resp_test!(del_missing_zero, c, {
    assert_eq!(c.del(&[common::key("nope")]).await.unwrap(), 0);
});

resp_test!(unlink, c, {
    let a = common::key("a");
    c.set(&a, "1").await.unwrap();
    assert_eq!(c.unlink(&[&a]).await.unwrap(), 1);
});

resp_test!(touch, c, {
    let a = common::key("a");
    let b = common::key("b");
    c.set(&a, "1").await.unwrap();
    c.set(&b, "2").await.unwrap();
    assert_eq!(c.touch(&[&a, &b, &common::key("m")]).await.unwrap(), 2);
});

resp_test!(expire_and_ttl, c, {
    let k = common::key("k");
    c.set(&k, "v").await.unwrap();
    assert!(c.expire(&k, 100).await.unwrap());
    let ttl = c.ttl(&k).await.unwrap();
    assert!(ttl > 0 && ttl <= 100);
    assert!(c.persist(&k).await.unwrap());
    assert_eq!(c.ttl(&k).await.unwrap(), -1);
});

resp_test!(ttl_missing_and_no_expiry, c, {
    let k = common::key("k");
    // Missing key -> -2.
    assert_eq!(c.ttl(&k).await.unwrap(), -2);
    c.set(&k, "v").await.unwrap();
    // No expiry -> -1.
    assert_eq!(c.ttl(&k).await.unwrap(), -1);
});

resp_test!(expire_nx_xx, c, {
    let k = common::key("k");
    c.set(&k, "v").await.unwrap();
    // NX sets only when no expiry exists.
    assert!(
        c.expire_opts(&k, 100, ExpireOptions::HasNoExpiry)
            .await
            .unwrap()
    );
    // NX again fails since an expiry now exists.
    assert!(
        !c.expire_opts(&k, 200, ExpireOptions::HasNoExpiry)
            .await
            .unwrap()
    );
    // XX succeeds since an expiry exists.
    assert!(
        c.expire_opts(&k, 200, ExpireOptions::HasExistingExpiry)
            .await
            .unwrap()
    );
});

resp_test!(expire_gt_lt, c, {
    let k = common::key("k");
    c.set(&k, "v").await.unwrap();
    c.expire(&k, 100).await.unwrap();
    // GT only applies when new > current.
    assert!(
        c.expire_opts(&k, 200, ExpireOptions::NewExpiryGreaterThanCurrent)
            .await
            .unwrap()
    );
    assert!(
        !c.expire_opts(&k, 50, ExpireOptions::NewExpiryGreaterThanCurrent)
            .await
            .unwrap()
    );
    // LT only applies when new < current.
    assert!(
        c.expire_opts(&k, 10, ExpireOptions::NewExpiryLessThanCurrent)
            .await
            .unwrap()
    );
});

resp_test!(pexpire_and_pttl, c, {
    let k = common::key("k");
    c.set(&k, "v").await.unwrap();
    assert!(c.pexpire(&k, 100_000).await.unwrap());
    assert!(c.pttl(&k).await.unwrap() > 0);
});

resp_test!(expireat_pexpireat, c, {
    let k = common::key("k");
    c.set(&k, "v").await.unwrap();
    let future = 4_102_444_800; // year 2100 in seconds
    assert!(c.expireat(&k, future).await.unwrap());
    assert!(c.expiretime(&k).await.unwrap() > 0);
    assert!(c.pexpireat(&k, future * 1000).await.unwrap());
    assert!(c.pexpiretime(&k).await.unwrap() > 0);
});

resp_test!(key_type, c, {
    let s = common::key("s");
    let l = common::key("l");
    c.set(&s, "v").await.unwrap();
    c.rpush(&l, &["a"]).await.unwrap();
    assert_eq!(c.key_type(&s).await.unwrap(), "string");
    assert_eq!(c.key_type(&l).await.unwrap(), "list");
    assert_eq!(c.key_type(common::key("m")).await.unwrap(), "none");
});

resp_test!(rename, c, {
    let k = common::key("k");
    let n = common::key("n");
    c.set(&k, "v").await.unwrap();
    c.rename(&k, &n).await.unwrap();
    assert_eq!(c.exists(&[&k]).await.unwrap(), 0);
    assert_eq!(c.get(&n).await.unwrap().as_deref(), Some(&b"v"[..]));
});

resp_test!(rename_missing_errors, c, {
    assert_request_error!(c.rename(common::key("nope"), common::key("dst")).await);
});

resp_test!(renamenx, c, {
    let k = common::key("k");
    let n = common::key("n");
    c.set(&k, "v").await.unwrap();
    assert!(c.renamenx(&k, &n).await.unwrap());
    // Now target exists; renaming another key onto it fails.
    let k2 = common::key("k2");
    c.set(&k2, "w").await.unwrap();
    assert!(!c.renamenx(&k2, &n).await.unwrap());
});

resp_test!(randomkey_present, c, {
    let k = common::key("k");
    c.set(&k, "v").await.unwrap();
    assert!(c.randomkey().await.unwrap().is_some());
});

resp_test!(dump_missing_none, c, {
    assert_eq!(c.dump(common::key("nope")).await.unwrap(), None);
});

resp_test!(copy, c, {
    let src = common::key("src");
    let dst = common::key("dst");
    c.set(&src, "v").await.unwrap();
    assert!(c.copy(&src, &dst, false).await.unwrap());
    assert_eq!(c.get(&dst).await.unwrap().as_deref(), Some(&b"v"[..]));
    // Without REPLACE, copying onto an existing key fails.
    c.set(&src, "w").await.unwrap();
    assert!(!c.copy(&src, &dst, false).await.unwrap());
    assert!(c.copy(&src, &dst, true).await.unwrap());
    assert_eq!(c.get(&dst).await.unwrap().as_deref(), Some(&b"w"[..]));
});

resp_test!(object_encoding, c, {
    let k = common::key("k");
    c.set(&k, "12345").await.unwrap();
    let enc = c.object_encoding(&k).await.unwrap();
    assert!(enc.is_some());
    assert_eq!(c.object_encoding(common::key("nope")).await.unwrap(), None);
});

resp_test!(sort_numeric, c, {
    let k = common::key("l");
    c.rpush(&k, &["3", "1", "2"]).await.unwrap();
    let asc = c.sort(&k, Some(OrderBy::Asc), None, false).await.unwrap();
    let asc: Vec<_> = asc.iter().map(|b| b.as_ref()).collect();
    assert_eq!(asc, vec![&b"1"[..], &b"2"[..], &b"3"[..]]);
});

resp_test!(sort_with_limit, c, {
    let k = common::key("l");
    c.rpush(&k, &["5", "4", "3", "2", "1"]).await.unwrap();
    let limited = c
        .sort(
            &k,
            Some(OrderBy::Asc),
            Some(Limit {
                offset: 1,
                count: 2,
            }),
            false,
        )
        .await
        .unwrap();
    let limited: Vec<_> = limited.iter().map(|b| b.as_ref()).collect();
    assert_eq!(limited, vec![&b"2"[..], &b"3"[..]]);
});

resp_test!(sort_alpha, c, {
    let k = common::key("l");
    c.rpush(&k, &["banana", "apple", "cherry"]).await.unwrap();
    let sorted = c.sort(&k, Some(OrderBy::Asc), None, true).await.unwrap();
    let sorted: Vec<_> = sorted.iter().map(|b| b.as_ref()).collect();
    assert_eq!(sorted, vec![&b"apple"[..], &b"banana"[..], &b"cherry"[..]]);
});
