// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Per-command string integration tests, run against a live server across both
//! RESP2 and RESP3. Mirrors `test_async_client.py` string coverage: happy path,
//! missing-key behaviour, wrong-type errors, bounds and SET conditions.

mod common;

use glide::StringCommands;
use glide::commands::options::{ConditionalChange, ExpirySet, SetOptions};

matrix_test!(set_and_get, c, {
    let k = common::key("str");
    c.set(&k, "hello").await.unwrap();
    assert_eq!(c.get(&k).await.unwrap().as_deref(), Some(&b"hello"[..]));
});

matrix_test!(get_missing_is_none, c, {
    assert_eq!(c.get(common::key("missing")).await.unwrap(), None);
});

matrix_test!(set_overwrites, c, {
    let k = common::key("str");
    c.set(&k, "a").await.unwrap();
    c.set(&k, "b").await.unwrap();
    assert_eq!(c.get(&k).await.unwrap().as_deref(), Some(&b"b"[..]));
});

matrix_test!(set_binary_value, c, {
    let k = common::key("bin");
    let payload = vec![0u8, 1, 2, 255, 0, 128];
    c.set(&k, payload.clone()).await.unwrap();
    assert_eq!(c.get(&k).await.unwrap().as_deref(), Some(&payload[..]));
});

matrix_test!(append_to_missing_creates, c, {
    let k = common::key("app");
    assert_eq!(c.append(&k, "abc").await.unwrap(), 3);
    assert_eq!(c.append(&k, "de").await.unwrap(), 5);
    assert_eq!(c.get(&k).await.unwrap().as_deref(), Some(&b"abcde"[..]));
});

matrix_test!(strlen_present_and_missing, c, {
    let k = common::key("sl");
    c.set(&k, "hello").await.unwrap();
    assert_eq!(c.strlen(&k).await.unwrap(), 5);
    assert_eq!(c.strlen(common::key("nope")).await.unwrap(), 0);
});

matrix_test!(getrange_bounds, c, {
    let k = common::key("gr");
    c.set(&k, "Hello World").await.unwrap();
    assert_eq!(c.getrange(&k, 0, 4).await.unwrap().as_ref(), b"Hello");
    // Negative indices count from the end.
    assert_eq!(c.getrange(&k, -5, -1).await.unwrap().as_ref(), b"World");
    // Out-of-range start yields empty.
    assert_eq!(c.getrange(&k, 100, 200).await.unwrap().as_ref(), b"");
});

matrix_test!(setrange_extends, c, {
    let k = common::key("sr");
    c.set(&k, "Hello World").await.unwrap();
    let len = c.setrange(&k, 6, "Redis").await.unwrap();
    assert_eq!(len, 11);
    assert_eq!(
        c.get(&k).await.unwrap().as_deref(),
        Some(&b"Hello Redis"[..])
    );
});

matrix_test!(setrange_zero_pads, c, {
    let k = common::key("srp");
    let len = c.setrange(&k, 5, "x").await.unwrap();
    assert_eq!(len, 6);
    assert_eq!(c.get(&k).await.unwrap().unwrap().len(), 6);
});

matrix_test!(incr_decr_family, c, {
    let k = common::key("ctr");
    assert_eq!(c.incr(&k).await.unwrap(), 1);
    assert_eq!(c.incr_by(&k, 9).await.unwrap(), 10);
    assert_eq!(c.decr(&k).await.unwrap(), 9);
    assert_eq!(c.decr_by(&k, 4).await.unwrap(), 5);
});

matrix_test!(incr_on_missing_starts_at_zero, c, {
    let k = common::key("ctr0");
    assert_eq!(c.incr_by(&k, 5).await.unwrap(), 5);
});

matrix_test!(incr_by_float, c, {
    let k = common::key("f");
    c.set(&k, "10.5").await.unwrap();
    let v = c.incr_by_float(&k, 0.1).await.unwrap();
    assert!((v - 10.6).abs() < 1e-9);
});

matrix_test!(incr_non_integer_errors, c, {
    let k = common::key("nonint");
    c.set(&k, "notanumber").await.unwrap();
    assert_request_error!(c.incr(&k).await);
});

matrix_test!(mset_mget, c, {
    let a = common::tkey("ms", "a");
    let b = common::tkey("ms", "b");
    let missing = common::tkey("ms", "m");
    c.mset(&[(&a, "1"), (&b, "2")]).await.unwrap();
    let got = c.mget(&[&a, &b, &missing]).await.unwrap();
    assert_eq!(got[0].as_deref(), Some(&b"1"[..]));
    assert_eq!(got[1].as_deref(), Some(&b"2"[..]));
    assert_eq!(got[2], None);
});

matrix_test!(msetnx_all_or_nothing, c, {
    let a = common::tkey("mn", "a");
    let b = common::tkey("mn", "b");
    assert!(c.msetnx(&[(&a, "1"), (&b, "2")]).await.unwrap());
    // Second call fails because keys already exist.
    assert!(
        !c.msetnx(&[(&a, "x"), (&common::tkey("mn", "c"), "3")])
            .await
            .unwrap()
    );
    assert_eq!(c.get(&a).await.unwrap().as_deref(), Some(&b"1"[..]));
});

matrix_test!(getdel_returns_and_removes, c, {
    let k = common::key("gd");
    c.set(&k, "value").await.unwrap();
    assert_eq!(c.getdel(&k).await.unwrap().as_deref(), Some(&b"value"[..]));
    assert_eq!(c.get(&k).await.unwrap(), None);
    // GETDEL on a missing key is None.
    assert_eq!(c.getdel(common::key("x")).await.unwrap(), None);
});

matrix_test!(getex_sets_expiry, c, {
    let k = common::key("gx");
    c.set(&k, "v").await.unwrap();
    assert_eq!(
        c.getex(&k, Some(ExpirySet::Seconds(100)))
            .await
            .unwrap()
            .as_deref(),
        Some(&b"v"[..])
    );
});

matrix_test!(set_nx_does_not_overwrite, c, {
    let k = common::key("nx");
    c.set(&k, "first").await.unwrap();
    let opts = SetOptions {
        conditional_set: Some(ConditionalChange::OnlyIfDoesNotExist),
        return_old_value: false,
        expiry: None,
    };
    c.set_options(&k, "second", opts).await.unwrap();
    assert_eq!(c.get(&k).await.unwrap().as_deref(), Some(&b"first"[..]));
});

matrix_test!(set_xx_only_if_exists, c, {
    let k = common::key("xx");
    let opts = SetOptions {
        conditional_set: Some(ConditionalChange::OnlyIfExists),
        return_old_value: false,
        expiry: None,
    };
    // XX on a missing key does not set.
    c.set_options(&k, "v", opts).await.unwrap();
    assert_eq!(c.get(&k).await.unwrap(), None);
    // After it exists, XX succeeds.
    c.set(&k, "a").await.unwrap();
    c.set_options(&k, "b", opts).await.unwrap();
    assert_eq!(c.get(&k).await.unwrap().as_deref(), Some(&b"b"[..]));
});

matrix_test!(set_get_returns_old_value, c, {
    let k = common::key("go");
    c.set(&k, "old").await.unwrap();
    let opts = SetOptions {
        conditional_set: None,
        return_old_value: true,
        expiry: None,
    };
    let old = c.set_options(&k, "new", opts).await.unwrap();
    assert_eq!(old.as_deref(), Some(&b"old"[..]));
    assert_eq!(c.get(&k).await.unwrap().as_deref(), Some(&b"new"[..]));
});

matrix_test!(set_with_expiry, c, {
    let k = common::key("ex");
    let opts = SetOptions {
        conditional_set: None,
        return_old_value: false,
        expiry: Some(ExpirySet::Seconds(100)),
    };
    c.set_options(&k, "v", opts).await.unwrap();
    assert_eq!(c.get(&k).await.unwrap().as_deref(), Some(&b"v"[..]));
});

matrix_test!(get_wrong_type_errors, c, {
    // GET against a list key must be a RequestError (WRONGTYPE).
    use glide::ListCommands;
    let k = common::key("wt");
    c.rpush(&k, &["a"]).await.unwrap();
    assert_request_error!(c.get(&k).await);
});

matrix_test!(lcs_len, c, {
    let k1 = common::tkey("lcs", "1");
    let k2 = common::tkey("lcs", "2");
    c.set(&k1, "ohmytext").await.unwrap();
    c.set(&k2, "mynewtext").await.unwrap();
    assert_eq!(c.lcs_len(&k1, &k2).await.unwrap(), 6);
});
