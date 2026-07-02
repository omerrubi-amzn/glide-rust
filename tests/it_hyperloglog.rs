// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Per-command HyperLogLog integration tests (RESP2 + RESP3).

mod common;

use glide::{HyperLogLogCommands, StringCommands};

matrix_test!(pfadd_pfcount, c, {
    let k = common::key("hll");
    assert!(c.pfadd(&k, &["a", "b", "c"]).await.unwrap());
    let count = c.pfcount(&[&k]).await.unwrap();
    assert_eq!(count, 3);
});

matrix_test!(pfadd_duplicate_no_change, c, {
    let k = common::key("hll");
    c.pfadd(&k, &["a", "b", "c"]).await.unwrap();
    // Adding only existing elements should not alter the registers.
    assert!(!c.pfadd(&k, &["a"]).await.unwrap());
});

matrix_test!(pfcount_missing_zero, c, {
    assert_eq!(c.pfcount(&[common::key("hll")]).await.unwrap(), 0);
});

matrix_test!(pfcount_union, c, {
    let k1 = common::tkey("hll", "1");
    let k2 = common::tkey("hll", "2");
    c.pfadd(&k1, &["a", "b", "c"]).await.unwrap();
    c.pfadd(&k2, &["c", "d", "e"]).await.unwrap();
    // Union cardinality across keys ~5 unique.
    assert_eq!(c.pfcount(&[&k1, &k2]).await.unwrap(), 5);
});

matrix_test!(pfmerge, c, {
    let k1 = common::tkey("hll", "1");
    let k2 = common::tkey("hll", "2");
    let dst = common::tkey("hll", "merged");
    c.pfadd(&k1, &["a", "b"]).await.unwrap();
    c.pfadd(&k2, &["c", "d"]).await.unwrap();
    c.pfmerge(&dst, &[&k1, &k2]).await.unwrap();
    assert_eq!(c.pfcount(&[&dst]).await.unwrap(), 4);
});

matrix_test!(pf_wrong_type_errors, c, {
    // A plain string that is not an HLL raises when counted.
    let k = common::key("wt");
    c.set(&k, "not-an-hll-value").await.unwrap();
    assert_request_error!(c.pfcount(&[&k]).await);
});
