// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Per-command bitmap integration tests (RESP2 + RESP3).

mod common;

use glide::commands::bitmap::{BitmapIndexType, BitwiseOperation};
use glide::{BitmapCommands, ListCommands};

resp_test!(setbit_getbit, c, {
    let k = common::key("bit");
    // SETBIT returns the previous bit (0 initially).
    assert_eq!(c.setbit(&k, 7, 1).await.unwrap(), 0);
    assert_eq!(c.getbit(&k, 7).await.unwrap(), 1);
    assert_eq!(c.getbit(&k, 0).await.unwrap(), 0);
    // Setting again returns the previous value (1).
    assert_eq!(c.setbit(&k, 7, 0).await.unwrap(), 1);
});

resp_test!(getbit_missing_zero, c, {
    assert_eq!(c.getbit(common::key("bit"), 100).await.unwrap(), 0);
});

resp_test!(bitcount, c, {
    let k = common::key("bit");
    c.setbit(&k, 0, 1).await.unwrap();
    c.setbit(&k, 1, 1).await.unwrap();
    c.setbit(&k, 7, 1).await.unwrap();
    assert_eq!(c.bitcount(&k).await.unwrap(), 3);
});

resp_test!(bitcount_missing_zero, c, {
    assert_eq!(c.bitcount(common::key("bit")).await.unwrap(), 0);
});

resp_test!(bitcount_range_byte, c, {
    let k = common::key("bit");
    // Two bytes: first byte has 8 set bits, second has 0.
    for i in 0..8 {
        c.setbit(&k, i, 1).await.unwrap();
    }
    assert_eq!(
        c.bitcount_range(&k, 0, 0, Some(BitmapIndexType::Byte))
            .await
            .unwrap(),
        8
    );
    assert_eq!(
        c.bitcount_range(&k, 1, 1, Some(BitmapIndexType::Byte))
            .await
            .unwrap(),
        0
    );
});

resp_test!(bitcount_range_bit, c, {
    let k = common::key("bit");
    c.setbit(&k, 5, 1).await.unwrap();
    c.setbit(&k, 6, 1).await.unwrap();
    assert_eq!(
        c.bitcount_range(&k, 0, 7, Some(BitmapIndexType::Bit))
            .await
            .unwrap(),
        2
    );
});

resp_test!(bitpos, c, {
    let k = common::key("bit");
    c.setbit(&k, 10, 1).await.unwrap();
    assert_eq!(c.bitpos(&k, 1).await.unwrap(), 10);
});

resp_test!(bitop_and, c, {
    let a = common::key("a");
    let b = common::key("b");
    let dst = common::key("dst");
    c.setbit(&a, 0, 1).await.unwrap();
    c.setbit(&a, 1, 1).await.unwrap();
    c.setbit(&b, 1, 1).await.unwrap();
    c.bitop(BitwiseOperation::And, &dst, &[&a, &b])
        .await
        .unwrap();
    assert_eq!(c.getbit(&dst, 0).await.unwrap(), 0);
    assert_eq!(c.getbit(&dst, 1).await.unwrap(), 1);
});

resp_test!(bitop_or, c, {
    let a = common::key("a");
    let b = common::key("b");
    let dst = common::key("dst");
    c.setbit(&a, 0, 1).await.unwrap();
    c.setbit(&b, 3, 1).await.unwrap();
    c.bitop(BitwiseOperation::Or, &dst, &[&a, &b])
        .await
        .unwrap();
    assert_eq!(c.getbit(&dst, 0).await.unwrap(), 1);
    assert_eq!(c.getbit(&dst, 3).await.unwrap(), 1);
});

resp_test!(bitop_xor, c, {
    let a = common::key("a");
    let b = common::key("b");
    let dst = common::key("dst");
    c.setbit(&a, 0, 1).await.unwrap();
    c.setbit(&b, 0, 1).await.unwrap();
    c.setbit(&b, 1, 1).await.unwrap();
    c.bitop(BitwiseOperation::Xor, &dst, &[&a, &b])
        .await
        .unwrap();
    assert_eq!(c.getbit(&dst, 0).await.unwrap(), 0);
    assert_eq!(c.getbit(&dst, 1).await.unwrap(), 1);
});

resp_test!(bitop_not, c, {
    let a = common::key("a");
    let dst = common::key("dst");
    c.setbit(&a, 0, 1).await.unwrap();
    c.bitop(BitwiseOperation::Not, &dst, &[&a]).await.unwrap();
    // NOT flips the first bit to 0 and the rest of the byte to 1.
    assert_eq!(c.getbit(&dst, 0).await.unwrap(), 0);
    assert_eq!(c.getbit(&dst, 1).await.unwrap(), 1);
});

resp_test!(bitmap_wrong_type_errors, c, {
    let k = common::key("wt");
    c.rpush(&k, &["x"]).await.unwrap();
    assert_request_error!(c.setbit(&k, 0, 1).await);
});
