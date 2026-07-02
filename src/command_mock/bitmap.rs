// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Mock-executor unit tests for the bitmap command family (command dispatch).
use super::Mock;
use crate::commands::bitmap::{
    BitEncoding, BitFieldOffset, BitFieldSubcommand, BitmapCommands, BitmapIndexType,
    BitwiseOperation,
};
use redis::Value;

#[tokio::test]
async fn setbit_getbit() {
    let m = Mock::int(0);
    assert_eq!(m.setbit("k", 7, 1).await.unwrap(), 0);
    m.assert_args(&["SETBIT", "k", "7", "1"]);

    let m = Mock::int(1);
    assert_eq!(m.getbit("k", 7).await.unwrap(), 1);
    m.assert_args(&["GETBIT", "k", "7"]);
}

#[tokio::test]
async fn bitcount_and_range() {
    let m = Mock::int(10);
    assert_eq!(m.bitcount("k").await.unwrap(), 10);
    m.assert_args(&["BITCOUNT", "k"]);

    let m = Mock::int(4);
    m.bitcount_range("k", 0, 10, None).await.unwrap();
    m.assert_args(&["BITCOUNT", "k", "0", "10"]);

    let m = Mock::int(4);
    m.bitcount_range("k", 0, 10, Some(BitmapIndexType::Byte))
        .await
        .unwrap();
    m.assert_args(&["BITCOUNT", "k", "0", "10", "BYTE"]);

    let m = Mock::int(4);
    m.bitcount_range("k", 0, 10, Some(BitmapIndexType::Bit))
        .await
        .unwrap();
    m.assert_args(&["BITCOUNT", "k", "0", "10", "BIT"]);
}

#[tokio::test]
async fn bitpos_and_range() {
    let m = Mock::int(3);
    assert_eq!(m.bitpos("k", 1).await.unwrap(), 3);
    m.assert_args(&["BITPOS", "k", "1"]);

    let m = Mock::int(3);
    m.bitpos_range("k", 1, 0, 10, Some(BitmapIndexType::Bit))
        .await
        .unwrap();
    m.assert_args(&["BITPOS", "k", "1", "0", "10", "BIT"]);
}

#[tokio::test]
async fn bitfield_get_set_incrby() {
    let m = Mock::array(vec![Value::Int(5)]);
    let r = m
        .bitfield(
            "k",
            &[BitFieldSubcommand::Get {
                encoding: BitEncoding::Unsigned(8),
                offset: BitFieldOffset::Bit(0),
            }],
        )
        .await
        .unwrap();
    m.assert_args(&["BITFIELD", "k", "GET", "u8", "0"]);
    assert_eq!(r, vec![Some(5)]);

    let m = Mock::array(vec![Value::Int(0), Value::Nil]);
    let r = m
        .bitfield(
            "k",
            &[
                BitFieldSubcommand::Set {
                    encoding: BitEncoding::Signed(5),
                    offset: BitFieldOffset::Multiplier(1),
                    value: 12,
                },
                BitFieldSubcommand::IncrBy {
                    encoding: BitEncoding::Unsigned(4),
                    offset: BitFieldOffset::Bit(2),
                    increment: 3,
                },
            ],
        )
        .await
        .unwrap();
    m.assert_args(&[
        "BITFIELD", "k", "SET", "i5", "#1", "12", "INCRBY", "u4", "2", "3",
    ]);
    assert_eq!(r, vec![Some(0), None]);
}

#[tokio::test]
async fn bitfield_readonly() {
    let m = Mock::array(vec![Value::Int(7)]);
    m.bitfield_readonly(
        "k",
        &[BitFieldSubcommand::Get {
            encoding: BitEncoding::Unsigned(8),
            offset: BitFieldOffset::Bit(0),
        }],
    )
    .await
    .unwrap();
    m.assert_args(&["BITFIELD_RO", "k", "GET", "u8", "0"]);
}

#[tokio::test]
async fn bitop_variants() {
    let m = Mock::int(8);
    m.bitop(BitwiseOperation::And, "dest", &["k1", "k2"])
        .await
        .unwrap();
    m.assert_args(&["BITOP", "AND", "dest", "k1", "k2"]);

    let m = Mock::int(8);
    m.bitop(BitwiseOperation::Not, "dest", &["k1"])
        .await
        .unwrap();
    m.assert_args(&["BITOP", "NOT", "dest", "k1"]);
}
