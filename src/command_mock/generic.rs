// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Mock-executor unit tests for the generic (key) command family.
use super::Mock;
use crate::commands::generic::GenericCommands;
use crate::commands::options::{
    ExpireOptions, Limit, MigrateOptions, ObjectType, OrderBy, RestoreOptions,
};
use bytes::Bytes;
use redis::Value;

#[tokio::test]
async fn del_unlink_exists_touch() {
    let m = Mock::int(2);
    assert_eq!(m.del(&["k1", "k2"]).await.unwrap(), 2);
    m.assert_args(&["DEL", "k1", "k2"]);

    let m = Mock::int(2);
    m.unlink(&["k1", "k2"]).await.unwrap();
    m.assert_args(&["UNLINK", "k1", "k2"]);

    let m = Mock::int(1);
    m.exists(&["k1"]).await.unwrap();
    m.assert_args(&["EXISTS", "k1"]);

    let m = Mock::int(1);
    m.touch(&["k1", "k2"]).await.unwrap();
    m.assert_args(&["TOUCH", "k1", "k2"]);
}

#[tokio::test]
async fn expire_family() {
    let m = Mock::int(1);
    assert!(m.expire("k", 60).await.unwrap());
    m.assert_args(&["EXPIRE", "k", "60"]);

    let m = Mock::int(1);
    m.expire_opts("k", 60, ExpireOptions::NewExpiryGreaterThanCurrent)
        .await
        .unwrap();
    m.assert_args(&["EXPIRE", "k", "60", "GT"]);

    let m = Mock::int(1);
    m.pexpire("k", 6000).await.unwrap();
    m.assert_args(&["PEXPIRE", "k", "6000"]);

    let m = Mock::int(1);
    m.expireat("k", 1700000000).await.unwrap();
    m.assert_args(&["EXPIREAT", "k", "1700000000"]);

    let m = Mock::int(1);
    m.pexpireat("k", 1700000000000).await.unwrap();
    m.assert_args(&["PEXPIREAT", "k", "1700000000000"]);

    let m = Mock::int(1);
    assert!(m.persist("k").await.unwrap());
    m.assert_args(&["PERSIST", "k"]);
}

#[tokio::test]
async fn ttl_family() {
    let m = Mock::int(55);
    assert_eq!(m.ttl("k").await.unwrap(), 55);
    m.assert_args(&["TTL", "k"]);

    let m = Mock::int(55000);
    m.pttl("k").await.unwrap();
    m.assert_args(&["PTTL", "k"]);

    let m = Mock::int(1700000000);
    m.expiretime("k").await.unwrap();
    m.assert_args(&["EXPIRETIME", "k"]);

    let m = Mock::int(1700000000000);
    m.pexpiretime("k").await.unwrap();
    m.assert_args(&["PEXPIRETIME", "k"]);
}

#[tokio::test]
async fn type_rename_randomkey_dump() {
    let m = Mock::simple("string");
    assert_eq!(m.key_type("k").await.unwrap(), "string");
    m.assert_args(&["TYPE", "k"]);

    let m = Mock::ok();
    m.rename("k", "k2").await.unwrap();
    m.assert_args(&["RENAME", "k", "k2"]);

    let m = Mock::int(1);
    assert!(m.renamenx("k", "k2").await.unwrap());
    m.assert_args(&["RENAMENX", "k", "k2"]);

    let m = Mock::bulk("somekey");
    assert_eq!(
        m.randomkey().await.unwrap(),
        Some(Bytes::from_static(b"somekey"))
    );
    m.assert_args(&["RANDOMKEY"]);

    let m = Mock::bulk("\x00serialized");
    assert!(m.dump("k").await.unwrap().is_some());
    m.assert_args(&["DUMP", "k"]);
}

#[tokio::test]
async fn copy_variants() {
    let m = Mock::int(1);
    assert!(m.copy("src", "dst", false).await.unwrap());
    m.assert_args(&["COPY", "src", "dst"]);

    let m = Mock::int(1);
    m.copy("src", "dst", true).await.unwrap();
    m.assert_args(&["COPY", "src", "dst", "REPLACE"]);

    let m = Mock::int(1);
    m.copy_with_options("src", "dst", Some(2), true)
        .await
        .unwrap();
    m.assert_args(&["COPY", "src", "dst", "DB", "2", "REPLACE"]);
}

#[tokio::test]
async fn object_subcommands() {
    let m = Mock::int(1);
    assert_eq!(m.object_refcount("k").await.unwrap(), Some(1));
    m.assert_args(&["OBJECT", "REFCOUNT", "k"]);

    let m = Mock::bulk("listpack");
    assert_eq!(
        m.object_encoding("k").await.unwrap(),
        Some(Bytes::from_static(b"listpack"))
    );
    m.assert_args(&["OBJECT", "ENCODING", "k"]);

    let m = Mock::int(10);
    m.object_idletime("k").await.unwrap();
    m.assert_args(&["OBJECT", "IDLETIME", "k"]);

    let m = Mock::int(3);
    m.object_freq("k").await.unwrap();
    m.assert_args(&["OBJECT", "FREQ", "k"]);

    let m = Mock::nil();
    assert_eq!(m.object_refcount("missing").await.unwrap(), None);
}

#[tokio::test]
async fn scan_encoding() {
    let m = Mock::array(vec![
        Value::BulkString(b"17".to_vec()),
        Value::Array(vec![Value::BulkString(b"k1".to_vec())]),
    ]);
    let (cursor, keys) = m
        .scan("0", Some(b"k*"), Some(100), Some(ObjectType::String))
        .await
        .unwrap();
    m.assert_args(&["SCAN", "0", "MATCH", "k*", "COUNT", "100", "TYPE", "string"]);
    assert_eq!(cursor, "17");
    assert_eq!(keys, vec![Bytes::from_static(b"k1")]);
}

#[tokio::test]
async fn sort_and_sort_store_and_ro() {
    let m = Mock::array(vec![Value::BulkString(b"1".to_vec())]);
    m.sort(
        "k",
        Some(OrderBy::Desc),
        Some(Limit {
            offset: 0,
            count: 10,
        }),
        true,
    )
    .await
    .unwrap();
    m.assert_args(&["SORT", "k", "LIMIT", "0", "10", "DESC", "ALPHA"]);

    let m = Mock::int(3);
    m.sort_store("k", "dst", None, None, false).await.unwrap();
    m.assert_args(&["SORT", "k", "STORE", "dst"]);

    let m = Mock::array(vec![Value::BulkString(b"1".to_vec())]);
    m.sort_ro("k", Some(OrderBy::Asc), None, false)
        .await
        .unwrap();
    m.assert_args(&["SORT_RO", "k", "ASC"]);
}

#[tokio::test]
async fn restore_encoding() {
    let m = Mock::ok();
    let opts = RestoreOptions {
        replace: true,
        absttl: false,
        idletime: Some(5),
        frequency: None,
    };
    m.restore("k", 0, "payload", opts).await.unwrap();
    m.assert_args(&["RESTORE", "k", "0", "payload", "REPLACE", "IDLETIME", "5"]);
}

#[tokio::test]
async fn wait_move_encoding() {
    let m = Mock::int(2);
    assert_eq!(m.wait(2, 100).await.unwrap(), 2);
    m.assert_args(&["WAIT", "2", "100"]);

    let m = Mock::int(1);
    assert!(m.move_key("k", 1).await.unwrap());
    m.assert_args(&["MOVE", "k", "1"]);
}

#[tokio::test]
async fn migrate_variants() {
    let m = Mock::ok();
    m.migrate("host", 6379, "k", 0, 500, MigrateOptions::default())
        .await
        .unwrap();
    m.assert_args(&["MIGRATE", "host", "6379", "k", "0", "500"]);

    let m = Mock::ok();
    let opts = MigrateOptions {
        copy: true,
        replace: true,
        ..Default::default()
    };
    m.migrate_keys("host", 6379, &["k1", "k2"], 0, 500, opts)
        .await
        .unwrap();
    m.assert_args(&[
        "MIGRATE", "host", "6379", "", "0", "500", "COPY", "REPLACE", "KEYS", "k1", "k2",
    ]);
}

#[tokio::test]
async fn watch_unwatch() {
    let m = Mock::ok();
    m.watch(&["k1", "k2"]).await.unwrap();
    m.assert_args(&["WATCH", "k1", "k2"]);

    let m = Mock::ok();
    m.unwatch().await.unwrap();
    m.assert_args(&["UNWATCH"]);
}
