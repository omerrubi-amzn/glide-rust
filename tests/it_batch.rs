// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Batch (pipeline + transaction) integration tests.
//!
//! Covers atomic transactions, non-atomic pipeline depth, `raise_on_error`
//! true/false behaviour, errors inside a transaction, and WATCH/MULTI framing
//! via `custom_command`.

mod common;

use glide::{Batch, CustomCommand, StringCommands};

#[tokio::test]
async fn atomic_transaction_ordered_results() {
    let srv = server_or_skip!();
    let c = srv.client().await;
    let k = common::key("tx");

    let mut batch = Batch::new(true);
    batch.set(&k, "10").incr(&k).incr(&k).get(&k);
    let results = c.exec(&batch, true).await.unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(glide::value::to_i64(results[1].clone()).unwrap(), 11);
    assert_eq!(glide::value::to_i64(results[2].clone()).unwrap(), 12);
    assert_eq!(glide::value::to_string(results[3].clone()).unwrap(), "12");
}

#[tokio::test]
async fn empty_batch_returns_empty() {
    let srv = server_or_skip!();
    let c = srv.client().await;
    let batch = Batch::new(true);
    let results = c.exec(&batch, true).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn non_atomic_pipeline_depth() {
    let srv = server_or_skip!();
    let c = srv.client().await;
    let k = common::key("pipe");

    let mut batch = Batch::new(false);
    batch.set(&k, "0");
    for _ in 0..100 {
        batch.incr(&k);
    }
    batch.get(&k);
    let results = c.exec(&batch, true).await.unwrap();
    // 1 SET + 100 INCR + 1 GET.
    assert_eq!(results.len(), 102);
    assert_eq!(
        glide::value::to_string(results[101].clone()).unwrap(),
        "100"
    );
}

#[tokio::test]
async fn pipeline_is_atomic_flag() {
    let atomic = Batch::new(true);
    assert!(atomic.is_atomic());
    let pipeline = Batch::new(false);
    assert!(!pipeline.is_atomic());
}

#[tokio::test]
async fn raise_on_error_true_surfaces_error() {
    let srv = server_or_skip!();
    let c = srv.client().await;
    let k = common::key("re");
    c.set(&k, "notanumber").await.unwrap();

    let mut batch = Batch::new(false);
    batch.incr(&k); // errors: value is not an integer
    let result = c.exec(&batch, true).await;
    assert!(result.is_err(), "expected error with raise_on_error=true");
}

#[tokio::test]
async fn raise_on_error_false_returns_inline() {
    let srv = server_or_skip!();
    let c = srv.client().await;
    let good = common::key("good");
    let bad = common::key("bad");
    c.set(&bad, "notanumber").await.unwrap();

    let mut batch = Batch::new(false);
    batch.set(&good, "1").incr(&good).incr(&bad); // last one errors
    let results = c.exec(&batch, false).await.unwrap();
    // All three positions are present even though one errored.
    assert_eq!(results.len(), 3);
    // The good INCR still produced 2.
    assert_eq!(glide::value::to_i64(results[1].clone()).unwrap(), 2);
}

#[tokio::test]
async fn error_inside_transaction_runtime() {
    let srv = server_or_skip!();
    let c = srv.client().await;
    let k = common::key("tx");
    c.set(&k, "notanumber").await.unwrap();

    // A runtime error inside MULTI/EXEC does not abort the whole transaction;
    // with raise_on_error=false the other results are returned.
    let mut batch = Batch::new(true);
    batch.command(&["SET", &common::key("ok"), "v"]).incr(&k);
    let results = c.exec(&batch, false).await.unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn custom_command_in_batch() {
    let srv = server_or_skip!();
    let c = srv.client().await;
    let k = common::key("cc");
    let mut batch = Batch::new(false);
    batch
        .command(&["SET", &k, "1"])
        .command(&["APPEND", &k, "0"])
        .command(&["GET", &k]);
    let results = c.exec(&batch, true).await.unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(glide::value::to_string(results[2].clone()).unwrap(), "10");
}

#[tokio::test]
async fn watch_multi_semantics() {
    let srv = server_or_skip!();
    let c = srv.client().await;
    let k = common::key("w");
    c.set(&k, "1").await.unwrap();

    // WATCH/UNWATCH are accepted (framing check). GLIDE multiplexes connections,
    // so we assert the commands succeed rather than optimistic-lock abort.
    let watch = c.custom_command(&["WATCH", &k]).await.unwrap();
    assert_eq!(glide::value::to_string(watch).unwrap(), "OK");

    let mut batch = Batch::new(true);
    batch.incr(&k);
    let results = c.exec(&batch, true).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(glide::value::to_i64(results[0].clone()).unwrap(), 2);

    let unwatch = c.custom_command(&["UNWATCH"]).await.unwrap();
    assert_eq!(glide::value::to_string(unwatch).unwrap(), "OK");
}

#[tokio::test]
async fn batch_spans_multiple_data_types() {
    let srv = server_or_skip!();
    let c = srv.client().await;
    let s = common::key("b_str");
    let l = common::key("b_list");
    let h = common::key("b_hash");
    let z = common::key("b_zset");

    let mut batch = Batch::new(false);
    batch
        .command(&["SET", &s, "v"])
        .command(&["RPUSH", &l, "a", "b", "c"])
        .command(&["HSET", &h, "f", "1"])
        .command(&["ZADD", &z, "1", "m"])
        .command(&["LLEN", &l])
        .command(&["HGET", &h, "f"])
        .command(&["ZCARD", &z]);
    let r = c.exec(&batch, true).await.unwrap();
    assert_eq!(r.len(), 7);
    assert_eq!(glide::value::to_i64(r[4].clone()).unwrap(), 3); // LLEN
    assert_eq!(glide::value::to_string(r[5].clone()).unwrap(), "1"); // HGET
    assert_eq!(glide::value::to_i64(r[6].clone()).unwrap(), 1); // ZCARD
}

#[tokio::test]
async fn batch_preserves_binary_values() {
    let srv = server_or_skip!();
    let c = srv.client().await;
    let k = common::key("b_bin");
    let payload = vec![0u8, 1, 2, 255, 0, 42];
    let mut batch = Batch::new(true);
    batch.set(&k, payload.clone()).get(&k);
    let r = c.exec(&batch, true).await.unwrap();
    assert_eq!(
        glide::value::to_bytes(r[1].clone()).unwrap().as_ref(),
        &payload[..]
    );
}

#[tokio::test]
async fn non_atomic_mixed_reads_writes_ordered() {
    let srv = server_or_skip!();
    let c = srv.client().await;
    let k = common::key("b_mix");
    let mut batch = Batch::new(false);
    batch
        .set(&k, "1")
        .get(&k)
        .incr(&k)
        .get(&k)
        .del(&[&k])
        .get(&k);
    let r = c.exec(&batch, true).await.unwrap();
    assert_eq!(r.len(), 6);
    assert_eq!(glide::value::to_string(r[1].clone()).unwrap(), "1");
    assert_eq!(glide::value::to_i64(r[2].clone()).unwrap(), 2);
    assert_eq!(glide::value::to_string(r[3].clone()).unwrap(), "2");
    // After DEL, GET is null.
    assert!(matches!(r[5], glide::Value::Nil));
}

#[tokio::test]
async fn cluster_atomic_transaction_same_slot() {
    let h = match common::shared_cluster() {
        Some(h) => h,
        None => {
            eprintln!("SKIP: cluster harness not feasible");
            return;
        }
    };
    let c = match h.client().await {
        Some(c) => c,
        None => {
            eprintln!("SKIP: cluster connect failed");
            return;
        }
    };
    // All keys share a hash tag → same slot → a cluster MULTI/EXEC is valid.
    let k1 = common::tkey("btx", "k1");
    let k2 = common::tkey("btx", "k2");
    let mut batch = Batch::new(true);
    batch
        .set(&k1, "10")
        .incr(&k1)
        .set(&k2, "x")
        .get(&k1)
        .get(&k2);
    let r = c.exec(&batch, true, None).await.unwrap();
    assert_eq!(r.len(), 5);
    assert_eq!(glide::value::to_i64(r[1].clone()).unwrap(), 11);
    assert_eq!(glide::value::to_string(r[3].clone()).unwrap(), "11");
    assert_eq!(glide::value::to_string(r[4].clone()).unwrap(), "x");
}

#[tokio::test]
async fn cluster_non_atomic_pipeline() {
    let h = match common::shared_cluster() {
        Some(h) => h,
        None => {
            eprintln!("SKIP: cluster harness not feasible");
            return;
        }
    };
    let c = match h.client().await {
        Some(c) => c,
        None => {
            eprintln!("SKIP: cluster connect failed");
            return;
        }
    };
    // A non-atomic pipeline may span slots; GLIDE routes each command.
    let mut batch = Batch::new(false);
    let a = common::key("bp_a");
    let b = common::key("bp_b");
    batch.set(&a, "1").set(&b, "2").get(&a).get(&b);
    let r = c.exec(&batch, true, None).await.unwrap();
    assert_eq!(r.len(), 4);
    assert_eq!(glide::value::to_string(r[2].clone()).unwrap(), "1");
    assert_eq!(glide::value::to_string(r[3].clone()).unwrap(), "2");
}
