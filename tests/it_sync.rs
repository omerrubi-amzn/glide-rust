// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Integration tests for the blocking (`sync`) clients.
//!
//! These are plain `#[test]`s (NOT `#[tokio::test]`): the sync client drives the
//! shared process-wide runtime via `block_on`, which must not run inside another
//! Tokio runtime. Restores and extends the sync coverage that was lost when the
//! monolithic `integration.rs` was split.

mod common;

use glide::sync::{SyncGlideClient, SyncGlideClusterClient};
use glide::{Batch, GlideClientConfiguration, GlideClusterClientConfiguration, Route, SetOptions};
use glide::{ConditionalChange, ExpirySet};
// Bring async command traits into scope for the `run` combinator closures.
use glide::*;

fn sync_client(port: u16) -> SyncGlideClient {
    SyncGlideClient::connect(GlideClientConfiguration::with_address("127.0.0.1", port))
        .expect("connect sync client")
}

#[test]
fn sync_standalone_common_commands() {
    let srv = server_or_skip!();
    let c = sync_client(srv.port);
    let k = common::key("sync:str");

    c.set(&k, "hello").unwrap();
    assert_eq!(c.get(&k).unwrap().as_deref(), Some(&b"hello"[..]));
    assert_eq!(c.exists(&[&k]).unwrap(), 1);
    assert_eq!(c.ping().unwrap(), "PONG");

    let ctr = common::key("sync:ctr");
    assert_eq!(c.incr(&ctr).unwrap(), 1);
    assert_eq!(c.incr(&ctr).unwrap(), 2);

    assert!(c.expire(&k, 100).unwrap());
    assert!(c.ttl(&k).unwrap() > 0);
    assert_eq!(c.del(&[&k]).unwrap(), 1);
    assert_eq!(c.get(&k).unwrap(), None);
}

#[test]
fn sync_standalone_set_options() {
    let srv = server_or_skip!();
    let c = sync_client(srv.port);
    let k = common::key("sync:opt");

    c.set(&k, "first").unwrap();
    // NX must not overwrite an existing key.
    let opts = SetOptions {
        conditional_set: Some(ConditionalChange::OnlyIfDoesNotExist),
        return_old_value: false,
        expiry: Some(ExpirySet::Seconds(50)),
    };
    c.set_options(&k, "second", opts).unwrap();
    assert_eq!(c.get(&k).unwrap().as_deref(), Some(&b"first"[..]));
}

#[test]
fn sync_standalone_custom_command_and_batch() {
    let srv = server_or_skip!();
    let c = sync_client(srv.port);
    let k = common::key("sync:cc");

    c.custom_command(&["SET", &k, "42"]).unwrap();
    let v = c.custom_command(&["GET", &k]).unwrap();
    assert_eq!(glide::value::to_string(v).unwrap(), "42");

    // Atomic transaction via the blocking exec.
    let bk = common::key("sync:batch");
    let mut batch = Batch::new(true);
    batch.set(&bk, "10").incr(&bk).incr(&bk).get(&bk);
    let results = c.exec(&batch, true).unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(glide::value::to_i64(results[2].clone()).unwrap(), 12);
    assert_eq!(glide::value::to_string(results[3].clone()).unwrap(), "12");
}

#[test]
fn sync_standalone_run_full_async_surface() {
    let srv = server_or_skip!();
    let c = sync_client(srv.port);
    let h = common::key("sync:hash");
    let l = common::key("sync:list");
    let z = common::key("sync:zset");
    let s = common::key("sync:set");

    // The `run` combinator unlocks the entire async command surface from sync code.
    let (hlen, llen, zscore, scard) = c.run(|client| {
        let (h, l, z, s) = (h.clone(), l.clone(), z.clone(), s.clone());
        async move {
            client
                .hset(&h, &[("f1", "v1"), ("f2", "v2")])
                .await
                .unwrap();
            client.rpush(&l, &["a", "b", "c"]).await.unwrap();
            client.zadd(&z, &[("m1", 1.0), ("m2", 2.0)]).await.unwrap();
            client.sadd(&s, &["x", "y", "z"]).await.unwrap();
            (
                client.hlen(&h).await.unwrap(),
                client.llen(&l).await.unwrap(),
                client.zscore(&z, "m2").await.unwrap(),
                client.scard(&s).await.unwrap(),
            )
        }
    });
    assert_eq!(hlen, 2);
    assert_eq!(llen, 3);
    assert_eq!(zscore, Some(2.0));
    assert_eq!(scard, 3);
}

#[test]
fn sync_cluster_commands() {
    let cluster = cluster_or_skip!();
    let client = SyncGlideClusterClient::connect(
        GlideClusterClientConfiguration::with_address("127.0.0.1", cluster.seed_port())
            .request_timeout(std::time::Duration::from_secs(5)),
    );
    let client = match client {
        Ok(c) => c,
        Err(_) => {
            eprintln!("SKIP: could not connect sync cluster client");
            return;
        }
    };

    assert_eq!(client.ping().unwrap(), "PONG");

    let k = common::key("sync:cluster:k");
    client.custom_command(&["SET", &k, "v"]).unwrap();
    let v = client.custom_command(&["GET", &k]).unwrap();
    assert_eq!(glide::value::to_string(v).unwrap(), "v");

    // Routed command to all primaries.
    client
        .custom_command_with_route(&["PING"], Route::AllPrimaries)
        .unwrap();

    // The run combinator against the cluster client.
    let got = client.run(|c| {
        let k = k.clone();
        async move { c.custom_command(&["GET", &k]).await.unwrap() }
    });
    assert_eq!(glide::value::to_string(got).unwrap(), "v");
}
