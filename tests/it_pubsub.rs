// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Pub/Sub integration tests (RESP2 + RESP3).
//!
//! Covers the publish side and `PUBSUB` introspection via `custom_command`, plus
//! the runtime subscribe/receive path: with the push channel enabled
//! (`enable_pubsub()` or connect-time `subscriptions`), `subscribe`/`psubscribe`
//! deliver messages through `get_pubsub_message`.

mod common;

use glide::CustomCommand;

resp_test!(publish_no_subscribers_returns_zero, c, {
    let chan = common::key("chan");
    let received = c
        .custom_command(&["PUBLISH", &chan, "hello"])
        .await
        .unwrap();
    assert_eq!(glide::value::to_i64(received).unwrap(), 0);
});

resp_test!(pubsub_channels_empty, c, {
    let reply = c.custom_command(&["PUBSUB", "CHANNELS"]).await.unwrap();
    // No active subscriptions on a fresh server.
    match reply {
        glide::Value::Array(items) => assert!(items.is_empty()),
        glide::Value::Nil => {}
        other => panic!("unexpected PUBSUB CHANNELS reply: {other:?}"),
    }
});

resp_test!(pubsub_numpat_zero, c, {
    let reply = c.custom_command(&["PUBSUB", "NUMPAT"]).await.unwrap();
    assert_eq!(glide::value::to_i64(reply).unwrap(), 0);
});

resp_test!(spublish_no_subscribers, c, {
    // Sharded publish (SPUBLISH) on a standalone server also returns 0.
    let chan = common::key("schan");
    match c.custom_command(&["SPUBLISH", &chan, "msg"]).await {
        Ok(v) => assert_eq!(glide::value::to_i64(v).unwrap(), 0),
        // Older servers may not support SPUBLISH in standalone mode.
        Err(glide::GlideError::Request(_)) => {}
        Err(other) => panic!("unexpected: {other:?}"),
    }
});

#[tokio::test]
async fn runtime_subscribe_receives_then_unsubscribe() {
    use glide::commands::pubsub::PubSubCommands;
    use glide::{GlideClient, GlideClientConfiguration};
    use std::time::Duration;

    let srv = match common::TestServer::start() {
        Some(s) => s,
        None => {
            eprintln!("SKIP: no valkey-server binary available");
            return;
        }
    };
    let chan = common::key("rt-chan");

    // A client with the push channel enabled but NO connect-time subscriptions.
    let subscriber = GlideClient::connect(
        GlideClientConfiguration::with_address("127.0.0.1", srv.port).enable_pubsub(),
    )
    .await
    .expect("connect subscriber");

    // Subscribe at runtime, then publish from a second client.
    subscriber.subscribe(&[chan.as_str()]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let publisher = srv.client().await;
    let n = publisher.publish(&chan, "runtime-hello").await.unwrap();
    assert!(n >= 1, "expected >=1 subscriber, got {n}");

    let msg = tokio::time::timeout(Duration::from_secs(3), subscriber.get_pubsub_message())
        .await
        .expect("timed out waiting for runtime-subscribed message")
        .expect("receive error");
    assert_eq!(msg.channel.as_ref(), chan.as_bytes());
    assert_eq!(msg.payload.as_ref(), b"runtime-hello");

    // Unsubscribe; subsequent publishes should not be received by us.
    subscriber.unsubscribe(&[chan.as_str()]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let n2 = publisher.publish(&chan, "after-unsub").await.unwrap();
    assert_eq!(n2, 0, "no subscribers should remain after unsubscribe");
}

#[tokio::test]
async fn runtime_psubscribe_pattern_receive() {
    use glide::commands::pubsub::PubSubCommands;
    use glide::{GlideClient, GlideClientConfiguration, PubSubMessageKind};
    use std::time::Duration;

    let srv = match common::TestServer::start() {
        Some(s) => s,
        None => {
            eprintln!("SKIP: no valkey-server binary available");
            return;
        }
    };
    let subscriber = GlideClient::connect(
        GlideClientConfiguration::with_address("127.0.0.1", srv.port).enable_pubsub(),
    )
    .await
    .expect("connect subscriber");

    subscriber.psubscribe(&["news.*"]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let publisher = srv.client().await;
    publisher.publish("news.tech", "breaking").await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(3), subscriber.get_pubsub_message())
        .await
        .expect("timed out")
        .expect("receive error");
    assert_eq!(msg.kind, PubSubMessageKind::PMessage);
    assert_eq!(msg.channel.as_ref(), b"news.tech");
    assert_eq!(msg.pattern.as_deref(), Some(&b"news.*"[..]));
}

#[tokio::test]
async fn runtime_unsubscribe_all_stops_delivery() {
    use glide::commands::pubsub::PubSubCommands;
    use glide::{GlideClient, GlideClientConfiguration};
    use std::time::Duration;

    let srv = match common::TestServer::start() {
        Some(s) => s,
        None => {
            eprintln!("SKIP: no valkey-server binary available");
            return;
        }
    };
    let c1 = common::key("uc1");
    let c2 = common::key("uc2");
    let subscriber = GlideClient::connect(
        GlideClientConfiguration::with_address("127.0.0.1", srv.port).enable_pubsub(),
    )
    .await
    .expect("connect subscriber");

    subscriber
        .subscribe(&[c1.as_str(), c2.as_str()])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let publisher = srv.client().await;
    assert!(publisher.publish(&c1, "x").await.unwrap() >= 1);

    // Unsubscribe from ALL exact channels (empty slice).
    subscriber.unsubscribe(&[] as &[&str]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(publisher.publish(&c1, "y").await.unwrap(), 0);
    assert_eq!(publisher.publish(&c2, "z").await.unwrap(), 0);
}
