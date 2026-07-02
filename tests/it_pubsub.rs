// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Pub/Sub integration tests (RESP2 + RESP3).
//!
//! The wrapper does not yet expose a typed subscription API (glide-core is
//! connected with `push_sender = None`, so pushed messages are not delivered to
//! the client). We therefore exercise the publish side and PUBSUB introspection
//! via `custom_command`, which is fully functional, and document the receive
//! path as a graceful SKIP.

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
async fn subscribe_receive_path_skipped() {
    // The typed subscribe/receive path requires wiring glide-core's push_sender,
    // which the wrapper does not expose yet. Documented as a SKIP so the intent
    // is tracked without a spurious failure.
    eprintln!("SKIP: typed pub/sub receive path not yet exposed by the wrapper");
}
