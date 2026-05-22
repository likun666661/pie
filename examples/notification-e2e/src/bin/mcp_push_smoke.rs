//! Phase-1 protocol/demo smoke for the MCP server-push notification source.
//!
//! What this proves:
//! - An MCP server can send server-push notification frames (no `id` field, plain JSON-RPC
//!   notification shape) over the same transport that handles `tools/list` / `tools/call`.
//! - A consumer can route those notification frames into a `Trigger`-shaped envelope
//!   independent of the request/response routing.
//! - Per-method idempotency-key derivation collapses duplicates.
//!
//! What this does NOT prove (phase 2):
//! - `AgentHarness::handle_trigger` is not exercised (it does not exist yet — lands in #20).
//! - No session `Custom` audit is written.
//! - No `/triggers` rendering.
//! - The real `McpClient::take_notifications()` outlet from RFC 1 §4.2.1 is not used; this
//!   example deliberately bypasses `pie_mcp::McpClient` and reads notifications straight off
//!   a local transport so we do not touch production code in phase 1.
//!
//! Run: `cargo run --bin mcp-push-smoke -p notification-e2e`

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use notification_e2e::envelope::{
    Authority, CredentialScope, DedupSink, DemoTrigger, PayloadVisibility, SourceKind,
};
use notification_e2e::redaction::{FAKE_HUB_TOKEN, assert_no_token_leak};
use pie_mcp::errors::McpError;
use pie_mcp::transport::Transport;
use tokio::sync::{Mutex as AsyncMutex, mpsc};

/// In-process pair of transports that exchange newline-delimited JSON. We use this in place
/// of `pie_mcp::StdioTransport` so the demo does not spawn a subprocess.
struct PipeTransport {
    tx: AsyncMutex<mpsc::UnboundedSender<String>>,
    rx: AsyncMutex<mpsc::UnboundedReceiver<String>>,
}

#[async_trait]
impl Transport for PipeTransport {
    async fn send_line(&self, line: String) -> Result<(), McpError> {
        self.tx
            .lock()
            .await
            .send(line)
            .map_err(|e| McpError::Transport(e.to_string()))
    }

    async fn recv_line(&self) -> Result<Option<String>, McpError> {
        Ok(self.rx.lock().await.recv().await)
    }

    async fn close(&self) {}
}

fn pipe_pair() -> (Arc<PipeTransport>, Arc<PipeTransport>) {
    let (a_tx, b_rx) = mpsc::unbounded_channel();
    let (b_tx, a_rx) = mpsc::unbounded_channel();
    let a = PipeTransport {
        tx: AsyncMutex::new(a_tx),
        rx: AsyncMutex::new(a_rx),
    };
    let b = PipeTransport {
        tx: AsyncMutex::new(b_tx),
        rx: AsyncMutex::new(b_rx),
    };
    (Arc::new(a), Arc::new(b))
}

/// A canned MCP server that sits on one end of a `PipeTransport` and pushes a fixed set of
/// notifications. Real MCP servers would interleave responses to requests too; this demo
/// only needs the push half.
async fn mock_mcp_server(transport: Arc<PipeTransport>) {
    // Three push frames in order:
    //   1. `notifications/tools/listChanged` — idempotency = "mcp:demo:tools/listChanged"
    //   2. `notifications/resources/updated` for uri=demo://doc/1
    //   3. Duplicate of (1) to demonstrate dedup
    let frames = [
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/tools/listChanged",
            "params": {}
        }),
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/resources/updated",
            "params": { "uri": "demo://doc/1" }
        }),
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/tools/listChanged",
            "params": {}
        }),
    ];
    for f in frames {
        let _ = transport.send_line(f.to_string()).await;
    }
    // Leave the channel open by parking; the consumer will stop after reading 3.
}

/// Consumer-side read loop. Reads notifications from the transport and emits `DemoTrigger`s
/// via the dedup sink. Stops after seeing `expected` accepted-or-deduped events.
async fn consume_notifications(
    transport: Arc<PipeTransport>,
    sink: &mut DedupSink,
    expected: usize,
) {
    let mut seen = 0usize;
    while seen < expected {
        let line = match transport.recv_line().await {
            Ok(Some(line)) => line,
            _ => break,
        };
        let v: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        // Notifications have no `id`. Responses do — they would be routed elsewhere. This is
        // exactly the split that production `McpClient` will perform once #20 ships, but in
        // phase 1 we keep the demo self-contained.
        if v.get("id").is_some() {
            continue;
        }
        let method = match v.get("method").and_then(|m| m.as_str()) {
            Some(m) => m,
            None => continue,
        };
        let params = v.get("params").cloned().unwrap_or(serde_json::Value::Null);
        let trigger = map_method_to_demo_trigger("demo", method, &params);
        seen += 1;
        match sink.submit(trigger.clone()) {
            Ok(()) => println!("{}", trigger.render()),
            Err(prev_trace) => {
                println!(
                    "[deduped] idempotency_key={} (previous trace_id={})\n",
                    trigger.idempotency_key, prev_trace
                );
            }
        }
    }
}

/// Derive a `DemoTrigger` from an MCP notification method + params, using the per-method
/// idempotency rules sketched in RFC 1 §4.2.3.
fn map_method_to_demo_trigger(
    server_name: &str,
    method: &str,
    params: &serde_json::Value,
) -> DemoTrigger {
    let idempotency_key = match method {
        "notifications/tools/listChanged" => format!("mcp:{}:tools/listChanged", server_name),
        "notifications/resources/listChanged" => {
            format!("mcp:{}:resources/listChanged", server_name)
        }
        "notifications/resources/updated" => {
            let uri = params
                .get("uri")
                .and_then(|u| u.as_str())
                .unwrap_or("unknown");
            format!("mcp:{}:resources/updated:{}", server_name, uri)
        }
        custom => {
            // Custom notifications must declare their own dedup key in payload (under
            // `_meta.pie_dedup_key` first, then `_pie_dedup_key`). If neither is present, the
            // notification would be dropped in production — but for this demo we synthesize a
            // deterministic key so the user sees what would happen.
            let key = params
                .get("_meta")
                .and_then(|m| m.get("pie_dedup_key"))
                .and_then(|k| k.as_str())
                .or_else(|| params.get("_pie_dedup_key").and_then(|k| k.as_str()));
            match key {
                Some(k) => format!("mcp:{}:{}:{}", server_name, custom, k),
                None => format!("mcp:{}:{}:DROP-IN-PROD", server_name, custom),
            }
        }
    };

    DemoTrigger {
        source_kind: SourceKind::Mcp,
        source_label: format!("MCP {}", server_name),
        event_label: method.to_string(),
        idempotency_key,
        trace_id: format!("trace-mcp-{}", uuid::Uuid::new_v4()),
        authority: Authority {
            principal_id: "01H8E0DEMOPRINCIPAL0000000".into(),
            principal_label: "demo-user".into(),
            credential_scope: CredentialScope::User,
        },
        payload_visibility: PayloadVisibility::Local,
        payload_summary: Some(format!("method={}", method)),
        received_at: Utc::now(),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("== MCP push smoke (phase 1 protocol/demo smoke) ==");
    println!("Note: this example bypasses pie_mcp::McpClient and reads notifications straight off");
    println!("a local transport. Production behavior change (the `take_notifications()` outlet)");
    println!("lands in RFC 1 / issue #20.");
    println!();

    // The fake token is constructed but never sent on the MCP wire — MCP credentials are
    // local subprocess-level concerns and would not appear in an MCP notification frame. The
    // assert below is here to enforce the redaction policy uniformly across both binaries.
    let _local_fake_token = FAKE_HUB_TOKEN;

    let (client_side, server_side) = pipe_pair();
    let server_handle = tokio::spawn(mock_mcp_server(server_side));

    let mut sink = DedupSink::new();
    consume_notifications(client_side, &mut sink, 3).await;

    println!("== summary ==");
    println!("  accepted: {}", sink.accepted().len());
    println!("  deduped:  {}", sink.deduped().len());

    server_handle.abort();

    // Phase-1 acceptance assertions.
    assert_eq!(
        sink.accepted().len(),
        2,
        "expected 2 unique triggers (listChanged + resources/updated)"
    );
    assert_eq!(
        sink.deduped().len(),
        1,
        "expected 1 deduped (second listChanged)"
    );

    // Render everything we saw and assert no token leak.
    let mut captured = String::new();
    for t in sink.accepted() {
        captured.push_str(&t.render());
    }
    captured.push_str(&serde_json::to_string(sink.accepted())?);
    assert_no_token_leak(&captured, "mcp-push-smoke output");

    println!("\nOK: mcp-push-smoke phase-1 acceptance passed.");
    Ok(())
}
