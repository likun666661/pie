# notification-e2e (phase 1 — protocol/demo smoke)

A standalone runnable demo of the **wire shape** of external notifications that will feed
pie's upcoming trigger runtime (RFC 1 / issue #20) and Cloudflare hub (RFC 0 / issue #21).

> **This is NOT a full runtime e2e.** It exercises the wire/transport halves only, prints a
> normalized `DemoTrigger` envelope to stdout, and demonstrates deduplication + redaction.
> The actual agent entrypoint (`AgentHarness::handle_trigger`), session `Custom` audit, and
> `/triggers` view all land in **phase 2** after issue #20 ships.

## Run

```sh
cargo run --bin mcp-push-smoke -p notification-e2e
cargo run --bin hub-ws-smoke   -p notification-e2e
```

Each binary prints one or more `DemoTrigger` envelopes and a summary, then exits 0.

## What phase 1 proves

- An MCP server can emit **server-push notifications** (no `id` field) on the same transport
  that handles `tools/list` / `tools/call`. A consumer can route those into a normalized
  `Trigger` envelope independent of the request/response path.
- A WebSocket hub server can emit `trigger` frames matching RFC 0 §3.2.2 over an outbound
  client connection, with the 5-stage ack lifecycle `received → accepted | rejected | failed → completed`.
- Both source variants converge on **one** normalized envelope shape:
  - `source_kind`, `source_label`, `event_label`
  - `idempotency_key`, `trace_id`
  - `authority { principal_id, principal_label, credential_scope }` (split per
    Provider/Auth-Lead's RFC 0 §4.4 / §3.2.2 requirement)
  - `payload_visibility` + `payload_summary`
- Duplicate `idempotency_key` is **visibly** deduped in both variants.
- Fake hub token `fake-hub-token-should-not-leak` never appears in any observable output
  (stdout, stderr, serialized envelope).

## What phase 1 deliberately does NOT prove

- No real `pie_agent_core::Trigger` type is used — `DemoTrigger` is a phase-1-only struct in
  `src/envelope.rs`. **Do not import `DemoTrigger` from production code.**
- No `AgentHarness::handle_trigger` API is called — that API does not exist yet. RFC 1
  (issue #20) introduces it.
- No session `Custom { custom_type: "trigger" }` audit is written. Phase 2 adds this.
- No `/triggers` slash command rendering. Phase 2 adds this.
- No permission evaluator (`Allow | Deny | Prompt`) integration. Phase 2 adds this.
- No real `WebSocketHubHook` implementation (lands with RFC 0 / issue #21).
- The real `McpClient::take_notifications()` outlet from RFC 1 §4.2.1 is **not** used. This
  demo bypasses `pie_mcp::McpClient` and reads notifications directly off a local transport
  to keep phase 1 purely additive (no production code change in `crates/mcp`).
- No real Cloudflare deployment.

## Layout

```
examples/notification-e2e/
  Cargo.toml
  README.md
  src/
    lib.rs           # crate doc + module exports
    envelope.rs      # DemoTrigger, DedupSink, SourceKind, PayloadVisibility, Authority
    redaction.rs     # FAKE_HUB_TOKEN constant + assert_no_token_leak helper
    bin/
      mcp_push_smoke.rs    # MCP source variant
      hub_ws_smoke.rs      # mock WebSocket hub source variant
  tests/
    smoke.rs         # captures binary output and asserts dedup + redaction + envelope shape
```

## Acceptance checklist (matches issue #22)

- [x] Two sub-examples (`mcp-push-smoke`, `hub-ws-smoke`) build and run.
- [x] Both print a normalized `DemoTrigger` envelope with all required fields.
- [x] Duplicate `idempotency_key` is visibly deduped in both sub-examples.
- [x] Fake token `fake-hub-token-should-not-leak` is asserted absent from observable output.
- [x] No real network calls (in-process mock MCP + `127.0.0.1` WebSocket).
- [x] README marks this as phase-1 protocol/demo smoke, not full runtime e2e.
- [x] All content is in English.

## Phase 2 (future, not in this crate)

When RFC 1 (#20) lands, the demo will be ported to:

- Use the real `pie_agent_core::Trigger` envelope.
- Implement a real `NotificationHook` (one for MCP push, one for WebSocket hub).
- Call `AgentHarness::handle_trigger(...)`.
- Persist a `SessionTreeEntry::Custom { custom_type: "trigger", data: TriggerRecord }`.
- Surface the result in `/triggers --source hub|mcp`.

That work is tracked separately and will not modify this crate (we will likely retire this
demo crate or move its sub-examples under `crates/coding-agent/examples/` once production
plumbing exists).
