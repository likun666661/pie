# Session-Scoped Public Webhook Endpoint — Design

Date: 2026-06-07
Status: approved (design), pending implementation plan

## Problem

A pie session should be registrable as an HTTP endpoint on the public internet, so
external callers (webhook senders, scripts, other services) can push messages into
that specific session. Interaction is one-way notification injection: the caller
POSTs a payload, gets an immediate 202, and the session's agent processes the
message asynchronously. The caller never receives the agent's reply.

## Decisions (settled during brainstorming)

| Decision | Choice |
| --- | --- |
| Interaction semantics | One-way notification injection (no request/response) |
| Public reachability | Relay through the pie hub (`pie.0xfefe.me`, code in `workers/fefe-hub`) |
| Caller authentication | Unguessable URL (capability token in path), no header auth |
| Offline behavior | Hub backlogs messages; replayed when the owning session reconnects |
| Endpoint granularity | Per-session (approach B); binding survives `--resume` |
| Default delivery mode | `run` (`TriggerDelivery::InjectAndRun`), overridable per endpoint |

Rejected alternatives: per-agent endpoint injected into whatever session is active
(approach A — wrong semantics, messages land in the wrong session after resume);
local-pending-file replay in addition to hub backlog (approach C — duplicates queue
state, YAGNI); direct local bind + external tunnel (user prefers hub relay, zero
local port exposure, TLS handled by the hub).

## Architecture

```
external caller            hub (workers/fefe-hub)              local pie
    |                          |                                |
    | POST /e/<token>          |                                |
    +------------------------->| token hash -> endpoints table  |
    |                          | insert notifications (pending) |
    | <----- 202 {ok} ---------+ AgentMailbox SSE push -------->| notifications/endpoint_message
    |                          |                                | EndpointRegistry: endpoint_id
    |                          |                                |   owned by this session? -no-> ignore, no ack
    |                          |                                |   yes -> build Trigger, inject
    |                          | <---- ack_notification --------+ ack only after successful injection
```

### Registration flow

1. In a session, the user runs `/endpoint register [label] [--mode run|summary]`.
2. pie calls the new hub MCP tool `register_endpoint` over the existing `pie-hub`
   MCP connection.
3. The hub mints a 32-byte random base64url token, stores only its SHA-256 hash
   (same pattern as agent tokens), and returns
   `https://pie.0xfefe.me/e/<token>` — plaintext shown exactly once.
4. pie writes the binding `endpoint_id -> session` into a session sidecar file
   `<session-uuid>.endpoints.json`, next to the session JSONL — same location and
   pattern as the existing `<session-uuid>.triggers.json` dynamic-trigger sidecar.

### Inbound flow

1. External caller POSTs to `/e/<token>` with an arbitrary body (JSON or text).
2. Hub hashes the token, looks up the `endpoints` row (404 if unknown or revoked —
   a uniform 404, no distinction, to resist probing), enforces a 64 KB body limit
   (413) and a per-endpoint fixed-window rate limit of 120 req/min (429).
3. Hub inserts a `notifications` row (`receiver_agent_id = owner`; payload embeds
   `endpoint_id`, body, `content_type`, `received_at`), pushes through the
   `AgentMailbox` Durable Object, responds `202 {ok: true, id}`.
4. Connected pie clients receive `notifications/endpoint_message` over the existing
   MCP SSE stream. Each client consults its session's `EndpointRegistry`:
   - not owned by this session → produce no trigger, do not ack (the message stays
     in the backlog for the owning session);
   - owned → build a `Trigger` with `payload_summary` derived from the body and
     deliver per the endpoint's mode (`run` default).
5. The client acks (`ack_notification`) only after successful injection. Failed
   injection leaves the notification un-acked for retry on next connect; the
   trigger runtime's 5-minute dedup window guards against short-term double
   injection.

### Offline / resume behavior

- The binding sidecar is loaded on `--resume`, exactly like dynamic trigger rules.
- On (re)connect, pie calls the existing `list_my_inbox` tool, filters un-acked
  `endpoint_message` notifications whose `endpoint_id` the session owns, injects
  them in order, and acks each. Backlog replay therefore reuses the hub's existing
  `pending → delivered → acked` state machine with no new hub-side mechanism.
- Multiple online pie processes for the same hub agent all receive the SSE frame;
  only the owner injects and acks. Non-owners stay silent.

## Hub-side changes (`workers/fefe-hub`)

- **Migration `0004_endpoints.sql`**:
  `endpoints(endpoint_id, owner_agent_id, token_hash, label, mode, created_at,
  revoked_at, last_used_at)`.
- **New route `POST /e/<token>`** — no Bearer auth; the URL is the capability.
  Validation and limits as described in the inbound flow.
- **Three new MCP tools** (following the auth/shape pattern of `register_agent`):
  - `register_endpoint(label?, mode?) -> {endpoint_id, url}`
  - `list_endpoints() -> {endpoints: [...]}` (never returns token plaintext)
  - `revoke_endpoint(endpoint_id) -> {ok}`
- **TTL backstop**: the `notifications` table currently has no cleanup. The `/e/`
  write path lazily deletes that agent's un-acked endpoint notifications older
  than 7 days.

## pie client-side changes

- **`crates/coding-agent/src/triggers/endpoint.rs`** (new): `EndpointRegistry` —
  loads/saves the `<session>.endpoints.json` sidecar; answers `owns(endpoint_id)`
  and the binding's delivery mode.
- **`mcp_notification_hook.rs`**: recognize `notifications/endpoint_message` from
  the `pie-hub` server as a first-class method (like the built-in
  `tools/listChanged`), *not* subject to the custom-notification privacy rule that
  drops params — the endpoint body is intended for the agent. `payload_summary` is
  the formatted body, source-labelled and truncated; the dedup key is the hub's
  `notification_id`. Foreign `endpoint_id` → `None` (no trigger, no ack).
- **`/endpoint` slash command** (registered in `Registry::with_builtins()`,
  following the `TriggersCommand` pattern; `ctx.session_id` is already available):
  `register [label] [--mode run|summary]` / `list` / `revoke <id>`.
- **Delivery**: endpoint messages bypass `direct_inject_action_hook`'s server-level
  `inject_summary` / `inject_and_run` classification; the per-endpoint mode (stored
  on both ends, hub row + sidecar) decides between `TriggerDelivery::InjectAndRun`
  (default) and `TriggerDelivery::InjectSummary`.

## Error handling & security

- Token plaintext is returned only at registration. The local sidecar stores the
  full URL (the session directory is user-private); logs and audit lines show only
  `endpoint_id`.
- Revocation is immediate (table lookup rejects). `/endpoint revoke` also removes
  the sidecar binding.
- Uniform 404 for unknown and revoked tokens; 413 over 64 KB; 429 over the rate
  limit.
- Loop-risk note: `InjectAndRun` here is low risk compared to the `mcp.toml`
  feedback-loop case documented in CLAUDE.md — the message source is an external
  caller, not the agent's own tool calls against the same server.

## Testing

- **Hub** (`tests/hub.test.mjs`, hermetic, no Cloudflare credentials): tool
  register/list/revoke round trips; `POST /e/` returning 202/404/413/429; backlog
  state transitions for endpoint messages; lazy TTL cleanup.
- **pie** (no live provider/hub calls, per CI key-clearing policy):
  `EndpointRegistry` sidecar read/write and resume restore; `map_notification`
  handling of `endpoint_message` (summary, dedup key, ownership gating);
  `/endpoint` argument parsing.

## Out of scope

- Request/response or poll/callback semantics (may layer on later; the endpoint
  design does not preclude them).
- HMAC signing of inbound requests (capability URL only for v1).
- Local non-loopback bind, built-in tunnels, TLS in pie itself.
