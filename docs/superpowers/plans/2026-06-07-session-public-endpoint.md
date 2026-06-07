# Session-Scoped Public Webhook Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a pie session register itself as a public HTTP endpoint (`https://pie.0xfefe.me/e/<token>`); external POSTs flow hub → SSE → trigger runtime into exactly that session, with hub-backlog replay on resume.

**Architecture:** Two halves. Hub half (Cloudflare Worker, `workers/fefe-hub`): new `endpoints` D1 table, capability-token route `POST /e/<token>`, three MCP tools (`register_endpoint` / `list_endpoints` / `revoke_endpoint`), endpoint messages stored as `notifications` rows with `sender_namespace = "endpoint"` and pushed as `notifications/endpoint_message` SSE frames. pie half (Rust): session sidecar `<session>.endpoints.json` holding `endpoint_id → session` bindings (`EndpointRegistry`, mirrors `DynamicTriggerRegistry`), a first-class branch in the MCP notification adapter that ownership-gates and maps frames to `Trigger`s, an action hook that delivers per-endpoint mode (`run` → `InjectAndRun`, `summary` → `InjectSummary`) and acks back to the hub, a one-shot backlog-replay hook on startup, and an `/endpoint` slash command.

**Tech Stack:** TypeScript (Cloudflare Worker, node:test), Rust 2024 (tokio, serde, existing `pie-agent-core` trigger runtime).

**Spec:** `docs/superpowers/specs/2026-06-07-session-public-endpoint-design.md`

**Verification commands:**
- Hub: `cd workers/fefe-hub && npm test` (hermetic — MemoryStore/MemoryMailbox, no Cloudflare credentials)
- pie: `cargo test -p pie-coding-agent` and finally `make ci`
- Rust tests must NOT hit real provider/hub APIs (CI clears all keys)

---

## File Structure

```
workers/fefe-hub/
  migrations/0004_endpoints.sql        (new)  endpoints table
  src/index.ts                         (mod)  EndpointRecord, Store methods, /e/ route,
                                              3 MCP tools, toSseEvent endpoint branch,
                                              inboxItem payload exposure
  tests/hub.test.mjs                   (mod)  endpoint tool + route + SSE + backlog tests

crates/coding-agent/src/
  session/mod.rs                       (mod)  endpoint_sidecar_path{,_for_session}, delete cleanup
  triggers/endpoint.rs                 (new)  EndpointMode, EndpointBinding, EndpointRegistry,
                                              map_endpoint_message, endpoint_action_hook,
                                              EndpointBacklogHook, replay helpers
  triggers/mod.rs                      (mod)  exports
  triggers/mcp_notification_hook.rs    (mod)  route notifications/endpoint_message
  hub_client.rs                        (mod)  register/list/revoke endpoint, ack, inbox fields
  commands.rs                          (mod)  /endpoint command
  main.rs                              (mod)  sidecar load, action-hook chain, backlog hook

docs/endpoints.md                      (new)  user docs
CHANGELOG.md                           (mod)  entry
```

---

## Task 1: Hub — endpoints migration + Store layer

**Files:**
- Create: `workers/fefe-hub/migrations/0004_endpoints.sql`
- Modify: `workers/fefe-hub/src/index.ts` (types ~line 30, `Store` interface ~line 172, `D1Store` ~line 244, `MemoryStore` ~line 596)

- [ ] **Step 1: Write the migration**

```sql
CREATE TABLE IF NOT EXISTS endpoints (
  endpoint_id TEXT PRIMARY KEY,
  owner_agent_id TEXT NOT NULL,
  token_hash TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL,
  mode TEXT NOT NULL,
  created_at TEXT NOT NULL,
  revoked_at TEXT,
  last_used_at TEXT,
  rl_window_start TEXT,
  rl_count INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (owner_agent_id) REFERENCES agents(agent_id)
);

CREATE INDEX IF NOT EXISTS idx_endpoints_owner ON endpoints(owner_agent_id, revoked_at);
```

- [ ] **Step 2: Add types + constants to `src/index.ts`**

Next to the other constants (after `SUMMARY_LIMIT_CHARS`, ~line 19):

```ts
const ENDPOINT_BODY_LIMIT_BYTES = 64 * 1024;
const ENDPOINT_RATE_LIMIT_PER_MINUTE = 120;
const ENDPOINT_BACKLOG_TTL_DAYS = 7;
const ENDPOINT_LABEL_LIMIT_CHARS = 64;
```

Next to the other type aliases (~line 30):

```ts
type EndpointMode = "run" | "summary";
```

Next to `NotificationRecord` (~line 154):

```ts
interface EndpointRecord {
  endpoint_id: string;
  owner_agent_id: string;
  token_hash: string;
  label: string;
  mode: EndpointMode;
  created_at: string;
  revoked_at: string | null;
  last_used_at: string | null;
  rl_window_start: string | null;
  rl_count: number;
}
```

- [ ] **Step 3: Extend the `Store` interface** (after `ackNotifications`, ~line 204)

```ts
  createEndpoint(endpoint: EndpointRecord): Promise<void>;
  getEndpointByTokenHash(tokenHash: string): Promise<EndpointRecord | null>;
  listEndpoints(ownerAgentId: string): Promise<EndpointRecord[]>;
  revokeEndpoint(endpointId: string, ownerAgentId: string, revokedAt: string): Promise<boolean>;
  updateEndpointUsage(endpointId: string, windowStart: string, count: number, lastUsedAt: string): Promise<void>;
  deleteExpiredEndpointNotifications(receiverAgentId: string, beforeIso: string): Promise<void>;
```

- [ ] **Step 4: Implement in `D1Store`** (after `ackNotifications`, ~line 593)

```ts
  async createEndpoint(endpoint: EndpointRecord): Promise<void> {
    await this.db
      .prepare(
        `INSERT INTO endpoints
         (endpoint_id, owner_agent_id, token_hash, label, mode,
          created_at, revoked_at, last_used_at, rl_window_start, rl_count)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .bind(
        endpoint.endpoint_id,
        endpoint.owner_agent_id,
        endpoint.token_hash,
        endpoint.label,
        endpoint.mode,
        endpoint.created_at,
        endpoint.revoked_at,
        endpoint.last_used_at,
        endpoint.rl_window_start,
        endpoint.rl_count,
      )
      .run();
  }

  getEndpointByTokenHash(tokenHash: string): Promise<EndpointRecord | null> {
    return this.db.prepare("SELECT * FROM endpoints WHERE token_hash = ?").bind(tokenHash).first<EndpointRecord>();
  }

  async listEndpoints(ownerAgentId: string): Promise<EndpointRecord[]> {
    const result = await this.db
      .prepare("SELECT * FROM endpoints WHERE owner_agent_id = ? ORDER BY created_at")
      .bind(ownerAgentId)
      .all<EndpointRecord>();
    return result.results ?? [];
  }

  async revokeEndpoint(endpointId: string, ownerAgentId: string, revokedAt: string): Promise<boolean> {
    const result = await this.db
      .prepare(
        `UPDATE endpoints SET revoked_at = ?
         WHERE endpoint_id = ? AND owner_agent_id = ? AND revoked_at IS NULL`,
      )
      .bind(revokedAt, endpointId, ownerAgentId)
      .run();
    return d1ChangedRows(result) === 1;
  }

  async updateEndpointUsage(endpointId: string, windowStart: string, count: number, lastUsedAt: string): Promise<void> {
    await this.db
      .prepare("UPDATE endpoints SET rl_window_start = ?, rl_count = ?, last_used_at = ? WHERE endpoint_id = ?")
      .bind(windowStart, count, lastUsedAt, endpointId)
      .run();
  }

  async deleteExpiredEndpointNotifications(receiverAgentId: string, beforeIso: string): Promise<void> {
    await this.db
      .prepare(
        `DELETE FROM notifications
         WHERE receiver_agent_id = ? AND sender_namespace = 'endpoint'
           AND status IN ('pending', 'delivered') AND created_at < ?`,
      )
      .bind(receiverAgentId, beforeIso)
      .run();
  }
```

- [ ] **Step 5: Implement in `MemoryStore`** (add a map next to the other private maps ~line 603, methods after `ackNotifications` ~line 777)

```ts
  private readonly endpoints = new Map<string, EndpointRecord>();
```

```ts
  async createEndpoint(endpoint: EndpointRecord): Promise<void> {
    this.endpoints.set(endpoint.endpoint_id, { ...endpoint });
  }

  async getEndpointByTokenHash(tokenHash: string): Promise<EndpointRecord | null> {
    return [...this.endpoints.values()].find((e) => e.token_hash === tokenHash) ?? null;
  }

  async listEndpoints(ownerAgentId: string): Promise<EndpointRecord[]> {
    return [...this.endpoints.values()]
      .filter((e) => e.owner_agent_id === ownerAgentId)
      .sort((a, b) => a.created_at.localeCompare(b.created_at));
  }

  async revokeEndpoint(endpointId: string, ownerAgentId: string, revokedAt: string): Promise<boolean> {
    const endpoint = this.endpoints.get(endpointId);
    if (!endpoint || endpoint.owner_agent_id !== ownerAgentId || endpoint.revoked_at) {
      return false;
    }
    this.endpoints.set(endpointId, { ...endpoint, revoked_at: revokedAt });
    return true;
  }

  async updateEndpointUsage(endpointId: string, windowStart: string, count: number, lastUsedAt: string): Promise<void> {
    const endpoint = this.endpoints.get(endpointId);
    if (endpoint) {
      this.endpoints.set(endpointId, { ...endpoint, rl_window_start: windowStart, rl_count: count, last_used_at: lastUsedAt });
    }
  }

  async deleteExpiredEndpointNotifications(receiverAgentId: string, beforeIso: string): Promise<void> {
    for (const [id, n] of this.notifications.entries()) {
      if (
        n.receiver_agent_id === receiverAgentId &&
        n.sender_namespace === "endpoint" &&
        (n.status === "pending" || n.status === "delivered") &&
        n.created_at < beforeIso
      ) {
        this.notifications.delete(id);
      }
    }
  }
```

- [ ] **Step 6: Verify it builds and existing tests pass**

Run: `cd workers/fefe-hub && npm test`
Expected: PASS (build compiles; all existing tests green; no new tests yet)

- [ ] **Step 7: Commit**

```bash
git add workers/fefe-hub/migrations/0004_endpoints.sql workers/fefe-hub/src/index.ts
git commit -m "hub: add endpoints table and store layer"
```

---

## Task 2: Hub — `register_endpoint` / `list_endpoints` / `revoke_endpoint` MCP tools

**Files:**
- Modify: `workers/fefe-hub/src/index.ts` (`callTool` ~line 1023, handlers near `sendNotification` ~line 2009, `TOOL_DEFINITIONS` ~line 2260, helpers near `payloadVisibilityValue` ~line 2624)
- Test: `workers/fefe-hub/tests/hub.test.mjs`

- [ ] **Step 1: Write failing tests** (append to `hub.test.mjs`, before the helper section; reuse `registerUser`, `callTool`, `rpc` helpers)

```js
test("register_endpoint mints a capability URL once and lists/revokes without leaking it", async () => {
  const app = createTestApp();
  const alice = await registerUser(app, "epalice");
  const agent = await callTool(app, alice.session_token, "register_agent", {
    handle: "epagent",
    display_name: "Endpoint Agent",
    description: "",
    capabilities: [],
  });

  const registered = await callTool(app, agent.hub_token, "register_endpoint", {
    label: "github hooks",
    mode: "summary",
  });
  assert.deepEqual(Object.keys(registered).sort(), ["endpoint_id", "label", "mode", "token_note", "url"]);
  assert.match(registered.endpoint_id, /^[0-9a-f-]{36}$/);
  assert.match(registered.url, /^https:\/\/hub\.test\/e\/hub_ep_/);
  assert.equal(registered.label, "github hooks");
  assert.equal(registered.mode, "summary");

  // Defaults: label "default", mode "run".
  const defaulted = await callTool(app, agent.hub_token, "register_endpoint", {});
  assert.equal(defaulted.label, "default");
  assert.equal(defaulted.mode, "run");

  // list never returns the token or URL.
  const listed = await callTool(app, agent.hub_token, "list_endpoints", {});
  assert.equal(listed.endpoints.length, 2);
  assert.doesNotMatch(JSON.stringify(listed), /hub_ep_/);
  assert.deepEqual(Object.keys(listed.endpoints[0]).sort(), [
    "created_at",
    "endpoint_id",
    "label",
    "last_used_at",
    "mode",
    "revoked_at",
  ]);

  // revoke own endpoint works; revoking it again fails.
  const revoked = await callTool(app, agent.hub_token, "revoke_endpoint", {
    endpoint_id: registered.endpoint_id,
  });
  assert.equal(revoked.revoked, true);
  const again = await rpc(app, agent.hub_token, "tools/call", {
    name: "revoke_endpoint",
    arguments: { endpoint_id: registered.endpoint_id },
  });
  assert.equal(again.error.data.name, "not_found");
});

test("register_endpoint rejects bad mode and another agent cannot revoke", async () => {
  const app = createTestApp();
  const alice = await registerUser(app, "epowner");
  const owner = await callTool(app, alice.session_token, "register_agent", {
    handle: "epowneragent",
    display_name: "Owner",
    description: "",
    capabilities: [],
  });
  const bob = await registerUser(app, "epthief");
  const thief = await callTool(app, bob.session_token, "register_agent", {
    handle: "epthiefagent",
    display_name: "Thief",
    description: "",
    capabilities: [],
  });

  const badMode = await rpc(app, owner.hub_token, "tools/call", {
    name: "register_endpoint",
    arguments: { mode: "shout" },
  });
  assert.equal(badMode.error.data.name, "schema_invalid");

  const registered = await callTool(app, owner.hub_token, "register_endpoint", {});
  const stolen = await rpc(app, thief.hub_token, "tools/call", {
    name: "revoke_endpoint",
    arguments: { endpoint_id: registered.endpoint_id },
  });
  assert.equal(stolen.error.data.name, "not_found");
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd workers/fefe-hub && npm test`
Expected: FAIL with `unknown tool register_endpoint`

- [ ] **Step 3: Implement the tools**

In `callTool` (~line 1023) the handlers need the request origin for URL minting. Add cases before `default`:

```ts
      case "register_endpoint":
        return this.registerEndpoint(await this.authenticate(request, "agent"), args, new URL(request.url).origin);
      case "list_endpoints":
        return this.listEndpoints(await this.authenticate(request, "agent"), args);
      case "revoke_endpoint":
        return this.revokeEndpoint(await this.authenticate(request, "agent"), args);
```

Handlers (add after `ackNotification`, ~line 2093). Token shape mirrors `issueAgentToken`: plaintext returned once, only SHA-256 stored. Permission: `notification:receive` — endpoint messages land in this agent's own inbox, and every existing token already has it (a new permission string would break tokens issued before this change).

```ts
  private async registerEndpoint(principal: Principal, args: Record<string, unknown>, origin: string): Promise<unknown> {
    assertAgent(principal);
    requirePermission(principal, "notification:receive");
    ensureOnly(args, ["label", "mode"]);
    const label = validatePlainText(optionalString(args.label, "label") ?? "default", "label", ENDPOINT_LABEL_LIMIT_CHARS);
    const mode = endpointModeValue(optionalString(args.mode, "mode") ?? "run");
    const endpointId = crypto.randomUUID();
    const token = `hub_ep_${randomSecret(32)}`;
    await this.store.createEndpoint({
      endpoint_id: endpointId,
      owner_agent_id: principal.agent_id,
      token_hash: await sha256Hex(token),
      label,
      mode,
      created_at: nowIso(),
      revoked_at: null,
      last_used_at: null,
      rl_window_start: null,
      rl_count: 0,
    });
    return {
      endpoint_id: endpointId,
      url: `${origin}/e/${token}`,
      label,
      mode,
      token_note: "Store this URL locally; the hub stores only a hash and will not show it again.",
    };
  }

  private async listEndpoints(principal: Principal, args: Record<string, unknown>): Promise<unknown> {
    assertAgent(principal);
    requirePermission(principal, "notification:receive");
    ensureOnly(args, []);
    const endpoints = await this.store.listEndpoints(principal.agent_id);
    return {
      endpoints: endpoints.map((endpoint) => ({
        endpoint_id: endpoint.endpoint_id,
        label: endpoint.label,
        mode: endpoint.mode,
        created_at: endpoint.created_at,
        revoked_at: endpoint.revoked_at,
        last_used_at: endpoint.last_used_at,
      })),
    };
  }

  private async revokeEndpoint(principal: Principal, args: Record<string, unknown>): Promise<unknown> {
    assertAgent(principal);
    requirePermission(principal, "notification:receive");
    ensureOnly(args, ["endpoint_id"]);
    const endpointId = uuidField(args, "endpoint_id");
    const revoked = await this.store.revokeEndpoint(endpointId, principal.agent_id, nowIso());
    if (!revoked) {
      throw ERR.notFound("No active endpoint with that id belongs to this agent.");
    }
    return { revoked: true };
  }
```

Helper next to `payloadVisibilityValue` (~line 2624):

```ts
function endpointModeValue(value: string): EndpointMode {
  if (value === "run" || value === "summary") return value;
  throw ERR.schemaInvalid(["mode must be run or summary"]);
}
```

`TOOL_DEFINITIONS` additions (~line 2316, after `unblock_sender`):

```ts
  tool("register_endpoint", "Mint a public webhook URL for this agent. Returns the capability URL once.", {
    label: "Optional plain-text label, at most 64 characters.",
    mode: "Delivery mode for the owning session: run (default) or summary.",
  }),
  tool("list_endpoints", "List this agent's webhook endpoints without token material.", {}),
  tool("revoke_endpoint", "Revoke one of this agent's webhook endpoints immediately.", {
    endpoint_id: "Endpoint UUID returned by register_endpoint.",
  }),
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd workers/fefe-hub && npm test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add workers/fefe-hub/src/index.ts workers/fefe-hub/tests/hub.test.mjs
git commit -m "hub: add register/list/revoke endpoint tools"
```

---

## Task 3: Hub — `POST /e/<token>` inbound route

**Files:**
- Modify: `workers/fefe-hub/src/index.ts` (`HubApp.fetch` ~line 902, new handler after `sendNotificationAsAgent` ~line 2071, helper near `addDaysIso` ~line 2797)
- Test: `workers/fefe-hub/tests/hub.test.mjs`

- [ ] **Step 1: Write failing tests**

```js
test("endpoint POST accepts, backlogs, rate limits, and 404s uniformly", async () => {
  const app = createTestApp();
  const alice = await registerUser(app, "eppost");
  const agent = await callTool(app, alice.session_token, "register_agent", {
    handle: "eppostagent",
    display_name: "Poster",
    description: "",
    capabilities: [],
  });
  const registered = await callTool(app, agent.hub_token, "register_endpoint", { label: "ci" });
  const path = new URL(registered.url).pathname;

  // Happy path: 202 with an id; lands in the inbox with the Shared payload body.
  const accepted = await app.fetch(
    new Request(`${BASE}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ build: 42, status: "red" }),
    }),
  );
  assert.equal(accepted.status, 202);
  const receipt = await accepted.json();
  assert.equal(receipt.ok, true);
  assert.match(receipt.id, /^[0-9a-f-]{36}$/);

  const inbox = await callTool(app, agent.hub_token, "list_my_inbox", {});
  assert.equal(inbox.items.length, 1);
  const item = inbox.items[0];
  assert.equal(item.notification_id, receipt.id);
  assert.equal(item.payload_visibility, "Shared");
  assert.equal(item.payload.endpoint_id, registered.endpoint_id);
  assert.equal(item.payload.label, "ci");
  assert.equal(item.payload.mode, "run");
  assert.equal(item.payload.body, JSON.stringify({ build: 42, status: "red" }));
  assert.equal(item.payload.content_type, "application/json");
  // The D1-visible summary stays bounded and never embeds the raw body.
  assert.doesNotMatch(item.summary, /build/);

  // Unknown token and revoked token both 404 with the same body.
  const unknown = await app.fetch(new Request(`${BASE}/e/hub_ep_${"a".repeat(64)}`, { method: "POST", body: "x" }));
  assert.equal(unknown.status, 404);
  await callTool(app, agent.hub_token, "revoke_endpoint", { endpoint_id: registered.endpoint_id });
  const revoked = await app.fetch(new Request(`${BASE}${path}`, { method: "POST", body: "x" }));
  assert.equal(revoked.status, 404);
  assert.deepEqual(await revoked.json(), await unknown.json());
});

test("endpoint POST enforces body cap and per-minute rate limit", async () => {
  const app = createTestApp();
  const alice = await registerUser(app, "eplimits");
  const agent = await callTool(app, alice.session_token, "register_agent", {
    handle: "eplimitsagent",
    display_name: "Limits",
    description: "",
    capabilities: [],
  });
  const registered = await callTool(app, agent.hub_token, "register_endpoint", {});
  const path = new URL(registered.url).pathname;

  const oversize = await app.fetch(
    new Request(`${BASE}${path}`, { method: "POST", body: "x".repeat(64 * 1024 + 1) }),
  );
  assert.equal(oversize.status, 413);

  let lastStatus = 0;
  for (let i = 0; i < 121; i += 1) {
    const response = await app.fetch(new Request(`${BASE}${path}`, { method: "POST", body: `n${i}` }));
    lastStatus = response.status;
  }
  assert.equal(lastStatus, 429);
});

test("endpoint POST lazily drops un-acked endpoint backlog older than 7 days", async () => {
  const store = new MemoryStore();
  const app = createTestApp(store);
  const alice = await registerUser(app, "epttl");
  const agent = await callTool(app, alice.session_token, "register_agent", {
    handle: "epttlagent",
    display_name: "TTL",
    description: "",
    capabilities: [],
  });
  const registered = await callTool(app, agent.hub_token, "register_endpoint", {});
  const path = new URL(registered.url).pathname;

  // Plant a stale endpoint notification directly in the store.
  const staleAt = new Date(Date.now() - 8 * 24 * 60 * 60 * 1000).toISOString();
  await store.createNotification({
    notification_id: "00000000-0000-4000-8000-00000000aaaa",
    receiver_agent_id: agent.agent.agent_id,
    sender_agent_id: agent.agent.agent_id,
    sender_handle: registered.endpoint_id,
    sender_namespace: "endpoint",
    summary: "endpoint default: message received",
    payload_json: JSON.stringify({ endpoint_id: registered.endpoint_id, body: "old" }),
    payload_visibility: "Shared",
    status: "pending",
    first_contact_required: 0,
    created_at: staleAt,
    delivered_at: null,
    acked_at: null,
  });

  const fresh = await app.fetch(new Request(`${BASE}${path}`, { method: "POST", body: "new" }));
  assert.equal(fresh.status, 202);

  const inbox = await callTool(app, agent.hub_token, "list_my_inbox", {});
  const bodies = inbox.items.map((item) => item.payload?.body);
  assert.ok(bodies.includes("new"));
  assert.ok(!bodies.includes("old"), "stale endpoint backlog must be dropped");
});
```

Note: the inbox `payload` field these tests assert on is added in this task's Step 3 (`inboxItem` change) because the route tests need it to observe bodies. The SSE frame branch comes in Task 4.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd workers/fefe-hub && npm test`
Expected: FAIL — `/e/...` returns 404 `not_found` for the happy path (202 expected)

- [ ] **Step 3: Implement**

Route in `HubApp.fetch`, before the final `return json({ error: "not_found" }, 404)` (~line 945):

```ts
      if (request.method === "POST" && url.pathname.startsWith("/e/")) {
        return this.handleEndpointPost(request, url);
      }
```

Handler (after `sendNotificationAsAgent`, ~line 2071). Uniform `{ error: "not_found" }` 404 for unknown/revoked/probe so callers can't distinguish:

```ts
  private async handleEndpointPost(request: Request, url: URL): Promise<Response> {
    const token = url.pathname.slice("/e/".length);
    if (!/^hub_ep_[A-Za-z0-9_-]{16,160}$/.test(token)) {
      return json({ error: "not_found" }, 404);
    }
    const bodyText = await request.text();
    if (byteLength(bodyText) > ENDPOINT_BODY_LIMIT_BYTES) {
      return json({ error: "body_too_large", cap_bytes: ENDPOINT_BODY_LIMIT_BYTES }, 413);
    }
    const endpoint = await this.store.getEndpointByTokenHash(await sha256Hex(token));
    if (!endpoint || endpoint.revoked_at) {
      return json({ error: "not_found" }, 404);
    }
    const owner = await this.store.getAgent(endpoint.owner_agent_id);
    if (!owner || owner.deleted_at) {
      return json({ error: "not_found" }, 404);
    }
    const now = nowIso();
    // Fixed one-minute window: "2026-06-07T12:34" buckets.
    const windowStart = now.slice(0, 16);
    const count = endpoint.rl_window_start === windowStart ? endpoint.rl_count + 1 : 1;
    if (count > ENDPOINT_RATE_LIMIT_PER_MINUTE) {
      return json({ error: "rate_limited" }, 429);
    }
    await this.store.updateEndpointUsage(endpoint.endpoint_id, windowStart, count, now);
    // Lazy TTL backstop: the notifications table has no other cleanup path.
    await this.store.deleteExpiredEndpointNotifications(owner.agent_id, daysAgoIso(ENDPOINT_BACKLOG_TTL_DAYS));
    const notification: NotificationRecord = {
      notification_id: crypto.randomUUID(),
      receiver_agent_id: owner.agent_id,
      sender_agent_id: owner.agent_id,
      sender_handle: endpoint.endpoint_id,
      sender_namespace: "endpoint",
      // Bounded display summary only — the raw body lives in payload_json (Shared).
      summary: `endpoint ${endpoint.label}: message received`,
      payload_json: JSON.stringify({
        endpoint_id: endpoint.endpoint_id,
        label: endpoint.label,
        mode: endpoint.mode,
        content_type: request.headers.get("content-type") ?? "application/octet-stream",
        body: bodyText,
        received_at: now,
      }),
      payload_visibility: "Shared",
      status: "pending",
      first_contact_required: 0,
      created_at: now,
      delivered_at: null,
      acked_at: null,
    };
    await this.store.createNotification(notification);
    const delivered = await this.mailbox.push(owner.agent_id, notification);
    if (delivered) {
      await this.store.markNotificationDelivered(notification.notification_id, nowIso());
    }
    return json({ ok: true, id: notification.notification_id }, 202);
  }
```

Helper next to `addDaysIso` (~line 2797):

```ts
function daysAgoIso(days: number): string {
  return new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
}
```

`inboxItem` (~line 2703) — expose the Shared payload so backlog replay can carry the body (matches the existing `send_notification` Shared contract; Local/Redacted payloads stay hidden):

```ts
function inboxItem(notification: NotificationRecord): Record<string, unknown> {
  return {
    notification_id: notification.notification_id,
    sender_agent_id: notification.sender_agent_id,
    sender: `@${notification.sender_handle}@${notification.sender_namespace}`,
    summary: notification.summary,
    payload_visibility: notification.payload_visibility,
    payload: notification.payload_visibility === "Shared" ? safeJsonParse(notification.payload_json) : undefined,
    first_contact_required: notification.first_contact_required === 1,
    status: notification.status,
    created_at: notification.created_at,
    delivered_at: notification.delivered_at,
  };
}
```

Note: `endpoint.label` is `validatePlainText`-bounded at registration (≤64 chars, no URLs), so the built `summary` stays under `SUMMARY_LIMIT_CHARS`.

Note: if any pre-existing test pins the exact key set of inbox items (`assert.deepEqual(Object.keys(item).sort(), ...)`), update its expectation to include the new `payload` key — that shape change is intentional.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd workers/fefe-hub && npm test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add workers/fefe-hub/src/index.ts workers/fefe-hub/tests/hub.test.mjs
git commit -m "hub: add public POST /e/<token> endpoint route"
```

---

## Task 4: Hub — `notifications/endpoint_message` SSE frame

**Files:**
- Modify: `workers/fefe-hub/src/index.ts` (`toSseEvent` ~line 2224)
- Test: `workers/fefe-hub/tests/hub.test.mjs`

- [ ] **Step 1: Write failing test**

```js
test("endpoint POST pushes a live notifications/endpoint_message SSE frame", async () => {
  const app = createTestApp();
  const alice = await registerUser(app, "epsse");
  const agent = await callTool(app, alice.session_token, "register_agent", {
    handle: "epsseagent",
    display_name: "SSE",
    description: "",
    capabilities: [],
  });
  const registered = await callTool(app, agent.hub_token, "register_endpoint", { label: "sse" });
  const path = new URL(registered.url).pathname;

  const stream = await app.fetch(
    new Request(`${BASE}/mcp`, {
      headers: { authorization: `Bearer ${agent.hub_token}`, accept: "text/event-stream" },
    }),
  );
  assert.equal(stream.status, 200);
  const reader = stream.body.getReader();

  const accepted = await app.fetch(new Request(`${BASE}${path}`, { method: "POST", body: "hello agent" }));
  assert.equal(accepted.status, 202);
  const receipt = await accepted.json();

  const chunk = await withTimeout(readChunk(reader), 2000);
  const frame = JSON.parse(chunk.split("data: ")[1].split("\n")[0]);
  assert.equal(frame.method, "notifications/endpoint_message");
  assert.equal(frame.params.notification_id, receipt.id);
  assert.equal(frame.params.endpoint_id, registered.endpoint_id);
  assert.equal(frame.params.label, "sse");
  assert.equal(frame.params.mode, "run");
  assert.equal(frame.params.body, "hello agent");
  assert.equal(frame.params._meta.pie_dedup_key, receipt.id);
  await reader.cancel();
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd workers/fefe-hub && npm test`
Expected: FAIL — frame method is `notifications/agent_message` (the existing fallback branch)

- [ ] **Step 3: Rewrite `toSseEvent` with a three-way branch**

Replace the whole function (~line 2224):

```ts
function toSseEvent(notification: NotificationRecord): string {
  let method: string;
  let params: Record<string, unknown>;
  if (notification.sender_namespace === "system") {
    method = "notifications/agent_revoked";
    params = {
      agent_id: notification.receiver_agent_id,
      revoked_at: notification.created_at,
      reason: "revoked",
      _meta: {
        pie_dedup_key: notification.notification_id,
        pie_summary: notification.summary,
      },
    };
  } else if (notification.sender_namespace === "endpoint") {
    const payload = safeJsonParse(notification.payload_json) as Record<string, unknown> | undefined;
    method = "notifications/endpoint_message";
    params = {
      notification_id: notification.notification_id,
      endpoint_id: payload?.endpoint_id,
      label: payload?.label,
      mode: payload?.mode,
      content_type: payload?.content_type,
      body: payload?.body,
      received_at: payload?.received_at,
      _meta: {
        pie_dedup_key: notification.notification_id,
      },
    };
  } else {
    method = "notifications/agent_message";
    params = {
      notification_id: notification.notification_id,
      agent_id: notification.sender_agent_id,
      handle: notification.sender_handle,
      namespace: notification.sender_namespace,
      sender: `@${notification.sender_handle}@${notification.sender_namespace}`,
      payload_visibility: notification.payload_visibility,
      first_contact_required: notification.first_contact_required === 1,
      payload: notification.payload_visibility === "Shared" ? safeJsonParse(notification.payload_json) : undefined,
      _meta: {
        pie_dedup_key: notification.notification_id,
        pie_summary: notification.summary,
        receiver_agent_id: notification.receiver_agent_id,
        sender_agent_id: notification.sender_agent_id,
        action_class: "notification",
      },
    };
  }
  const data = { jsonrpc: "2.0", method, params };
  return `id: ${notification.notification_id}\nevent: message\ndata: ${JSON.stringify(data)}\n\n`;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd workers/fefe-hub && npm test`
Expected: PASS (all — including pre-existing agent_message/agent_revoked SSE tests)

- [ ] **Step 5: Commit**

```bash
git add workers/fefe-hub/src/index.ts workers/fefe-hub/tests/hub.test.mjs
git commit -m "hub: push endpoint messages as notifications/endpoint_message"
```

---

## Task 5: pie — session sidecar helpers

**Files:**
- Modify: `crates/coding-agent/src/session/mod.rs`

- [ ] **Step 1: Write failing tests** (in the existing `tests` module of `session/mod.rs`)

```rust
    #[test]
    fn endpoint_sidecar_path_lives_next_to_session_file() {
        let path = std::path::Path::new("/tmp/session-id.jsonl");
        assert_eq!(
            endpoint_sidecar_path(path),
            std::path::PathBuf::from("/tmp/session-id.endpoints.json")
        );
    }

    #[tokio::test]
    async fn delete_removes_endpoint_sidecar() {
        let dir = tempdir().unwrap();
        let repo = JsonlSessionRepo::new(dir.path());
        let session = repo.create("/cwd").await.unwrap();
        let id = session
            .storage()
            .get_metadata_json()
            .await
            .unwrap()
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        let session_path = repo.list().await.unwrap().pop().unwrap();
        let endpoint_path = endpoint_sidecar_path(&session_path);
        std::fs::write(&endpoint_path, "{\"version\":1,\"endpoints\":[]}").unwrap();

        let deleted = delete_by_id(&repo, &id).await.unwrap();

        assert_eq!(deleted, session_path);
        assert!(!endpoint_path.exists());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p pie-coding-agent session::tests::endpoint_sidecar_path_lives_next_to_session_file`
Expected: FAIL to compile — `endpoint_sidecar_path` not found

- [ ] **Step 3: Implement** (next to `cron_sidecar_path`, ~line 29)

```rust
/// Public endpoint bindings are session-scoped sidecars, parallel to trigger sidecars.
pub fn endpoint_sidecar_path(session_path: &std::path::Path) -> PathBuf {
    session_path.with_extension("endpoints.json")
}

/// Return the endpoint-binding sidecar for a live session.
pub async fn endpoint_sidecar_path_for_session(
    session: &Session,
    repo: &JsonlSessionRepo,
) -> Result<PathBuf> {
    let metadata = session.storage().get_metadata_json().await?;
    if let Some(path) = metadata.get("path").and_then(|v| v.as_str()) {
        return Ok(endpoint_sidecar_path(std::path::Path::new(path)));
    }

    let session_id = metadata
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown-session");
    Ok(repo.root().join(format!("{session_id}.endpoints.json")))
}
```

In `delete_by_id` (~line 168), after the cron sidecar removal block:

```rust
    let endpoint_sidecar = endpoint_sidecar_path(&path);
    match tokio::fs::remove_file(&endpoint_sidecar).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("delete {}", endpoint_sidecar.display())),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p pie-coding-agent session::tests`
Expected: PASS (all session tests)

- [ ] **Step 5: Commit**

```bash
git add crates/coding-agent/src/session/mod.rs
git commit -m "pie: add endpoint sidecar path helpers"
```

---

## Task 6: pie — `EndpointRegistry` (triggers/endpoint.rs)

**Files:**
- Create: `crates/coding-agent/src/triggers/endpoint.rs`
- Modify: `crates/coding-agent/src/triggers/mod.rs`

- [ ] **Step 1: Create the module with types, registry, and failing tests**

New file `crates/coding-agent/src/triggers/endpoint.rs`. Storage shape mirrors `dynamic.rs` (`{version, endpoints}`, atomic tmp+rename writes):

```rust
//! Session-scoped public webhook endpoint bindings.
//!
//! A binding records that hub endpoint `endpoint_id` belongs to THIS session. The hub
//! fans out `notifications/endpoint_message` frames to every connected client of the
//! owning agent; only the session whose sidecar holds the binding converts the frame
//! into a runtime `Trigger` (and acks it). Foreign frames are ignored so they stay in
//! the hub backlog for the owning session.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EndpointMode {
    /// Inject the message into the parent chat AND run one model turn.
    Run,
    /// Inject the message summary into the parent chat only; no model call.
    Summary,
}

impl EndpointMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Summary => "summary",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "run" => Some(Self::Run),
            "summary" => Some(Self::Summary),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointBinding {
    pub endpoint_id: String,
    pub label: String,
    pub mode: EndpointMode,
    /// Full public URL. Contains the capability token; the session directory is
    /// user-private, and the registration flow already showed it to the user once.
    pub url: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
pub struct EndpointRegistry {
    inner: Arc<Mutex<EndpointRegistryState>>,
}

#[derive(Clone, Debug, Default)]
struct EndpointRegistryState {
    bindings: Vec<EndpointBinding>,
    storage_path: Option<PathBuf>,
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum EndpointStorageError {
    #[error("read endpoint bindings: {0}")]
    Read(String),
    #[error("parse endpoint bindings: {0}")]
    Parse(String),
    #[error("write endpoint bindings: {0}")]
    Write(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EndpointFile {
    version: u32,
    endpoints: Vec<EndpointBinding>,
}

const ENDPOINT_FILE_VERSION: u32 = 1;

impl EndpointRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_from_path(&self, path: impl Into<PathBuf>) -> Result<(), EndpointStorageError> {
        let path = path.into();
        let bindings = read_bindings_file(&path)?;
        let mut state = self.inner.lock();
        state.bindings = bindings;
        state.storage_path = Some(path);
        Ok(())
    }

    pub fn storage_path(&self) -> Option<PathBuf> {
        self.inner.lock().storage_path.clone()
    }

    pub fn add_binding(&self, binding: EndpointBinding) -> Result<(), EndpointStorageError> {
        let mut state = self.inner.lock();
        let mut next = state.bindings.clone();
        next.retain(|b| b.endpoint_id != binding.endpoint_id);
        next.push(binding);
        if let Some(path) = &state.storage_path {
            write_bindings_file(path, &next)?;
        }
        state.bindings = next;
        Ok(())
    }

    pub fn remove_binding(
        &self,
        endpoint_id: &str,
    ) -> Result<Option<EndpointBinding>, EndpointStorageError> {
        let mut state = self.inner.lock();
        let Some(pos) = state
            .bindings
            .iter()
            .position(|b| b.endpoint_id == endpoint_id)
        else {
            return Ok(None);
        };
        let mut next = state.bindings.clone();
        let removed = next.remove(pos);
        if let Some(path) = &state.storage_path {
            write_bindings_file(path, &next)?;
        }
        state.bindings = next;
        Ok(Some(removed))
    }

    pub fn list(&self) -> Vec<EndpointBinding> {
        self.inner.lock().bindings.clone()
    }

    /// Return the binding for `endpoint_id` when THIS session owns it.
    pub fn owns(&self, endpoint_id: &str) -> Option<EndpointBinding> {
        self.inner
            .lock()
            .bindings
            .iter()
            .find(|b| b.endpoint_id == endpoint_id)
            .cloned()
    }
}

pub fn global_endpoint_registry() -> &'static EndpointRegistry {
    static CELL: OnceCell<EndpointRegistry> = OnceCell::new();
    CELL.get_or_init(EndpointRegistry::new)
}

fn read_bindings_file(path: &Path) -> Result<Vec<EndpointBinding>, EndpointStorageError> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(EndpointStorageError::Read(e.to_string())),
    };
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    let file: EndpointFile =
        serde_json::from_str(&text).map_err(|e| EndpointStorageError::Parse(e.to_string()))?;
    Ok(file.endpoints)
}

fn write_bindings_file(
    path: &Path,
    bindings: &[EndpointBinding],
) -> Result<(), EndpointStorageError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| EndpointStorageError::Write(e.to_string()))?;
    }
    let file = EndpointFile {
        version: ENDPOINT_FILE_VERSION,
        endpoints: bindings.to_vec(),
    };
    let text =
        serde_json::to_string_pretty(&file).map_err(|e| EndpointStorageError::Write(e.to_string()))?;
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("endpoints.json");
    let tmp = path.with_file_name(format!("{file_name}.tmp-{}", Uuid::new_v4().simple()));
    std::fs::write(&tmp, text).map_err(|e| EndpointStorageError::Write(e.to_string()))?;
    std::fs::rename(&tmp, path).map_err(|e| EndpointStorageError::Write(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn binding(endpoint_id: &str, mode: EndpointMode) -> EndpointBinding {
        EndpointBinding {
            endpoint_id: endpoint_id.into(),
            label: "ci".into(),
            mode,
            url: format!("https://hub.test/e/hub_ep_{endpoint_id}"),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn persists_and_reloads_bindings() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("session.endpoints.json");
        let registry = EndpointRegistry::new();
        registry.load_from_path(&path).expect("load empty");
        registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");

        let reloaded = EndpointRegistry::new();
        reloaded.load_from_path(&path).expect("reload");
        assert_eq!(reloaded.list().len(), 1);
        assert_eq!(reloaded.list()[0].endpoint_id, "ep-1");
        assert_eq!(reloaded.list()[0].mode, EndpointMode::Run);
    }

    #[test]
    fn owns_distinguishes_local_from_foreign() {
        let registry = EndpointRegistry::new();
        registry
            .add_binding(binding("ep-mine", EndpointMode::Summary))
            .expect("add");
        assert!(registry.owns("ep-mine").is_some());
        assert!(registry.owns("ep-other").is_none());
    }

    #[test]
    fn remove_binding_updates_storage_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("session.endpoints.json");
        let registry = EndpointRegistry::new();
        registry.load_from_path(&path).expect("load empty");
        registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");

        let removed = registry.remove_binding("ep-1").expect("remove");
        assert_eq!(removed.map(|b| b.endpoint_id), Some("ep-1".to_string()));

        let reloaded = EndpointRegistry::new();
        reloaded.load_from_path(&path).expect("reload");
        assert!(reloaded.list().is_empty());
    }

    #[test]
    fn re_adding_same_endpoint_id_replaces_binding() {
        let registry = EndpointRegistry::new();
        registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");
        registry
            .add_binding(binding("ep-1", EndpointMode::Summary))
            .expect("re-add");
        assert_eq!(registry.list().len(), 1);
        assert_eq!(registry.list()[0].mode, EndpointMode::Summary);
    }

    #[test]
    fn mode_serde_round_trips_lowercase() {
        assert_eq!(
            serde_json::to_string(&EndpointMode::Run).unwrap(),
            "\"run\""
        );
        assert_eq!(EndpointMode::parse("summary"), Some(EndpointMode::Summary));
        assert_eq!(EndpointMode::parse("shout"), None);
    }
}
```

- [ ] **Step 2: Wire the module into `triggers/mod.rs`**

```rust
pub mod endpoint;
```

and a re-export block next to the others:

```rust
#[allow(unused_imports)]
pub use endpoint::{
    EndpointBinding, EndpointMode, EndpointRegistry, global_endpoint_registry,
};
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p pie-coding-agent triggers::endpoint`
Expected: PASS (5 tests)

- [ ] **Step 4: Commit**

```bash
git add crates/coding-agent/src/triggers/endpoint.rs crates/coding-agent/src/triggers/mod.rs
git commit -m "pie: add session-scoped EndpointRegistry"
```

---

## Task 7: pie — map `notifications/endpoint_message` → `Trigger`

**Files:**
- Modify: `crates/coding-agent/src/triggers/endpoint.rs` (mapping fn lives here so backlog replay can reuse it)
- Modify: `crates/coding-agent/src/triggers/mcp_notification_hook.rs` (route the method; make two helpers `pub(crate)`)

- [ ] **Step 1: Write failing tests** (append to `triggers/endpoint.rs` tests module)

```rust
    use pie_agent_core::{PayloadVisibility, ReplacementPolicy, SourceKind, TriggerSource};
    use serde_json::json;

    fn endpoint_params(endpoint_id: &str, body: &str) -> serde_json::Value {
        json!({
            "notification_id": "11111111-1111-4111-8111-111111111111",
            "endpoint_id": endpoint_id,
            "label": "wire-label-ignored",
            "mode": "summary",
            "content_type": "application/json",
            "body": body,
            "received_at": "2026-06-07T00:00:00Z",
            "_meta": { "pie_dedup_key": "11111111-1111-4111-8111-111111111111" }
        })
    }

    #[test]
    fn owned_endpoint_message_maps_to_shared_trigger() {
        let registry = EndpointRegistry::new();
        registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");

        let trigger = map_endpoint_message(
            "pie-hub",
            &endpoint_params("ep-1", "{\"build\":42}"),
            &registry,
        )
        .expect("owned message maps");

        assert!(matches!(
            trigger.source,
            TriggerSource::Mcp { ref server_name, ref method }
                if server_name == "pie-hub" && method == "notifications/endpoint_message"
        ));
        assert_eq!(trigger.source_kind, SourceKind::Mcp);
        assert_eq!(trigger.payload_visibility, PayloadVisibility::Shared);
        assert_eq!(trigger.replacement_policy, ReplacementPolicy::Drop);
        assert_eq!(
            trigger.idempotency_key,
            "mcp:pie-hub:endpoint:11111111-1111-4111-8111-111111111111"
        );
        let payload = trigger.payload.expect("shared payload");
        assert_eq!(
            payload.get("body").and_then(|v| v.as_str()),
            Some("{\"build\":42}")
        );
        // Display fields come from the LOCAL binding, not the wire frame.
        assert_eq!(payload.get("label").and_then(|v| v.as_str()), Some("ci"));
        assert_eq!(payload.get("mode").and_then(|v| v.as_str()), Some("run"));
        let summary = trigger.payload_summary.expect("summary");
        assert!(summary.contains("endpoint ci"), "{summary}");
    }

    #[test]
    fn foreign_endpoint_message_is_ignored() {
        let registry = EndpointRegistry::new();
        registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");
        assert!(
            map_endpoint_message("pie-hub", &endpoint_params("ep-other", "x"), &registry)
                .is_none(),
            "foreign endpoint_id must not produce a trigger"
        );
    }

    #[test]
    fn endpoint_summary_is_redacted_but_payload_body_is_verbatim() {
        let registry = EndpointRegistry::new();
        registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");
        let body = "deploy hub_agent_secret_should_not_persist now";
        let trigger =
            map_endpoint_message("pie-hub", &endpoint_params("ep-1", body), &registry)
                .expect("maps");
        let summary = trigger.payload_summary.unwrap();
        assert!(
            !summary.contains("hub_agent_secret_should_not_persist"),
            "audit summary must redact token-like text: {summary}"
        );
        // The Shared payload carries the verbatim body for the agent prompt.
        assert_eq!(
            trigger.payload.unwrap().get("body").and_then(|v| v.as_str()),
            Some(body)
        );
    }

    #[test]
    fn endpoint_message_without_required_fields_is_ignored() {
        let registry = EndpointRegistry::new();
        registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");
        assert!(map_endpoint_message("pie-hub", &json!({}), &registry).is_none());
        assert!(
            map_endpoint_message("pie-hub", &json!({ "endpoint_id": "ep-1" }), &registry)
                .is_none(),
            "missing notification_id must not map"
        );
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p pie-coding-agent triggers::endpoint`
Expected: FAIL to compile — `map_endpoint_message` not found

- [ ] **Step 3: Implement `map_endpoint_message` in `triggers/endpoint.rs`**

Add imports at the top of the file:

```rust
use pie_agent_core::{
    CredentialScope, PayloadVisibility, ReplacementPolicy, SourceKind, Trigger, TriggerAuthority,
    TriggerSource,
};

use super::mcp_notification_hook::{safe_display, safe_idempotency_segment};
```

And the function:

```rust
/// Map one `notifications/endpoint_message` params object to a runtime `Trigger`,
/// gated on session ownership. Returns `None` for frames whose `endpoint_id` is not
/// bound to this session (another pie process owns them — leave the hub backlog row
/// for it) and for malformed frames.
///
/// First-class hub frame: unlike generic custom notifications, the body is *meant*
/// for the agent, so it travels verbatim in the `Shared` payload. The persisted audit
/// summary stays bounded + redacted like every other summary.
pub fn map_endpoint_message(
    server_name: &str,
    params: &serde_json::Value,
    registry: &EndpointRegistry,
) -> Option<Trigger> {
    let endpoint_id = params.get("endpoint_id")?.as_str()?;
    let binding = registry.owns(endpoint_id)?;
    let notification_id = params.get("notification_id")?.as_str()?;
    let body = params.get("body").and_then(|v| v.as_str()).unwrap_or("");
    let content_type = params
        .get("content_type")
        .and_then(|v| v.as_str())
        .unwrap_or("application/octet-stream");
    let received_at = params
        .get("received_at")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    Some(Trigger {
        source: TriggerSource::Mcp {
            server_name: server_name.to_string(),
            method: "notifications/endpoint_message".to_string(),
        },
        source_kind: SourceKind::Mcp,
        source_label: format!("mcp:{server_name}"),
        event_label: format!("endpoint {}", binding.label),
        payload_visibility: PayloadVisibility::Shared,
        payload_summary: Some(format!(
            "endpoint {}: {}",
            binding.label,
            safe_display(body, 200)
        )),
        payload: Some(serde_json::json!({
            "endpoint_id": endpoint_id,
            "notification_id": notification_id,
            "label": binding.label,
            "mode": binding.mode.as_str(),
            "content_type": content_type,
            "body": body,
            "received_at": received_at,
        })),
        idempotency_key: format!(
            "mcp:{server_name}:endpoint:{}",
            safe_idempotency_segment(notification_id)
        ),
        replacement_policy: ReplacementPolicy::Drop,
        trace_id: Uuid::new_v4().to_string(),
        authority: TriggerAuthority {
            principal_id: format!("mcp:{server_name}:endpoint:{endpoint_id}"),
            principal_label: format!("endpoint {}", binding.label),
            credential_scope: CredentialScope::User,
            allowed_source_actions: Vec::new(),
            expires_at: None,
        },
        received_at: Utc::now(),
    })
}
```

- [ ] **Step 4: Expose the two helpers and route the method in `mcp_notification_hook.rs`**

Change visibility of the existing helpers (no behavior change):

```rust
pub(crate) fn safe_display(value: &str, cap: usize) -> String {
```

```rust
pub(crate) fn safe_idempotency_segment(value: &str) -> String {
```

In `map_notification` (~line 182), add as the FIRST statement of the function body:

```rust
    // Endpoint pushes are first-class hub frames with their own ownership gate,
    // summary rule, and Shared payload — handled apart from the custom-notification
    // privacy path (RFC 1 §4.2.3) on purpose.
    if n.method == "notifications/endpoint_message" {
        return crate::triggers::endpoint::map_endpoint_message(
            server_name,
            &n.params,
            crate::triggers::endpoint::global_endpoint_registry(),
        );
    }
```

Note: tests exercise `map_endpoint_message` with a local registry; do NOT mutate `global_endpoint_registry()` in tests (it is process-global and tests run in parallel). The `map_notification` route against the (empty) global registry simply yields `None` — covered implicitly by the existing drop-path tests.

- [ ] **Step 5: Run tests**

Run: `cargo test -p pie-coding-agent triggers::`
Expected: PASS (endpoint tests + all pre-existing mcp_notification_hook tests)

- [ ] **Step 6: Commit**

```bash
git add crates/coding-agent/src/triggers/endpoint.rs crates/coding-agent/src/triggers/mcp_notification_hook.rs
git commit -m "pie: map endpoint_message frames to ownership-gated triggers"
```

---

## Task 8: pie — endpoint action hook (per-endpoint delivery + ack)

**Files:**
- Modify: `crates/coding-agent/src/triggers/endpoint.rs`
- Modify: `crates/coding-agent/src/triggers/mod.rs` (re-exports)

- [ ] **Step 1: Write failing tests** (append to `triggers/endpoint.rs` tests; model after `dynamic.rs::tests::direct_inject_action_hook_routes_built_in_hub_by_live_mode`)

```rust
    use pie_agent_core::{
        BeforeTriggerActionContext, PromoteAction, TriggerDelivery, TriggerRuntimeSnapshot,
    };
    use tokio_util::sync::CancellationToken;

    fn endpoint_ctx(registry: &EndpointRegistry, endpoint_id: &str, body: &str) -> BeforeTriggerActionContext {
        let trigger = map_endpoint_message(
            crate::config::HUB_SERVER_NAME,
            &endpoint_params(endpoint_id, body),
            registry,
        )
        .expect("trigger maps");
        BeforeTriggerActionContext {
            trigger,
            runtime: TriggerRuntimeSnapshot {
                dedup_entries: 0,
                active_traces: 0,
                accepted_total: 0,
                deduped_total: 0,
                cycle_suppressed_total: 0,
            },
        }
    }

    fn recording_acker() -> (EndpointAcker, Arc<Mutex<Vec<String>>>) {
        let acked = Arc::new(Mutex::new(Vec::<String>::new()));
        let sink = acked.clone();
        let acker: EndpointAcker = Arc::new(move |id: String| {
            sink.lock().push(id);
        });
        (acker, acked)
    }

    fn fallthrough_inner() -> pie_agent_core::BeforeTriggerActionHook {
        Arc::new(|ctx: BeforeTriggerActionContext, _cancel: CancellationToken| {
            Box::pin(async move { pie_agent_core::TriggerAction::default_for(&ctx.trigger) })
        })
    }

    #[tokio::test]
    async fn run_mode_injects_body_and_acks() {
        let registry = EndpointRegistry::new();
        registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");
        let (acker, acked) = recording_acker();
        let hook = endpoint_action_hook(registry.clone(), acker, fallthrough_inner());

        let action = hook(endpoint_ctx(&registry, "ep-1", "deploy now"), CancellationToken::new()).await;

        assert!(matches!(action.delivery, TriggerDelivery::InjectAndRun));
        assert!(action.prompt.contains("deploy now"), "{}", action.prompt);
        assert!(action.prompt.contains("endpoint ci"), "{}", action.prompt);
        assert_eq!(
            acked.lock().clone(),
            vec!["11111111-1111-4111-8111-111111111111".to_string()]
        );
    }

    #[tokio::test]
    async fn summary_mode_promotes_summary_without_model_turn() {
        let registry = EndpointRegistry::new();
        registry
            .add_binding(binding("ep-1", EndpointMode::Summary))
            .expect("add");
        let (acker, acked) = recording_acker();
        let hook = endpoint_action_hook(registry.clone(), acker, fallthrough_inner());

        let action = hook(endpoint_ctx(&registry, "ep-1", "fyi"), CancellationToken::new()).await;

        assert!(matches!(action.delivery, TriggerDelivery::InjectSummary));
        assert!(matches!(
            action.promote,
            PromoteAction::PromoteSummaryNow { .. }
        ));
        assert_eq!(acked.lock().len(), 1);
    }

    #[tokio::test]
    async fn non_endpoint_triggers_fall_through_without_ack() {
        let registry = EndpointRegistry::new();
        let (acker, acked) = recording_acker();
        let hook = endpoint_action_hook(registry, acker, fallthrough_inner());

        // A plain hub agent_message trigger — must reach the inner hook untouched.
        let other_registry = EndpointRegistry::new();
        other_registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");
        let mut ctx = endpoint_ctx(&other_registry, "ep-1", "x");
        if let TriggerSource::Mcp { method, .. } = &mut ctx.trigger.source {
            *method = "notifications/agent_message".to_string();
        }

        let action = hook(ctx, CancellationToken::new()).await;
        assert!(matches!(action.delivery, TriggerDelivery::SubAgent));
        assert!(acked.lock().is_empty());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p pie-coding-agent triggers::endpoint`
Expected: FAIL to compile — `EndpointAcker` / `endpoint_action_hook` not found

- [ ] **Step 3: Implement in `triggers/endpoint.rs`**

Add imports:

```rust
use pie_agent_core::{
    BeforeTriggerActionContext, BeforeTriggerActionHook, PromoteAction, TriggerAction,
    TriggerDelivery,
};
use tokio_util::sync::CancellationToken;
```

And the hook:

```rust
/// Acknowledge an endpoint notification back to the hub. Injected as a closure so the
/// hook stays unit-testable without a network; `main.rs` passes a closure that spawns a
/// `HubClient::ack_notifications` call.
pub type EndpointAcker = Arc<dyn Fn(String) + Send + Sync>;

/// Route endpoint-message triggers by their per-endpoint mode, bypassing the server-level
/// `inject_summary` / `inject_and_run` classification in `direct_inject_action_hook`.
/// Everything else falls through to `inner`. Acks fire here — the moment the owning
/// session accepts the message — so the hub backlog stops replaying it.
pub fn endpoint_action_hook(
    registry: EndpointRegistry,
    acker: EndpointAcker,
    inner: BeforeTriggerActionHook,
) -> BeforeTriggerActionHook {
    Arc::new(
        move |ctx: BeforeTriggerActionContext, cancel: CancellationToken| {
            let is_endpoint = matches!(
                &ctx.trigger.source,
                pie_agent_core::TriggerSource::Mcp { server_name, method }
                    if server_name == crate::config::HUB_SERVER_NAME
                        && method == "notifications/endpoint_message"
            );
            if !is_endpoint {
                return inner(ctx, cancel);
            }
            let payload = ctx.trigger.payload.clone().unwrap_or(serde_json::Value::Null);
            let endpoint_id = payload
                .get("endpoint_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let Some(binding) = registry.owns(endpoint_id) else {
                // The adapter already ownership-gated; defensive fall-through only.
                return inner(ctx, cancel);
            };
            if let Some(notification_id) = payload.get("notification_id").and_then(|v| v.as_str()) {
                acker(notification_id.to_string());
            }
            let body = payload
                .get("body")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let received_at = payload
                .get("received_at")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let label = binding.label.clone();
            match binding.mode {
                EndpointMode::Run => Box::pin(async move {
                    TriggerAction {
                        prompt: format!(
                            "[endpoint {label}] message received at {received_at}:\n\n{body}"
                        ),
                        promote: PromoteAction::None,
                        promote_requires_approval: false,
                        delivery: TriggerDelivery::InjectAndRun,
                    }
                }),
                EndpointMode::Summary => {
                    let has_summary = ctx.trigger.payload_summary.is_some();
                    Box::pin(async move {
                        TriggerAction {
                            prompt: String::new(),
                            promote: if has_summary {
                                PromoteAction::PromoteSummaryNow {
                                    template_body: Some(
                                        "{{trigger.payload_summary}}".to_string(),
                                    ),
                                }
                            } else {
                                PromoteAction::None
                            },
                            promote_requires_approval: false,
                            delivery: TriggerDelivery::InjectSummary,
                        }
                    })
                }
            }
        },
    )
}
```

Add to the `triggers/mod.rs` endpoint re-export list: `EndpointAcker, endpoint_action_hook`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p pie-coding-agent triggers::endpoint`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/coding-agent/src/triggers/endpoint.rs crates/coding-agent/src/triggers/mod.rs
git commit -m "pie: per-endpoint delivery action hook with hub ack"
```

---

## Task 9: pie — HubClient endpoint + ack calls

**Files:**
- Modify: `crates/coding-agent/src/hub_client.rs`

- [ ] **Step 1: Extend `HubInboxItem`** (~line 73) — both new fields default so older hubs keep parsing:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct HubInboxItem {
    #[serde(default)]
    pub notification_id: Option<String>,
    pub sender: String,
    pub summary: String,
    pub payload_visibility: String,
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
    pub first_contact_required: bool,
    pub status: String,
    pub created_at: String,
}
```

- [ ] **Step 2: Add receipt types** (next to `HubSendReceipt`, ~line 83):

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct HubEndpointReceipt {
    pub endpoint_id: String,
    pub url: String,
    pub label: String,
    pub mode: String,
}
```

- [ ] **Step 3: Add client methods** (inside `impl HubClient`, after `list_inbox` ~line 194):

```rust
    pub async fn register_endpoint(&self, label: &str, mode: &str) -> Result<HubEndpointReceipt> {
        self.call_json(
            "register_endpoint",
            json!({ "label": label, "mode": mode }),
        )
        .await
    }

    pub async fn revoke_endpoint(&self, endpoint_id: &str) -> Result<()> {
        #[derive(Deserialize)]
        struct Response {
            #[allow(dead_code)]
            revoked: bool,
        }
        let _: Response = self
            .call_json("revoke_endpoint", json!({ "endpoint_id": endpoint_id }))
            .await?;
        Ok(())
    }

    pub async fn ack_notifications(&self, notification_ids: &[String]) -> Result<()> {
        #[derive(Deserialize)]
        struct Response {
            #[allow(dead_code)]
            acked_notification_ids: Vec<String>,
        }
        let _: Response = self
            .call_json(
                "ack_notification",
                json!({ "notification_ids": notification_ids }),
            )
            .await?;
        Ok(())
    }

    /// Inbox page for backlog replay. Unlike `list_inbox` (display-clamped to 10), this
    /// uses the hub's real page cap.
    pub async fn list_inbox_backlog(&self, limit: usize) -> Result<Vec<HubInboxItem>> {
        let page: Page<HubInboxItem> = self
            .call_json("list_my_inbox", json!({ "limit": limit.clamp(1, 100) }))
            .await?;
        Ok(page.items)
    }
```

- [ ] **Step 4: Verify compile + existing tests** (these methods are a thin typed boundary; correctness is covered by the hub-side tests and the network is off-limits in Rust tests)

Run: `cargo test -p pie-coding-agent hub_client`
Expected: PASS (existing mention-parser tests; crate compiles)

- [ ] **Step 5: Commit**

```bash
git add crates/coding-agent/src/hub_client.rs
git commit -m "pie: hub client endpoint register/revoke/ack/backlog calls"
```

---

## Task 10: pie — backlog replay hook

**Files:**
- Modify: `crates/coding-agent/src/triggers/endpoint.rs`
- Modify: `crates/coding-agent/src/triggers/mod.rs` (re-export `EndpointBacklogHook`)

- [ ] **Step 1: Write failing tests for the pure replay conversion** (append to tests module)

```rust
    #[test]
    fn replay_params_rebuilds_sse_shape_from_inbox_payload() {
        let payload = json!({
            "endpoint_id": "ep-1",
            "label": "ci",
            "mode": "run",
            "content_type": "text/plain",
            "body": "backlogged",
            "received_at": "2026-06-07T00:00:00Z"
        });
        let params = replay_params("22222222-2222-4222-8222-222222222222", &payload)
            .expect("payload converts");
        assert_eq!(
            params.get("notification_id").and_then(|v| v.as_str()),
            Some("22222222-2222-4222-8222-222222222222")
        );
        assert_eq!(params.get("body").and_then(|v| v.as_str()), Some("backlogged"));
        assert_eq!(params.get("endpoint_id").and_then(|v| v.as_str()), Some("ep-1"));

        // The rebuilt params feed straight into the live mapping path.
        let registry = EndpointRegistry::new();
        registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");
        let trigger =
            map_endpoint_message("pie-hub", &params, &registry).expect("replay maps");
        assert_eq!(
            trigger.idempotency_key,
            "mcp:pie-hub:endpoint:22222222-2222-4222-8222-222222222222"
        );
    }

    #[test]
    fn replay_params_rejects_non_endpoint_payload() {
        assert!(replay_params("id-1", &json!({ "something": "else" })).is_none());
        assert!(replay_params("id-1", &json!("not an object")).is_none());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p pie-coding-agent triggers::endpoint`
Expected: FAIL to compile — `replay_params` not found

- [ ] **Step 3: Implement `replay_params` + `EndpointBacklogHook`**

Add imports:

```rust
use async_trait::async_trait;
use pie_agent_core::{HookError, HookState, NotificationHook, NotificationHookStatus, TriggerSink};
```

Implementation:

```rust
/// Rebuild SSE-frame-shaped params from an inbox item's Shared payload so backlog
/// replay reuses the exact live mapping path (`map_endpoint_message`).
pub fn replay_params(
    notification_id: &str,
    payload: &serde_json::Value,
) -> Option<serde_json::Value> {
    let endpoint_id = payload.get("endpoint_id")?.as_str()?;
    Some(serde_json::json!({
        "notification_id": notification_id,
        "endpoint_id": endpoint_id,
        "label": payload.get("label"),
        "mode": payload.get("mode"),
        "content_type": payload.get("content_type"),
        "body": payload.get("body"),
        "received_at": payload.get("received_at"),
    }))
}

/// One-shot `NotificationHook`: on session start, pull the hub inbox backlog and inject
/// any un-acked endpoint messages this session owns. Acks happen downstream in
/// `endpoint_action_hook` — the same path live SSE messages take — so a message is only
/// acked once the owning session accepts it. Foreign endpoint messages are skipped and
/// stay in the backlog for their owner.
pub struct EndpointBacklogHook {
    registry: EndpointRegistry,
    status: Arc<Mutex<NotificationHookStatus>>,
}

impl EndpointBacklogHook {
    pub fn new(registry: EndpointRegistry) -> Self {
        let mut status = NotificationHookStatus::pending();
        status.subscription_labels = vec!["hub:endpoint-backlog".into()];
        Self {
            registry,
            status: Arc::new(Mutex::new(status)),
        }
    }
}

#[async_trait]
impl NotificationHook for EndpointBacklogHook {
    fn label(&self) -> &str {
        "hub:endpoint-backlog"
    }

    async fn run(&self, sink: TriggerSink) -> Result<(), HookError> {
        self.status.lock().state = HookState::Connected;
        let client = match crate::hub_client::HubClient::connect_default().await {
            Ok(client) => client,
            Err(e) => {
                // No hub credential / hub unreachable — replay is best-effort.
                self.status.lock().state = HookState::Disconnected {
                    reason: format!("backlog replay skipped: {e}"),
                };
                return Ok(());
            }
        };
        let items = match client.list_inbox_backlog(100).await {
            Ok(items) => items,
            Err(e) => {
                client.close().await;
                self.status.lock().state = HookState::Disconnected {
                    reason: format!("backlog list failed: {e}"),
                };
                return Ok(());
            }
        };
        client.close().await;
        let mut replayed = 0usize;
        for item in items {
            let Some(notification_id) = item.notification_id.as_deref() else {
                continue;
            };
            let Some(payload) = item.payload.as_ref() else {
                continue;
            };
            let Some(params) = replay_params(notification_id, payload) else {
                continue;
            };
            let Some(trigger) =
                map_endpoint_message(crate::config::HUB_SERVER_NAME, &params, &self.registry)
            else {
                continue; // foreign or malformed — leave in backlog
            };
            if sink.send(trigger).is_err() {
                self.status.lock().state = HookState::Disconnected {
                    reason: "sink closed".into(),
                };
                return Err(HookError::SinkClosed);
            }
            replayed += 1;
        }
        let mut status = self.status.lock();
        status.last_event_at = Some(Utc::now());
        status.state = HookState::Disconnected {
            reason: format!("backlog replay complete ({replayed} message(s))"),
        };
        Ok(())
    }

    fn status(&self) -> NotificationHookStatus {
        self.status.lock().clone()
    }
}
```

Add `EndpointBacklogHook, replay_params` to the `triggers/mod.rs` endpoint re-export list.

Note: `run()` is not unit-tested (it requires hub credentials and CI clears all keys; a missing credential makes it exit cleanly). The conversion logic it delegates to (`replay_params` + `map_endpoint_message`) is fully covered above.

- [ ] **Step 4: Run tests**

Run: `cargo test -p pie-coding-agent triggers::endpoint`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/coding-agent/src/triggers/endpoint.rs crates/coding-agent/src/triggers/mod.rs
git commit -m "pie: endpoint backlog replay hook"
```

---

## Task 11: pie — `/endpoint` slash command

**Files:**
- Modify: `crates/coding-agent/src/commands.rs`

- [ ] **Step 1: Write failing tests for argument parsing** (in the existing `commands.rs` tests module — find `#[cfg(test)] mod tests` near the file bottom and append)

```rust
    #[test]
    fn endpoint_register_args_parse_label_and_mode() {
        use crate::triggers::EndpointMode;

        let default = parse_endpoint_register_args(&[]).expect("defaults");
        assert_eq!(default.label, "default");
        assert_eq!(default.mode, EndpointMode::Run);

        let labeled =
            parse_endpoint_register_args(&["github-hooks".into()]).expect("label only");
        assert_eq!(labeled.label, "github-hooks");
        assert_eq!(labeled.mode, EndpointMode::Run);

        let full = parse_endpoint_register_args(&[
            "ci".into(),
            "--mode".into(),
            "summary".into(),
        ])
        .expect("label + mode");
        assert_eq!(full.label, "ci");
        assert_eq!(full.mode, EndpointMode::Summary);

        assert!(parse_endpoint_register_args(&["--mode".into()]).is_err());
        assert!(
            parse_endpoint_register_args(&["--mode".into(), "shout".into()]).is_err()
        );
        assert!(
            parse_endpoint_register_args(&["a".into(), "b".into()]).is_err(),
            "second positional arg must be rejected"
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pie-coding-agent commands::tests::endpoint_register_args_parse_label_and_mode`
Expected: FAIL to compile — `parse_endpoint_register_args` not found

- [ ] **Step 3: Implement the command** (place after the `HubCommand` block, ~line 1296; register in `with_builtins` after `r.register(Arc::new(HubCommand));`)

```rust
        r.register(Arc::new(EndpointCommand));
```

```rust
struct EndpointCommand;

#[async_trait]
impl SlashCommand for EndpointCommand {
    fn name(&self) -> &'static str {
        "endpoint"
    }

    fn description(&self) -> &'static str {
        "register this session as a public hub webhook endpoint"
    }

    fn usage(&self) -> &'static str {
        "[list|register [label] [--mode run|summary]|revoke <endpoint-id>]"
    }

    async fn run(&self, argv: &[String], _ctx: &CommandCtx<'_>) -> CommandOutcome {
        match argv.first().map(String::as_str) {
            None | Some("list") => endpoint_list(),
            Some("register") => endpoint_register(&argv[1..]).await,
            Some("revoke") => endpoint_revoke(&argv[1..]).await,
            Some(_) => CommandOutcome::Error(
                "usage: /endpoint [list|register [label] [--mode run|summary]|revoke <endpoint-id>]"
                    .into(),
            ),
        }
    }
}

fn endpoint_list() -> CommandOutcome {
    let bindings = crate::triggers::global_endpoint_registry().list();
    if bindings.is_empty() {
        cprintln!("no endpoints registered for this session; run /endpoint register");
        return CommandOutcome::Handled;
    }
    for binding in bindings {
        cprintln!(
            "{} [{}] {} {}",
            binding.endpoint_id,
            binding.mode.as_str(),
            binding.label,
            binding.url
        );
    }
    CommandOutcome::Handled
}

pub(crate) struct EndpointRegisterArgs {
    pub(crate) label: String,
    pub(crate) mode: crate::triggers::EndpointMode,
}

pub(crate) fn parse_endpoint_register_args(
    args: &[String],
) -> Result<EndpointRegisterArgs, String> {
    let mut label: Option<String> = None;
    let mut mode = crate::triggers::EndpointMode::Run;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" => {
                let Some(value) = args.get(i + 1) else {
                    return Err("usage: /endpoint register [label] [--mode run|summary]".into());
                };
                mode = crate::triggers::EndpointMode::parse(value)
                    .ok_or_else(|| format!("unknown mode `{value}`; use run or summary"))?;
                i += 2;
            }
            other if label.is_none() && !other.starts_with("--") => {
                label = Some(other.to_string());
                i += 1;
            }
            other => return Err(format!("unexpected argument `{other}`")),
        }
    }
    Ok(EndpointRegisterArgs {
        label: label.unwrap_or_else(|| "default".into()),
        mode,
    })
}

async fn endpoint_register(args: &[String]) -> CommandOutcome {
    let parsed = match parse_endpoint_register_args(args) {
        Ok(parsed) => parsed,
        Err(e) => return CommandOutcome::Error(e),
    };
    let client = match crate::hub_client::HubClient::connect_default().await {
        Ok(client) => client,
        Err(e) => {
            return CommandOutcome::Error(format!(
                "hub connection failed: {e}; run /hub join first"
            ));
        }
    };
    let receipt = match client
        .register_endpoint(&parsed.label, parsed.mode.as_str())
        .await
    {
        Ok(receipt) => receipt,
        Err(e) => {
            client.close().await;
            return CommandOutcome::Error(format!("register endpoint failed: {e}"));
        }
    };
    client.close().await;
    let binding = crate::triggers::EndpointBinding {
        endpoint_id: receipt.endpoint_id.clone(),
        label: receipt.label.clone(),
        mode: parsed.mode,
        url: receipt.url.clone(),
        created_at: chrono::Utc::now(),
    };
    if let Err(e) = crate::triggers::global_endpoint_registry().add_binding(binding) {
        return CommandOutcome::Error(format!(
            "endpoint registered on hub but the local binding failed to save: {e}; \
             revoke it with /endpoint revoke {}",
            receipt.endpoint_id
        ));
    }
    cprintln!("endpoint registered: {} ({})", receipt.endpoint_id, receipt.label);
    cprintln!("public URL (anyone holding it can POST into this session):");
    cprintln!("{}", receipt.url);
    cprintln!(
        "mode: {} — messages are {} this session",
        parsed.mode.as_str(),
        match parsed.mode {
            crate::triggers::EndpointMode::Run => "injected and run by",
            crate::triggers::EndpointMode::Summary => "injected as summaries into",
        }
    );
    CommandOutcome::Handled
}

async fn endpoint_revoke(args: &[String]) -> CommandOutcome {
    let Some(endpoint_id) = args.first() else {
        return CommandOutcome::Error("usage: /endpoint revoke <endpoint-id>".into());
    };
    let client = match crate::hub_client::HubClient::connect_default().await {
        Ok(client) => client,
        Err(e) => {
            return CommandOutcome::Error(format!(
                "hub connection failed: {e}; run /hub join first"
            ));
        }
    };
    if let Err(e) = client.revoke_endpoint(endpoint_id).await {
        client.close().await;
        return CommandOutcome::Error(format!("revoke failed: {e}"));
    }
    client.close().await;
    match crate::triggers::global_endpoint_registry().remove_binding(endpoint_id) {
        Ok(Some(_)) => cprintln!("endpoint {endpoint_id} revoked and unbound from this session"),
        Ok(None) => cprintln!(
            "endpoint {endpoint_id} revoked on hub (it was not bound to this session)"
        ),
        Err(e) => {
            return CommandOutcome::Error(format!(
                "revoked on hub but local binding removal failed: {e}"
            ));
        }
    }
    CommandOutcome::Handled
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p pie-coding-agent commands::`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/coding-agent/src/commands.rs
git commit -m "pie: add /endpoint slash command"
```

---

## Task 12: pie — main.rs wiring

**Files:**
- Modify: `crates/coding-agent/src/main.rs`

No new unit tests — this task is pure composition of pieces tested in Tasks 6–11; the build + full suite is the check.

- [ ] **Step 1: Load the sidecar** (after the cron load, ~line 424):

```rust
    let endpoint_registry = triggers::global_endpoint_registry().clone();
    let endpoint_path = session::endpoint_sidecar_path_for_session(&session, &repo).await?;
    let endpoint_load_error = endpoint_registry.load_from_path(endpoint_path).err();
```

- [ ] **Step 2: Define the acker and wrap the action-hook chain** (~line 614). The endpoint hook sits INSIDE cron but OUTSIDE `direct_inject_action_hook`, so per-endpoint mode wins over the server-level hub inject mode:

```rust
    // Endpoint messages ack back to the hub the moment this session accepts them, so the
    // backlog stops replaying them on the next reconnect. Fire-and-forget: a failed ack
    // just means one redundant replay later.
    let endpoint_acker: triggers::EndpointAcker = std::sync::Arc::new(|notification_id: String| {
        tokio::spawn(async move {
            match hub_client::HubClient::connect_default().await {
                Ok(client) => {
                    if let Err(e) = client.ack_notifications(&[notification_id]).await {
                        tracing::warn!("endpoint ack failed: {e}");
                    }
                    client.close().await;
                }
                Err(e) => tracing::warn!("endpoint ack skipped: {e}"),
            }
        });
    });
    opts.before_trigger_action = Some(triggers::cron_action_hook(
        cron_registry.clone(),
        triggers::endpoint_action_hook(
            endpoint_registry.clone(),
            endpoint_acker,
            triggers::direct_inject_action_hook(
                mcp_inject_summary_servers,
                mcp_inject_and_run_servers,
                triggers::before_trigger_action_hook(dynamic_trigger_registry.clone()),
            ),
        ),
    ));
```

(Replaces the existing `opts.before_trigger_action = ...` block at line 614–621. If `tracing` is not already imported in main.rs scope, use the fully qualified `tracing::warn!` as written — the crate already depends on tracing via `logging::init`.)

- [ ] **Step 3: Register the backlog replay hook** (after the `DynamicTriggerCheckHook` registration, ~line 666):

```rust
    // One-shot endpoint backlog replay: messages POSTed while this session was offline
    // are pulled from the hub inbox and re-injected via the live mapping path. No-op
    // when this session has no endpoint bindings or no hub credential.
    if !endpoint_registry.list().is_empty() {
        harness.register_notification_hook(std::sync::Arc::new(
            triggers::EndpointBacklogHook::new(endpoint_registry.clone()),
        ));
    }
```

- [ ] **Step 4: Surface the load status** (after the cron load-status block, ~line 789):

```rust
    if let Some(err) = &endpoint_load_error {
        app.error_line(format!("endpoints: {err}"));
    } else if !endpoint_registry.list().is_empty() {
        let location = endpoint_registry
            .storage_path()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "memory".into());
        app.system_line(format!(
            "loaded {} endpoint binding(s) from {}",
            endpoint_registry.list().len(),
            location
        ));
    }
```

- [ ] **Step 5: Build and run the full crate suite**

Run: `cargo test -p pie-coding-agent`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/coding-agent/src/main.rs
git commit -m "pie: wire endpoint registry, action hook, and backlog replay"
```

---

## Task 13: Docs, changelog, full CI

**Files:**
- Create: `docs/endpoints.md`
- Modify: `CHANGELOG.md`, `CLAUDE.md` (one paragraph in the Triggers section)

- [ ] **Step 1: Write `docs/endpoints.md`**

```markdown
# Public webhook endpoints

Register a pie session as a public HTTP endpoint on the hub. External callers POST to
the URL; the message travels hub → SSE → trigger runtime into exactly that session.

## Usage

Inside a session (requires `/hub join` first):

    /endpoint register ci-alerts            # mode defaults to run
    /endpoint register fyi --mode summary
    /endpoint list
    /endpoint revoke <endpoint-id>

`register` prints the public URL once, e.g. `https://pie.0xfefe.me/e/hub_ep_…`. The URL
itself is the credential (capability token): anyone holding it can POST. It is stored in
the session sidecar `<session>.endpoints.json` next to the transcript, so the binding —
and delivery into this exact session — survives `--resume`.

External callers POST anything (JSON or text, ≤ 64 KB):

    curl -X POST -H 'content-type: application/json' \
      -d '{"build": 42, "status": "red"}' \
      https://pie.0xfefe.me/e/hub_ep_…

Responses: `202 {ok, id}` accepted (always, even when the session is offline — the hub
backlogs and the session replays on resume); `404` unknown or revoked token; `413` over
64 KB; `429` over 120 requests/minute per endpoint.

## Delivery modes

- `run` (default): the message body is injected into the session chat and the agent runs
  one turn to react to it.
- `summary`: the message is injected as a chat line only; no model call.

## Offline behavior

Messages POSTed while the session is offline stay in the hub backlog (un-acked
notifications). When the session resumes (or pie restarts), a one-shot replay hook pulls
the backlog and injects owned endpoint messages in order. Un-acked endpoint messages
older than 7 days are dropped lazily. Multiple sessions of the same hub agent can hold
different endpoints; each message is delivered to (and acked by) the owning session only.

## Security notes

- The URL is shown once at registration; the hub stores only a SHA-256 hash.
- Revocation (`/endpoint revoke`) is immediate.
- The unguessable-URL model fits webhook senders that cannot set custom headers. Treat
  the URL like a password; re-register to rotate.
```

- [ ] **Step 2: CHANGELOG entry**

Add at the top of `CHANGELOG.md`, matching the existing entry format (inspect the first few lines and mirror them):

```markdown
- Public webhook endpoints: `/endpoint register` mints a hub capability URL
  (`https://pie.0xfefe.me/e/<token>`); external POSTs inject into the owning session
  (run/summary modes), with hub backlog replay on resume. (docs/endpoints.md)
```

- [ ] **Step 3: CLAUDE.md** — append one paragraph at the end of the `## Triggers` section:

```markdown
Public webhook endpoints (`/endpoint register`) are a hub-mediated trigger source: the
hub mints a capability URL (`POST /e/<token>`, route + tools in `workers/fefe-hub`),
stores inbound messages as `sender_namespace = "endpoint"` notification rows, and pushes
`notifications/endpoint_message` frames. The session-side binding lives in the
`<session>.endpoints.json` sidecar (`triggers/endpoint.rs`); only the owning session maps
the frame to a trigger, delivers per-endpoint (`run` → `InjectAndRun`, `summary` →
`InjectSummary`, bypassing the server-level inject classification), and acks. Un-acked
backlog replays on resume via `EndpointBacklogHook`.
```

- [ ] **Step 4: Full verification**

Run: `make ci`
Expected: PASS (fmt-check + clippy `-D warnings` + workspace tests)

Run: `cd workers/fefe-hub && npm test`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add docs/endpoints.md CHANGELOG.md CLAUDE.md
git commit -m "docs: public webhook endpoints"
```

---

## Post-plan notes for the implementer

- **Type drift watch:** `EndpointMode::as_str()` strings (`"run"`/`"summary"`) must stay aligned with the hub's `endpointModeValue` and the `mode` field in `payload_json` — three places, one vocabulary.
- **Do not** mutate `global_endpoint_registry()` in any test; construct local `EndpointRegistry` instances (tests run in parallel in one process).
- **`pie_agent_core` API check:** `BeforeTriggerActionContext`, `TriggerRuntimeSnapshot`, `TriggerAction::default_for`, `PromoteAction::PromoteSummaryNow`, `NotificationHookStatus::pending()` are all used today in `triggers/dynamic.rs` — if any signature differs at implementation time, mirror the current usage in that file.
- If `wrangler.toml` needs the new migration listed explicitly, check how `0002`/`0003` are registered (the `migrations/` directory convention usually suffices for `wrangler d1 migrations apply`).
```
