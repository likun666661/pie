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
the backlog and injects owned endpoint messages in order. Endpoint messages older than
7 days are dropped lazily. Multiple sessions of the same hub agent can hold different
endpoints; each message is delivered to (and acked by) the owning session only.

## Security notes

- The URL is shown once at registration; the hub stores only a SHA-256 hash.
- Revocation (`/endpoint revoke`) is immediate.
- The unguessable-URL model fits webhook senders that cannot set custom headers. Treat
  the URL like a password; re-register to rotate.
