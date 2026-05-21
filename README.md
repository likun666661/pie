# pie

A Rust port of the [`pi` agent harness](https://github.com/earendil-works/pi) — extensible coding
agent and the LLM-runtime stack underneath it. Cargo workspace, three member crates, layered
strictly one direction:

```
pie/
  Cargo.toml                  ← [workspace] root
  crates/
    ai/             pie-ai             unified streaming LLM client
    agent/          pie-agent-core     stateful agent runtime
    coding-agent/   pie-coding-agent   the `pie` CLI binary
```

## What each crate does

| Crate | Highlights |
|-------|-----------|
| **`pie-ai`** | 10 wire protocols (anthropic / openai-responses / openai-completions / openai-codex / azure / google / google-vertex / amazon-bedrock / mistral + faux); 938-model catalog; Anthropic OAuth PKCE; provider-level HTTP retry (429/5xx/timeout); cross-provider stream-event normalization. |
| **`pie-agent-core`** | Bare `Agent` state machine + agent loop with sequential **and** parallel tool execution; all 4 lifecycle hooks (`before/after_tool_call`, `transform_context`, `should_stop_after_turn`, `prepare_next_turn`); `AgentHarness` with auto-compaction, branch ops, model/thinking switching, prompt templates, skills catalog; JSONL + memory session repos with branching. |
| **`pie-coding-agent`** | The `pie` CLI: line-based REPL TUI (crossterm), 8 tools (read/write/edit/bash/ls/grep/find/memory), JSONL sessions with `--resume`/`--list-sessions`/`--delete-session`, cross-session memory at `~/.pie/memory/`, `AgentSession` LLM-error auto-retry layer (TS regex parity + exponential backoff). |

## Quick start

```bash
# Build everything + run the agent
cargo build --release
export ANTHROPIC_API_KEY=sk-ant-...      # or OPENAI_API_KEY / GROQ_API_KEY / ...
./target/release/pie

# Resume the most recent session for this cwd
./target/release/pie --resume

# Common flags
./target/release/pie --help
./target/release/pie --list-sessions
./target/release/pie --provider anthropic --model claude-haiku-4-5
./target/release/pie --thinking high
```

REPL commands inside the loop: `/help`, `/clear`, `/quit`.

## Build + test

```bash
cargo build --workspace
cargo test --workspace      # 121 tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

## Status

| Crate | Tests | Notes |
|-------|------:|-------|
| pie-ai           | 66 | All 10 providers compile; reqwest-level retry on all of them; Bedrock SigV4 / Vertex ADC / Codex WebSocket are TODO (Bearer-token paths work) |
| pie-agent-core   | 32 | Harness fully ported (auto-compaction, branch ops, model switching, all hooks wired, parallel exec) |
| pie-coding-agent | 23 | 8 tools, jsonl sessions with resume, cross-session memory, auto-retry on LLM errors |

## Configuration paths

`pie` reads/writes:

| Path | What |
|------|------|
| `~/.pie/sessions/<cwd-hash>/<uuidv7>.jsonl` | Per-cwd session files |
| `~/.pie/memory/*.md`                        | Cross-session memory (auto-injected into system prompt) |
| `$PIE_DIR`                                  | Override the base directory |

## Deliberately out of scope (vs the TS reference)

Extensions / skills loader / themes / settings-manager / keybindings; print/json/rpc modes; SDK
as a library; `/login` + auth-storage; export-html; `pi packages`; image generation; the full
TUI (no editor/autocomplete/fuzzy/kill-ring/undo/terminal-image — we use crossterm directly).

## CI / Release

| Workflow | Trigger | Job |
|----------|---------|-----|
| [`.github/workflows/ci.yml`](.github/workflows/ci.yml)           | push / PR to `main` | `cargo fmt --check` + `cargo clippy -D warnings` + `cargo test --workspace` (ubuntu + macos) |
| [`.github/workflows/release.yml`](.github/workflows/release.yml) | tag `v*` (or `workflow_dispatch`) | cross-build for `x86_64-unknown-linux-gnu` / `aarch64-unknown-linux-gnu` / `x86_64-apple-darwin` / `aarch64-apple-darwin`, upload tarballs to GitHub Releases |

## License

[MIT](LICENSE)
