//! Phase-1 acceptance tests. Each test runs one of the demo binaries via `cargo run` and
//! asserts the observable acceptance: the binary exits 0, the envelope is rendered, dedup is
//! visible, and the fake hub token never appears in stdout/stderr.

use std::process::Command;

// Resolve the binary paths at compile time via Cargo's CARGO_BIN_EXE_<name> support.
const MCP_PUSH_SMOKE_BIN: &str = env!("CARGO_BIN_EXE_mcp-push-smoke");
const HUB_WS_SMOKE_BIN: &str = env!("CARGO_BIN_EXE_hub-ws-smoke");

const FAKE_TOKEN: &str = "fake-hub-token-should-not-leak";

#[test]
fn mcp_push_smoke_acceptance() {
    let out = Command::new(MCP_PUSH_SMOKE_BIN)
        .output()
        .expect("spawn mcp-push-smoke");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "binary exited non-zero\nstdout:\n{}\nstderr:\n{}",
        stdout,
        stderr
    );
    // Acceptance items.
    assert!(
        stdout.contains("source_kind"),
        "expected envelope render in stdout"
    );
    assert!(stdout.contains("Mcp"), "expected SourceKind::Mcp in output");
    assert!(
        stdout.contains("idempotency_key"),
        "envelope must show idempotency_key"
    );
    assert!(
        stdout.contains("[deduped]"),
        "expected visible dedup output"
    );
    assert!(stdout.contains("OK: mcp-push-smoke phase-1 acceptance passed."));
    assert!(
        !stdout.contains(FAKE_TOKEN) && !stderr.contains(FAKE_TOKEN),
        "redaction breach in mcp-push-smoke: raw fake token visible"
    );
}

#[test]
fn hub_ws_smoke_acceptance() {
    let out = Command::new(HUB_WS_SMOKE_BIN)
        .output()
        .expect("spawn hub-ws-smoke");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "binary exited non-zero\nstdout:\n{}\nstderr:\n{}",
        stdout,
        stderr
    );
    assert!(stdout.contains("source_kind"));
    assert!(stdout.contains("Hub"), "expected SourceKind::Hub in output");
    assert!(stdout.contains("idempotency_key"));
    assert!(
        stdout.contains("[deduped]"),
        "expected visible dedup output"
    );
    assert!(stdout.contains("OK: hub-ws-smoke phase-1 acceptance passed."));
    assert!(
        !stdout.contains(FAKE_TOKEN) && !stderr.contains(FAKE_TOKEN),
        "redaction breach in hub-ws-smoke: raw fake token visible"
    );
}
