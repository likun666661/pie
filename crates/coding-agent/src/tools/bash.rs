//! `bash` tool. Mirrors `packages/coding-agent/src/core/tools/bash.ts`. Runs the command via
//! `sh -c`, captures stdout+stderr, honors an optional timeout (seconds), and honors the
//! agent's cancellation token.
//!
//! Simpler than TS: no temp-file spill on overflow, no environment scrubbing, no shell-shimmed
//! `cd` tracking. Output is tail-truncated so the most recent (and most useful) lines survive.

use async_trait::async_trait;
use pie_agent_core::{AgentTool, AgentToolError, AgentToolResult, AgentToolUpdate};
use pie_ai::{Tool, UserContentBlock};
use serde_json::{Value, json};
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

use super::truncate::{DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES, truncate_tail};

pub struct BashTool;

#[async_trait]
impl AgentTool for BashTool {
    fn definition(&self) -> &Tool {
        &DEFINITION
    }

    fn label(&self) -> &str {
        "bash"
    }

    async fn execute(
        &self,
        _id: &str,
        params: Value,
        cancel: CancellationToken,
        _on_update: Option<AgentToolUpdate>,
    ) -> Result<AgentToolResult, AgentToolError> {
        let command = params
            .get("command")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AgentToolError::from("missing `command`"))?;
        let timeout_secs = params.get("timeout").and_then(|v| v.as_u64());

        let mut child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| AgentToolError::from(format!("spawn: {e}")))?;
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        let collect = async move {
            let mut out = String::new();
            let mut err = String::new();
            if let Some(mut s) = stdout {
                let _ = s.read_to_string(&mut out).await;
            }
            if let Some(mut s) = stderr {
                let _ = s.read_to_string(&mut err).await;
            }
            let status = child.wait().await;
            (out, err, status)
        };

        let race = async {
            tokio::select! {
                v = collect => v,
                _ = cancel.cancelled() => {
                    // Best-effort kill; child has already been moved into `collect`, but if we
                    // got cancelled before collect started reading, the OS will reap the orphan
                    // when sh exits. Acceptable for "simple".
                    (String::new(), "[aborted]".into(), Ok(std::process::ExitStatus::default()))
                }
            }
        };

        let (stdout_s, stderr_s, status) = if let Some(secs) = timeout_secs {
            match timeout(Duration::from_secs(secs), race).await {
                Ok(v) => v,
                Err(_) => (
                    String::new(),
                    format!("[timed out after {secs}s]"),
                    Ok(std::process::ExitStatus::default()),
                ),
            }
        } else {
            race.await
        };

        let exit = status.ok().and_then(|s| s.code()).unwrap_or(-1);
        let (stdout_trim, st) = truncate_tail(&stdout_s, DEFAULT_MAX_LINES, DEFAULT_MAX_BYTES);
        let (stderr_trim, _) = truncate_tail(&stderr_s, DEFAULT_MAX_LINES, DEFAULT_MAX_BYTES);

        let mut text = format!("$ {command}\n");
        if let Some(note) = st.note() {
            text.push_str(&note);
            text.push('\n');
        }
        if !stdout_trim.is_empty() {
            text.push_str(&stdout_trim);
            if !stdout_trim.ends_with('\n') {
                text.push('\n');
            }
        }
        if !stderr_trim.is_empty() {
            text.push_str("[stderr]\n");
            text.push_str(&stderr_trim);
            if !stderr_trim.ends_with('\n') {
                text.push('\n');
            }
        }
        text.push_str(&format!("[exit {exit}]"));

        let is_error = exit != 0;
        Ok(AgentToolResult {
            content: vec![UserContentBlock::text(text)],
            details: json!({
                "command": command,
                "exitCode": exit,
                "isError": is_error,
            }),
            terminate: None,
        })
    }
}

use once_cell::sync::Lazy;
static DEFINITION: Lazy<Tool> = Lazy::new(|| Tool {
    name: "bash".into(),
    description: format!(
        "Run a shell command via `sh -c`. Returns stdout+stderr (tail-truncated to {DEFAULT_MAX_LINES} lines / {} KiB) and exit code. Optional `timeout` in seconds.",
        DEFAULT_MAX_BYTES / 1024
    ),
    parameters: json!({
        "type": "object",
        "properties": {
            "command": { "type": "string", "description": "Shell command to execute" },
            "timeout": { "type": "integer", "description": "Timeout in seconds (optional)" },
        },
        "required": ["command"],
    }),
});
