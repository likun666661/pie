//! AgentSession — auto-retry wrapper around [`pie_agent_core::AgentHarness`]. 1:1 port of
//! `packages/coding-agent/src/core/agent-session.ts` retry logic (`_isRetryableError` +
//! `_prepareRetry`).
//!
//! On a retryable LLM error the wrapper:
//! 1. Drops the failed assistant message from agent state (kept in session log).
//! 2. Waits exponentially (`base_delay_ms * 2^(attempt-1)`, capped).
//! 3. Calls `harness.continue_()` to re-run.
//! 4. Up to `max_retries` times. After that, the error surfaces to the caller.
//!
//! Retryable patterns: `overloaded`, `rate.?limit`, `429`, `5xx`, network/connection errors,
//! `ended without`, `stream ended before message_stop`, `terminated`, `retry delay`. Mirrors
//! the TS regex.

use std::sync::Arc;

use once_cell::sync::Lazy;
use pie_agent_core::{AgentEvent, AgentHarness, AgentListener, AgentMessage, AgentRunError};
use pie_ai::{AssistantMessage as PiAssistantMessage, Message as PiMessage};
use regex::Regex;

#[derive(Clone, Debug)]
pub struct RetrySettings {
    pub enabled: bool,
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for RetrySettings {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries: 5,
            base_delay_ms: 1_000,
            max_delay_ms: 60_000,
        }
    }
}

/// Regex matching error messages we should retry. Compiled once.
static RETRYABLE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)overloaded|provider.?returned.?error|rate.?limit|too many requests|429|500|502|503|504|service.?unavailable|server.?error|internal.?error|network.?error|connection.?error|connection.?refused|connection.?lost|websocket.?closed|websocket.?error|other side closed|fetch failed|upstream.?connect|reset before headers|socket hang up|ended without|stream ended before message_stop|http2 request did not get a response|timed? out|timeout|terminated|retry delay",
    )
    .expect("retry regex")
});

pub fn is_retryable_error(err_message: &str) -> bool {
    RETRYABLE_RE.is_match(err_message)
}

/// Lightweight wrapper. Holds the harness + retry settings; not a deep clone of TS
/// `AgentSession` (no extension orchestration, no event-bus fan-out — just retry).
pub struct AgentSession {
    harness: Arc<AgentHarness>,
    settings: RetrySettings,
}

impl AgentSession {
    pub fn new(harness: Arc<AgentHarness>, settings: RetrySettings) -> Self {
        Self { harness, settings }
    }

    #[allow(dead_code)] // public API for embedders; not used by the binary itself.
    pub fn harness(&self) -> &AgentHarness {
        &self.harness
    }

    /// Subscribe to underlying lifecycle events. Useful for UI listeners.
    #[allow(dead_code)] // public API for embedders; not used by the binary itself.
    pub fn subscribe(&self, listener: AgentListener) -> impl FnOnce() {
        self.harness.subscribe(listener)
    }

    /// Prompt with retry. Same signature as `AgentHarness::prompt` but loops on retryable LLM
    /// errors with exponential backoff.
    pub async fn prompt(&self, text: impl Into<String>) -> Result<(), AgentRunError> {
        let text = text.into();
        let mut last_err: Option<AgentRunError> = None;
        let mut attempt: u32 = 0;
        loop {
            let r = if attempt == 0 {
                self.harness.prompt(text.clone()).await
            } else {
                self.harness.continue_().await
            };
            match r {
                Ok(()) => {
                    if !self.is_retryable_assistant(&self.last_assistant()) {
                        return Ok(());
                    }
                    // Successful prompt() can still leave a synthesized error assistant message
                    // (provider stream encoded the error). Re-evaluate via retry policy.
                }
                Err(e) => {
                    last_err = Some(e);
                }
            }

            if !self.settings.enabled || attempt >= self.settings.max_retries {
                return Err(last_err.unwrap_or_else(|| {
                    AgentRunError::Other("retries exhausted with no error captured".into())
                }));
            }

            // Check the latest assistant message to decide retryability.
            let assistant = self.last_assistant();
            let retryable = match (&assistant, &last_err) {
                (Some(a), _) if a.stop_reason == pie_ai::StopReason::Error => a
                    .error_message
                    .as_deref()
                    .map(is_retryable_error)
                    .unwrap_or(false),
                (_, Some(e)) => is_retryable_error(&format!("{e}")),
                _ => false,
            };
            if !retryable {
                return match last_err {
                    Some(e) => Err(e),
                    None => Ok(()),
                };
            }

            attempt += 1;
            let delay_ms = backoff_ms(
                attempt,
                self.settings.base_delay_ms,
                self.settings.max_delay_ms,
            );
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;

            // Drop the failed assistant message from agent state so continue_() doesn't replay
            // a context that ends in an error.
            self.pop_failed_assistant();
            last_err = None;
        }
    }

    fn last_assistant(&self) -> Option<PiAssistantMessage> {
        let s = self.harness.agent().state();
        for m in s.messages.iter().rev() {
            if let AgentMessage::Llm(PiMessage::Assistant(a)) = m {
                return Some(a.clone());
            }
        }
        None
    }

    fn is_retryable_assistant(&self, a: &Option<PiAssistantMessage>) -> bool {
        let Some(a) = a else { return false };
        if a.stop_reason != pie_ai::StopReason::Error {
            return false;
        }
        a.error_message
            .as_deref()
            .map(is_retryable_error)
            .unwrap_or(false)
    }

    fn pop_failed_assistant(&self) {
        let mut s = self.harness.agent().state();
        while let Some(last) = s.messages.last() {
            if matches!(last, AgentMessage::Llm(PiMessage::Assistant(a)) if a.stop_reason == pie_ai::StopReason::Error)
            {
                s.messages.pop();
            } else {
                break;
            }
        }
    }
}

fn backoff_ms(attempt: u32, base: u64, max: u64) -> u64 {
    let n = (base as u128) << attempt.min(10);
    n.min(max as u128) as u64
}

// Lifecycle events the wrapper emits on its own (not via Agent). Consumers wanting visibility
// into retries subscribe to these.
#[allow(dead_code)] // declared for future embedder use; the binary doesn't emit these yet.
#[derive(Clone, Debug)]
pub enum AgentSessionEvent {
    AutoRetryStart {
        attempt: u32,
        max_attempts: u32,
        delay_ms: u64,
        error_message: String,
    },
    AutoRetryEnd {
        success: bool,
        attempt: u32,
        final_error: Option<String>,
    },
}

/// Convert a base `AgentEvent` listener type into the kind we use here for symmetry.
#[allow(dead_code)]
pub fn forward_to_listener(_: AgentEvent) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_patterns_match_ts_regex() {
        assert!(is_retryable_error("overloaded_error"));
        assert!(is_retryable_error(
            "Provider returned error: 429 Too Many Requests"
        ));
        assert!(is_retryable_error("rate limit exceeded"));
        assert!(is_retryable_error("HTTP 503 Service Unavailable"));
        assert!(is_retryable_error("websocket closed"));
        assert!(is_retryable_error("stream ended before message_stop"));
        assert!(is_retryable_error("socket hang up"));
        assert!(is_retryable_error("reset before headers"));
        assert!(!is_retryable_error("bad request: missing field"));
        assert!(!is_retryable_error("Unauthorized"));
        assert!(!is_retryable_error("model not found"));
    }

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff_ms(1, 1000, 60_000), 2000);
        assert_eq!(backoff_ms(2, 1000, 60_000), 4000);
        assert_eq!(backoff_ms(8, 1000, 60_000), 60_000);
    }
}
