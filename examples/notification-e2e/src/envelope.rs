//! Phase-1 `DemoTrigger` envelope.
//!
//! Mirrors the shape of `pie_agent_core::Trigger` planned by RFC 1 (#20) so the demo output
//! looks like the real envelope, but lives ONLY in this example crate. Do not import this from
//! production code. Phase 2 will swap consumers of `DemoTrigger` to the real type once #20
//! lands.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Where the notification originated. Mirrors `pie_agent_core::SourceKind` from RFC 1 §2.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    Local,
    Mcp,
    Hub,
}

/// Privacy tier for the carried payload. Mirrors RFC 1 §2.3 / RFC 0 §3.2.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadVisibility {
    Local,
    Shared,
    Redacted,
}

/// Audit/authorization summary attached to every trigger. Token material is NEVER stored here.
/// `principal_id` is opaque-stable (ULID-style); `principal_label` is for display.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Authority {
    pub principal_id: String,
    pub principal_label: String,
    pub credential_scope: CredentialScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CredentialScope {
    User,
    Project,
    Team,
    Agent,
    None,
}

/// Phase-1 normalized envelope.
///
/// Both source variants (MCP push, mock WebSocket hub) produce a `DemoTrigger`; that
/// convergence is the whole point of phase 1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DemoTrigger {
    pub source_kind: SourceKind,
    pub source_label: String,
    pub event_label: String,
    pub idempotency_key: String,
    pub trace_id: String,
    pub authority: Authority,
    pub payload_visibility: PayloadVisibility,
    pub payload_summary: Option<String>,
    pub received_at: DateTime<Utc>,
}

impl DemoTrigger {
    /// Pretty-print for stdout demos. Deliberately verbose so reviewers can eyeball every
    /// field. The format is for human reading, not for parsing by downstream code.
    pub fn render(&self) -> String {
        let mut s = String::new();
        s.push_str("[trigger]\n");
        s.push_str(&format!("  source_kind:        {:?}\n", self.source_kind));
        s.push_str(&format!("  source_label:       {}\n", self.source_label));
        s.push_str(&format!("  event_label:        {}\n", self.event_label));
        s.push_str(&format!("  idempotency_key:    {}\n", self.idempotency_key));
        s.push_str(&format!("  trace_id:           {}\n", self.trace_id));
        s.push_str(&format!(
            "  authority:          principal_id={} principal_label={} credential_scope={:?}\n",
            self.authority.principal_id,
            self.authority.principal_label,
            self.authority.credential_scope
        ));
        s.push_str(&format!(
            "  payload_visibility: {:?}\n",
            self.payload_visibility
        ));
        s.push_str(&format!(
            "  payload_summary:    {}\n",
            self.payload_summary.as_deref().unwrap_or("(none)")
        ));
        s.push_str(&format!("  received_at:        {}\n", self.received_at));
        s
    }
}

/// In-memory dedup keyed by `idempotency_key`. Mirrors RFC 1 §5 dedup window concept but
/// without any time-based eviction — phase 1 only needs "duplicate keys collapse".
#[derive(Default)]
pub struct DedupSink {
    seen: HashMap<String, DemoTrigger>,
    accepted: Vec<DemoTrigger>,
    deduped: Vec<String>,
}

impl DedupSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `Ok(())` if the trigger was newly accepted; `Err(existing_trace_id)` if it is a
    /// duplicate of a previously-accepted trigger.
    pub fn submit(&mut self, t: DemoTrigger) -> Result<(), String> {
        if let Some(prev) = self.seen.get(&t.idempotency_key) {
            self.deduped.push(t.idempotency_key.clone());
            return Err(prev.trace_id.clone());
        }
        self.seen.insert(t.idempotency_key.clone(), t.clone());
        self.accepted.push(t);
        Ok(())
    }

    pub fn accepted(&self) -> &[DemoTrigger] {
        &self.accepted
    }

    pub fn deduped(&self) -> &[String] {
        &self.deduped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(idempotency_key: &str) -> DemoTrigger {
        DemoTrigger {
            source_kind: SourceKind::Mcp,
            source_label: "MCP test".into(),
            event_label: "test event".into(),
            idempotency_key: idempotency_key.into(),
            trace_id: format!("trace-{}", idempotency_key),
            authority: Authority {
                principal_id: "01H000000000000000000000".into(),
                principal_label: "test-user".into(),
                credential_scope: CredentialScope::User,
            },
            payload_visibility: PayloadVisibility::Local,
            payload_summary: Some("hello".into()),
            received_at: Utc::now(),
        }
    }

    #[test]
    fn dedup_collapses_duplicates() {
        let mut sink = DedupSink::new();
        assert!(sink.submit(t("k1")).is_ok());
        assert!(sink.submit(t("k2")).is_ok());
        let r = sink.submit(t("k1"));
        assert!(r.is_err());
        assert_eq!(sink.accepted().len(), 2);
        assert_eq!(sink.deduped(), &["k1".to_string()]);
    }

    #[test]
    fn render_includes_all_fields() {
        let trig = t("k1");
        let rendered = trig.render();
        assert!(rendered.contains("source_kind"));
        assert!(rendered.contains("MCP test"));
        assert!(rendered.contains("test event"));
        assert!(rendered.contains("k1"));
        assert!(rendered.contains("trace-k1"));
        assert!(rendered.contains("01H000000000000000000000"));
        assert!(rendered.contains("test-user"));
        assert!(rendered.contains("User"));
        assert!(rendered.contains("Local"));
        assert!(rendered.contains("hello"));
    }
}
