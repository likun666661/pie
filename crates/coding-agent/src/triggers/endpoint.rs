//! Session-scoped public webhook endpoint bindings.
//!
//! A binding records that hub endpoint `endpoint_id` belongs to THIS session. The hub
//! fans out `notifications/endpoint_message` frames to every connected client of the
//! owning agent; only the session whose sidecar holds the binding converts the frame
//! into a runtime `Trigger` (and acks it). Foreign frames are ignored so they stay in
//! the hub backlog for the owning session.

// Public API consumed by later tasks (trigger mapping / action-hook wiring).
#![allow(dead_code)]

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use pie_agent_core::{
    CredentialScope, PayloadVisibility, ReplacementPolicy, SourceKind, Trigger, TriggerAuthority,
    TriggerSource,
};
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
    use super::mcp_notification_hook::{safe_display, safe_idempotency_segment};

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
    let text = serde_json::to_string_pretty(&file)
        .map_err(|e| EndpointStorageError::Write(e.to_string()))?;
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
    use pie_agent_core::{PayloadVisibility, ReplacementPolicy, SourceKind, TriggerSource};
    use serde_json::json;

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
            map_endpoint_message("pie-hub", &endpoint_params("ep-other", "x"), &registry).is_none(),
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
        let trigger = map_endpoint_message("pie-hub", &endpoint_params("ep-1", body), &registry)
            .expect("maps");
        let summary = trigger.payload_summary.unwrap();
        assert!(
            !summary.contains("hub_agent_secret_should_not_persist"),
            "audit summary must redact token-like text: {summary}"
        );
        // The Shared payload carries the verbatim body for the agent prompt.
        assert_eq!(
            trigger
                .payload
                .unwrap()
                .get("body")
                .and_then(|v| v.as_str()),
            Some(body)
        );
    }

    #[test]
    fn endpoint_url_token_is_redacted_from_summary() {
        let registry = EndpointRegistry::new();
        registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");
        let body = "echo hub_ep_abcdef0123456789abcdef0123456789 back";
        let trigger = map_endpoint_message("pie-hub", &endpoint_params("ep-1", body), &registry)
            .expect("maps");
        let summary = trigger.payload_summary.unwrap();
        assert!(
            !summary.contains("hub_ep_abcdef"),
            "capability tokens must not leak into the audit summary: {summary}"
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
            map_endpoint_message("pie-hub", &json!({ "endpoint_id": "ep-1" }), &registry).is_none(),
            "missing notification_id must not map"
        );
    }
}
