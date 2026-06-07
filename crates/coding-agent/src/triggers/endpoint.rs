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

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use pie_agent_core::{
    BeforeTriggerActionContext, BeforeTriggerActionHook, CredentialScope, HookError, HookState,
    NotificationHook, NotificationHookStatus, PayloadVisibility, PromoteAction, ReplacementPolicy,
    SourceKind, Trigger, TriggerAction, TriggerAuthority, TriggerDelivery, TriggerSink,
    TriggerSource,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
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
                TriggerSource::Mcp { server_name, method }
                    if server_name == crate::config::HUB_SERVER_NAME
                        && method == "notifications/endpoint_message"
            );
            if !is_endpoint {
                return inner(ctx, cancel);
            }
            let payload = ctx
                .trigger
                .payload
                .clone()
                .unwrap_or(serde_json::Value::Null);
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
                                    template_body: Some("{{trigger.payload_summary}}".to_string()),
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

    use pie_agent_core::{
        BeforeTriggerActionContext, PromoteAction, TriggerDelivery, TriggerRuntimeSnapshot,
    };
    use tokio_util::sync::CancellationToken;

    fn endpoint_ctx(
        registry: &EndpointRegistry,
        endpoint_id: &str,
        body: &str,
    ) -> BeforeTriggerActionContext {
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
        Arc::new(
            |ctx: BeforeTriggerActionContext, _cancel: CancellationToken| {
                Box::pin(async move { pie_agent_core::TriggerAction::default_for(&ctx.trigger) })
            },
        )
    }

    #[tokio::test]
    async fn run_mode_injects_body_and_acks() {
        let registry = EndpointRegistry::new();
        registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");
        let (acker, acked) = recording_acker();
        let hook = endpoint_action_hook(registry.clone(), acker, fallthrough_inner());

        let action = hook(
            endpoint_ctx(&registry, "ep-1", "deploy now"),
            CancellationToken::new(),
        )
        .await;

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

        let action = hook(
            endpoint_ctx(&registry, "ep-1", "fyi"),
            CancellationToken::new(),
        )
        .await;

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
        assert_eq!(
            params.get("body").and_then(|v| v.as_str()),
            Some("backlogged")
        );
        assert_eq!(
            params.get("endpoint_id").and_then(|v| v.as_str()),
            Some("ep-1")
        );

        // The rebuilt params feed straight into the live mapping path.
        let registry = EndpointRegistry::new();
        registry
            .add_binding(binding("ep-1", EndpointMode::Run))
            .expect("add");
        let trigger = map_endpoint_message("pie-hub", &params, &registry).expect("replay maps");
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
}
