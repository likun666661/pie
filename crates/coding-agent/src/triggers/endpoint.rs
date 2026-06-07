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
