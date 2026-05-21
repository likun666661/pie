//! Session helpers — wraps `pie_agent_core::JsonlSessionRepo` with resume / list / delete
//! semantics scoped to the current cwd hash.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use pie_agent_core::{JsonlSessionRepo, Session};

use crate::config::sessions_dir_for_cwd;

pub struct SessionEntry {
    pub path: PathBuf,
    pub id: String,
    pub created_at: String,
    pub preview: Option<String>,
}

pub async fn open_repo(cwd: &std::path::Path) -> JsonlSessionRepo {
    JsonlSessionRepo::new(sessions_dir_for_cwd(cwd))
}

/// Create a brand-new session under the current cwd's sessions dir.
pub async fn create(repo: &JsonlSessionRepo, cwd: &std::path::Path) -> Result<Session> {
    Ok(repo.create(cwd.to_string_lossy().to_string()).await?)
}

/// Resume the most recent session for this cwd, or a specific one by id when supplied.
pub async fn resume(
    repo: &JsonlSessionRepo,
    explicit_id: Option<&str>,
) -> Result<Session> {
    let files = repo.list().await?;
    if files.is_empty() {
        bail!("no sessions to resume in {}", repo.root().display());
    }
    let chosen = if let Some(id) = explicit_id {
        files
            .iter()
            .find(|p| {
                p.file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s == id || s.starts_with(id))
                    .unwrap_or(false)
            })
            .with_context(|| format!("no session matches id {id}"))?
            .clone()
    } else {
        // JsonlSessionRepo::list() sorts ascending by name (UUIDv7), so the tail is newest.
        files.last().cloned().unwrap()
    };
    Ok(repo.open(&chosen).await?)
}

/// List sessions for this cwd, oldest → newest, with a short preview from the first user
/// message when available.
pub async fn list_entries(repo: &JsonlSessionRepo) -> Result<Vec<SessionEntry>> {
    let mut out = Vec::new();
    for path in repo.list().await? {
        let session = repo.open(&path).await?;
        let meta = session.storage().get_metadata_json().await?;
        let id = meta.get("id").and_then(|v| v.as_str()).unwrap_or("?").to_string();
        let created_at = meta.get("createdAt").and_then(|v| v.as_str()).unwrap_or("?").to_string();
        let preview = first_user_text(&session).await;
        out.push(SessionEntry { path, id, created_at, preview });
    }
    Ok(out)
}

async fn first_user_text(session: &Session) -> Option<String> {
    let entries = session.entries().await.ok()?;
    for e in entries {
        if let pie_agent_core::SessionTreeEntry::Message {
            message: pie_agent_core::AgentMessage::Llm(pie_ai::Message::User(u)),
            ..
        } = e
        {
            let text = match u.content {
                pie_ai::UserContent::Text(s) => s,
                pie_ai::UserContent::Blocks(blocks) => blocks
                    .into_iter()
                    .filter_map(|b| match b {
                        pie_ai::UserContentBlock::Text(t) => Some(t.text),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" "),
            };
            let mut text = text.replace('\n', " ");
            if text.len() > 80 {
                text.truncate(80);
                text.push('…');
            }
            return Some(text);
        }
    }
    None
}

/// Delete a session by id (full UUIDv7) or a unique prefix.
pub async fn delete_by_id(repo: &JsonlSessionRepo, id: &str) -> Result<PathBuf> {
    for path in repo.list().await? {
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        if stem == id || stem.starts_with(id) {
            repo.delete(&path).await?;
            return Ok(path);
        }
    }
    bail!("no session matches id {id}");
}
