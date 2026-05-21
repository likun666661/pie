//! Paths and identity. Mirrors `packages/coding-agent/src/config.ts` in spirit — one source of
//! truth for `~/.pie/...` and the cwd-hash directory layout.

use std::path::PathBuf;

use sha2::{Digest, Sha256};

/// Base directory: `${PIE_DIR:-$HOME/.pie}`.
pub fn base_dir() -> PathBuf {
    if let Ok(p) = std::env::var("PIE_DIR") {
        return PathBuf::from(p);
    }
    directories::BaseDirs::new()
        .map(|d| d.home_dir().join(".pie"))
        .unwrap_or_else(|| PathBuf::from(".pie"))
}

/// Sessions live under `<base>/sessions/<cwd-hash>/<uuidv7>.jsonl`. Hashing the cwd lets us
/// scope `--resume` to "last session opened from this directory".
pub fn sessions_dir_for_cwd(cwd: &std::path::Path) -> PathBuf {
    let hash = cwd_hash(cwd);
    base_dir().join("sessions").join(hash)
}

/// Memory dir is global (not per-cwd) — that's the whole point of cross-session memory.
pub fn memory_dir() -> PathBuf {
    base_dir().join("memory")
}

/// Deterministic short hash of an absolute cwd path. Same input → same dir, so reopening from
/// the same project always finds prior sessions.
pub fn cwd_hash(cwd: &std::path::Path) -> String {
    let mut h = Sha256::new();
    h.update(cwd.to_string_lossy().as_bytes());
    let digest = h.finalize();
    hex::encode(&digest[..6]) // 12 chars; plenty for low-collision per-cwd buckets
}
