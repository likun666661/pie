//! Native `ExecutionEnv` — std::fs + tokio::process. Partial 1:1 port of
//! `packages/agent/src/harness/env/nodejs.ts` (~528 lines).
//!
//! Currently exposes everything skills need (file_info, list_dir, read_text_file, canonical,
//! absolute_path, exists). Other methods (write, append, temp dirs, exec) have minimal
//! implementations sufficient for the current test surface; advanced cases (concurrent fs
//! watchers, sandboxed exec) land as TODOs.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::fs;
use tokio::io::AsyncBufReadExt;
use tokio_util::sync::CancellationToken;

use crate::harness::types::*;

pub struct NativeEnv {
    cwd: String,
}

impl NativeEnv {
    pub fn new(cwd: impl Into<String>) -> Self {
        Self { cwd: cwd.into() }
    }

    pub fn current() -> std::io::Result<Self> {
        let cwd = std::env::current_dir()?.to_string_lossy().to_string();
        Ok(Self::new(cwd))
    }

    fn resolve(&self, path: &str) -> PathBuf {
        let p = Path::new(path);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            Path::new(&self.cwd).join(p)
        }
    }
}

fn map_io_error(e: std::io::Error, path: Option<&str>) -> FileError {
    use std::io::ErrorKind;
    let code = match e.kind() {
        ErrorKind::NotFound => FileErrorCode::NotFound,
        ErrorKind::PermissionDenied => FileErrorCode::PermissionDenied,
        ErrorKind::InvalidInput | ErrorKind::InvalidData => FileErrorCode::InvalidPath,
        _ => FileErrorCode::Unknown,
    };
    let mut err = FileError::new(code, e.to_string());
    if let Some(p) = path {
        err = err.with_path(p);
    }
    err
}

fn file_info_from_meta(name: String, path: String, m: std::fs::Metadata) -> FileInfo {
    let kind = if m.file_type().is_symlink() {
        FileKind::Symlink
    } else if m.is_dir() {
        FileKind::Directory
    } else {
        FileKind::File
    };
    let mtime_ms = m
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    FileInfo {
        name,
        path,
        kind,
        size: m.len(),
        mtime_ms,
    }
}

#[async_trait]
impl ExecutionEnv for NativeEnv {
    fn cwd(&self) -> &str {
        &self.cwd
    }

    async fn absolute_path(&self, path: &str, _cancel: CancellationToken) -> FsResult<String> {
        Ok(self.resolve(path).to_string_lossy().to_string())
    }

    async fn join_path(&self, parts: &[&str], _cancel: CancellationToken) -> FsResult<String> {
        let mut p = PathBuf::new();
        for part in parts {
            p.push(part);
        }
        Ok(p.to_string_lossy().to_string())
    }

    async fn read_text_file(&self, path: &str, _cancel: CancellationToken) -> FsResult<String> {
        let p = self.resolve(path);
        fs::read_to_string(&p)
            .await
            .map_err(|e| map_io_error(e, Some(path)))
    }

    async fn read_text_lines(
        &self,
        path: &str,
        max_lines: Option<usize>,
        _cancel: CancellationToken,
    ) -> FsResult<Vec<String>> {
        let p = self.resolve(path);
        let file = fs::File::open(&p)
            .await
            .map_err(|e| map_io_error(e, Some(path)))?;
        let mut reader = tokio::io::BufReader::new(file).lines();
        let mut out = Vec::new();
        let cap = max_lines.unwrap_or(usize::MAX);
        while out.len() < cap {
            match reader.next_line().await {
                Ok(Some(line)) => out.push(line),
                Ok(None) => break,
                Err(e) => return Err(map_io_error(e, Some(path))),
            }
        }
        Ok(out)
    }

    async fn read_binary_file(&self, path: &str, _cancel: CancellationToken) -> FsResult<Vec<u8>> {
        let p = self.resolve(path);
        fs::read(&p).await.map_err(|e| map_io_error(e, Some(path)))
    }

    async fn write_file(
        &self,
        path: &str,
        content: &[u8],
        _cancel: CancellationToken,
    ) -> FsResult<()> {
        let p = self.resolve(path);
        if let Some(parent) = p.parent() {
            let _ = fs::create_dir_all(parent).await;
        }
        fs::write(&p, content)
            .await
            .map_err(|e| map_io_error(e, Some(path)))
    }

    async fn append_file(
        &self,
        path: &str,
        content: &[u8],
        _cancel: CancellationToken,
    ) -> FsResult<()> {
        use tokio::io::AsyncWriteExt;
        let p = self.resolve(path);
        if let Some(parent) = p.parent() {
            let _ = fs::create_dir_all(parent).await;
        }
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&p)
            .await
            .map_err(|e| map_io_error(e, Some(path)))?;
        f.write_all(content)
            .await
            .map_err(|e| map_io_error(e, Some(path)))
    }

    async fn file_info(&self, path: &str, _cancel: CancellationToken) -> FsResult<FileInfo> {
        let p = self.resolve(path);
        let m = fs::symlink_metadata(&p)
            .await
            .map_err(|e| map_io_error(e, Some(path)))?;
        let name = p
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        Ok(file_info_from_meta(
            name,
            p.to_string_lossy().to_string(),
            m,
        ))
    }

    async fn list_dir(&self, path: &str, _cancel: CancellationToken) -> FsResult<Vec<FileInfo>> {
        let p = self.resolve(path);
        let mut rd = fs::read_dir(&p)
            .await
            .map_err(|e| map_io_error(e, Some(path)))?;
        let mut out = Vec::new();
        while let Some(entry) = rd
            .next_entry()
            .await
            .map_err(|e| map_io_error(e, Some(path)))?
        {
            let m = entry
                .metadata()
                .await
                .map_err(|e| map_io_error(e, Some(&entry.path().to_string_lossy())))?;
            let name = entry.file_name().to_string_lossy().to_string();
            let abs = entry.path().to_string_lossy().to_string();
            out.push(file_info_from_meta(name, abs, m));
        }
        Ok(out)
    }

    async fn exists(&self, path: &str, _cancel: CancellationToken) -> FsResult<bool> {
        let p = self.resolve(path);
        match fs::symlink_metadata(&p).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(map_io_error(e, Some(path))),
        }
    }

    async fn canonical_path(&self, path: &str, _cancel: CancellationToken) -> FsResult<String> {
        let p = self.resolve(path);
        let resolved = fs::canonicalize(&p)
            .await
            .map_err(|e| map_io_error(e, Some(path)))?;
        Ok(resolved.to_string_lossy().to_string())
    }

    async fn create_dir(
        &self,
        path: &str,
        recursive: bool,
        _cancel: CancellationToken,
    ) -> FsResult<()> {
        let p = self.resolve(path);
        let res = if recursive {
            fs::create_dir_all(&p).await
        } else {
            fs::create_dir(&p).await
        };
        res.map_err(|e| map_io_error(e, Some(path)))
    }

    async fn remove(
        &self,
        path: &str,
        recursive: bool,
        _force: bool,
        _cancel: CancellationToken,
    ) -> FsResult<()> {
        let p = self.resolve(path);
        let m = match fs::symlink_metadata(&p).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(map_io_error(e, Some(path))),
        };
        let res = if m.is_dir() {
            if recursive {
                fs::remove_dir_all(&p).await
            } else {
                fs::remove_dir(&p).await
            }
        } else {
            fs::remove_file(&p).await
        };
        res.map_err(|e| map_io_error(e, Some(path)))
    }

    async fn create_temp_dir(
        &self,
        prefix: Option<&str>,
        _cancel: CancellationToken,
    ) -> FsResult<String> {
        let p = std::env::temp_dir().join(format!(
            "{}-{}",
            prefix.unwrap_or("tmp-"),
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&p)
            .await
            .map_err(|e| map_io_error(e, None))?;
        Ok(p.to_string_lossy().to_string())
    }

    async fn create_temp_file(
        &self,
        prefix: Option<&str>,
        suffix: Option<&str>,
        _cancel: CancellationToken,
    ) -> FsResult<String> {
        let name = format!(
            "{}{}{}",
            prefix.unwrap_or(""),
            uuid::Uuid::new_v4().simple(),
            suffix.unwrap_or("")
        );
        let p = std::env::temp_dir().join(name);
        fs::write(&p, b"")
            .await
            .map_err(|e| map_io_error(e, None))?;
        Ok(p.to_string_lossy().to_string())
    }

    async fn exec(&self, command: &str, options: ExecOptions) -> ExecResult<ExecOutput> {
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c").arg(command);
        if let Some(cwd) = &options.cwd {
            cmd.current_dir(cwd);
        } else {
            cmd.current_dir(&self.cwd);
        }
        if let Some(env) = &options.env {
            for (k, v) in env {
                cmd.env(k, v);
            }
        }
        // TODO: timeout, onStdout/onStderr streaming, abort honoring.
        let out = cmd
            .output()
            .await
            .map_err(|e| ExecutionError::new(ExecutionErrorCode::SpawnFailed, e.to_string()))?;
        Ok(ExecOutput {
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            exit_code: out.status.code().unwrap_or(-1),
        })
    }
}
