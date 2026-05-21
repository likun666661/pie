//! `find` tool — filename-only glob match across a directory tree. Models
//! `packages/coding-agent/src/core/tools/find.ts` at a simplified level.

use async_trait::async_trait;
use ignore::WalkBuilder;
use pie_agent_core::{
    AgentTool, AgentToolError, AgentToolResult, AgentToolUpdate, ToolExecutionMode,
};
use pie_ai::{Tool, UserContentBlock};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

const DEFAULT_LIMIT: usize = 1_000;

pub struct FindTool;

#[async_trait]
impl AgentTool for FindTool {
    fn definition(&self) -> &Tool {
        &DEFINITION
    }

    fn label(&self) -> &str {
        "find"
    }

    fn execution_mode(&self) -> Option<ToolExecutionMode> {
        Some(ToolExecutionMode::Parallel)
    }

    async fn execute(
        &self,
        _id: &str,
        params: Value,
        cancel: CancellationToken,
        _on_update: Option<AgentToolUpdate>,
    ) -> Result<AgentToolResult, AgentToolError> {
        let glob = params
            .get("glob")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AgentToolError::from("missing `glob`"))?
            .to_string();
        let path = params
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or(".")
            .to_string();
        let limit = params
            .get("limit")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize)
            .unwrap_or(DEFAULT_LIMIT);

        let cancel_clone = cancel.clone();
        let glob_for_blocking = glob.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<Vec<String>, String> {
            let mut tb = ignore::types::TypesBuilder::new();
            tb.add("g", &glob_for_blocking).map_err(|e| e.to_string())?;
            tb.select("g");
            let types = tb.build().map_err(|e| e.to_string())?;
            let walker = WalkBuilder::new(&path).standard_filters(true).types(types).build();
            let mut out = Vec::new();
            for entry in walker {
                if cancel_clone.is_cancelled() {
                    break;
                }
                let Ok(entry) = entry else { continue };
                if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                    continue;
                }
                out.push(entry.path().display().to_string());
                if out.len() >= limit {
                    break;
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| AgentToolError::from(format!("spawn_blocking: {e}")))?;
        let paths = result.map_err(AgentToolError::from)?;

        let mut text = format!("find {glob}: {} hits\n", paths.len());
        for p in &paths {
            text.push_str(p);
            text.push('\n');
        }
        Ok(AgentToolResult {
            content: vec![UserContentBlock::text(text)],
            details: json!({ "paths": paths }),
            terminate: None,
        })
    }
}

use once_cell::sync::Lazy;
static DEFINITION: Lazy<Tool> = Lazy::new(|| Tool {
    name: "find".into(),
    description: format!(
        "Find files by filename glob. Honors .gitignore. Output limited to {DEFAULT_LIMIT} paths."
    ),
    parameters: json!({
        "type": "object",
        "properties": {
            "glob": { "type": "string", "description": "Filename glob (e.g. *.rs, README*)" },
            "path": { "type": "string", "description": "Directory to search (default: current)" },
            "limit": { "type": "integer", "description": "Max path count" },
        },
        "required": ["glob"],
    }),
});

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn finds_files_by_glob() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), "").unwrap();
        std::fs::write(dir.path().join("b.txt"), "").unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/c.rs"), "").unwrap();

        let tool = FindTool;
        let r = tool
            .execute(
                "f",
                json!({ "glob": "*.rs", "path": dir.path().to_str().unwrap() }),
                CancellationToken::new(),
                None,
            )
            .await
            .unwrap();
        let text = match &r.content[0] {
            pie_ai::UserContentBlock::Text(t) => t.text.clone(),
            _ => panic!("expected text"),
        };
        assert!(text.contains("a.rs"));
        assert!(text.contains("c.rs"));
        assert!(!text.contains("b.txt"));
    }
}
