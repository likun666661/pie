//! `grep` tool — line-based regex match across a directory tree. Models
//! `packages/coding-agent/src/core/tools/grep.ts` at a simplified level: no thread pool, no
//! ripgrep delegation, just `ignore::WalkBuilder` + the `regex` crate. Output is truncated to
//! the first N matches.

use async_trait::async_trait;
use ignore::WalkBuilder;
use pie_agent_core::{
    AgentTool, AgentToolError, AgentToolResult, AgentToolUpdate, ToolExecutionMode,
};
use pie_ai::{Tool, UserContentBlock};
use regex::Regex;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

const DEFAULT_MAX_MATCHES: usize = 200;
const DEFAULT_MAX_FILES: usize = 5_000;

pub struct GrepTool;

#[async_trait]
impl AgentTool for GrepTool {
    fn definition(&self) -> &Tool {
        &DEFINITION
    }

    fn label(&self) -> &str {
        "grep"
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
        let pattern = params
            .get("pattern")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AgentToolError::from("missing `pattern`"))?;
        let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
        let glob = params.get("glob").and_then(|v| v.as_str());
        let case_insensitive = params
            .get("case_insensitive")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let max_matches = params
            .get("limit")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize)
            .unwrap_or(DEFAULT_MAX_MATCHES);

        let mut builder = regex::RegexBuilder::new(pattern);
        builder.case_insensitive(case_insensitive);
        let re: Regex =
            builder.build().map_err(|e| AgentToolError::from(format!("regex: {e}")))?;

        // Walk synchronously inside spawn_blocking so .gitignore + sibling files are honored
        // by `ignore` and we don't block the runtime.
        let path = path.to_string();
        let glob = glob.map(str::to_string);
        let re_clone = re.clone();
        let cancel_clone = cancel.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<Vec<MatchOut>, String> {
            let mut walker = WalkBuilder::new(&path);
            walker.standard_filters(true).hidden(true);
            if let Some(g) = &glob {
                let mut tb = ignore::types::TypesBuilder::new();
                tb.add("g", g).map_err(|e| e.to_string())?;
                tb.select("g");
                let types = tb.build().map_err(|e| e.to_string())?;
                walker.types(types);
            }
            let walker = walker.build();
            let mut out: Vec<MatchOut> = Vec::new();
            let mut files_scanned = 0usize;
            for entry in walker {
                if cancel_clone.is_cancelled() {
                    break;
                }
                let entry = match entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                    continue;
                }
                files_scanned += 1;
                if files_scanned > DEFAULT_MAX_FILES {
                    break;
                }
                let p = entry.path();
                let body = match std::fs::read_to_string(p) {
                    Ok(b) => b,
                    Err(_) => continue, // binary or unreadable; skip
                };
                for (lineno, line) in body.lines().enumerate() {
                    if re_clone.is_match(line) {
                        out.push(MatchOut {
                            path: p.display().to_string(),
                            lineno: lineno + 1,
                            text: line.to_string(),
                        });
                        if out.len() >= max_matches {
                            return Ok(out);
                        }
                    }
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| AgentToolError::from(format!("spawn_blocking: {e}")))?;
        let matches = result.map_err(AgentToolError::from)?;

        let mut text = format!("grep: {} hits\n", matches.len());
        for m in matches.iter().take(max_matches) {
            text.push_str(&format!("{}:{}: {}\n", m.path, m.lineno, m.text));
        }
        if matches.len() >= max_matches {
            text.push_str(&format!("[truncated at {max_matches} matches]\n"));
        }
        Ok(AgentToolResult {
            content: vec![UserContentBlock::text(text)],
            details: json!({ "matches": matches.len() }),
            terminate: None,
        })
    }
}

struct MatchOut {
    path: String,
    lineno: usize,
    text: String,
}

use once_cell::sync::Lazy;
static DEFINITION: Lazy<Tool> = Lazy::new(|| Tool {
    name: "grep".into(),
    description: format!(
        "Search files for lines matching a regex. Honors .gitignore. Output limited to {DEFAULT_MAX_MATCHES} matches."
    ),
    parameters: json!({
        "type": "object",
        "properties": {
            "pattern": { "type": "string", "description": "Regex pattern" },
            "path": { "type": "string", "description": "Directory to search (default: current)" },
            "glob": { "type": "string", "description": "Optional filename glob (e.g. *.rs)" },
            "case_insensitive": { "type": "boolean", "description": "Case-insensitive match" },
            "limit": { "type": "integer", "description": "Max match count" },
        },
        "required": ["pattern"],
    }),
});

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn finds_matches_in_file_tree() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), "hello world\nfoo bar\n").unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/b.txt"), "another hello\n").unwrap();

        let tool = GrepTool;
        let r = tool
            .execute(
                "g",
                json!({ "pattern": "hello", "path": dir.path().to_str().unwrap() }),
                CancellationToken::new(),
                None,
            )
            .await
            .unwrap();
        let text = match &r.content[0] {
            pie_ai::UserContentBlock::Text(t) => t.text.clone(),
            _ => panic!("expected text"),
        };
        assert!(text.contains("hello world"));
        assert!(text.contains("another hello"));
    }
}
