//! MCP server configuration loader. Reads `~/.pie/mcp.toml` (and `<cwd>/.pie/mcp.toml`),
//! spawns each configured stdio server, runs the initialize+tools/list handshake, and
//! returns the resulting AgentTool list ready to append to `default_tools()`.
//!
//! Failure is non-fatal at the load level: a server that fails to start emits a startup
//! diagnostic and is skipped. The agent runs without it.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use pie_agent_core::AgentTool;
use pie_mcp::{McpClient, StdioTransport};
use serde::Deserialize;

use crate::config::base_dir;
use crate::tools::mcp_adapter::McpAgentTool;

#[derive(Debug, Default, Deserialize)]
pub struct McpConfig {
    #[serde(default)]
    pub server: Vec<ServerConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub name: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
}

/// Output of loading. Holds tools (to register with the agent) and diagnostics (startup
/// failures to print to the user).
pub struct LoadedMcp {
    pub tools: Vec<Arc<dyn AgentTool>>,
    pub diagnostics: Vec<String>,
    pub client_count: usize,
}

/// Load and connect every MCP server from the project + user configs. Project entries with
/// the same `name` as a user entry override.
pub async fn load_all(cwd: &Path) -> LoadedMcp {
    let mut diagnostics = Vec::new();
    let project_path = cwd.join(".pie").join("mcp.toml");
    let user_path = base_dir().join("mcp.toml");

    let mut configs: Vec<ServerConfig> = Vec::new();
    for (path, label) in [(&user_path, "user"), (&project_path, "project")] {
        if let Some(cfg) = read_config(path, &mut diagnostics, label).await {
            for s in cfg.server {
                if let Some(i) = configs.iter().position(|x| x.name == s.name) {
                    configs[i] = s;
                } else {
                    configs.push(s);
                }
            }
        }
    }

    let mut tools: Vec<Arc<dyn AgentTool>> = Vec::new();
    for s in configs.iter() {
        match connect_one(s).await {
            Ok(server_tools) => tools.extend(server_tools),
            Err(e) => {
                diagnostics.push(format!("mcp server '{}' failed: {e}", s.name));
            }
        }
    }
    LoadedMcp {
        tools,
        diagnostics,
        client_count: configs.len(),
    }
}

async fn read_config(path: &Path, diagnostics: &mut Vec<String>, label: &str) -> Option<McpConfig> {
    if !tokio::fs::try_exists(path).await.unwrap_or(false) {
        return None;
    }
    match tokio::fs::read_to_string(path).await {
        Ok(text) => match toml::from_str::<McpConfig>(&text) {
            Ok(cfg) => Some(cfg),
            Err(e) => {
                diagnostics.push(format!(
                    "mcp config ({label}, {}): parse failed: {e}",
                    path.display()
                ));
                None
            }
        },
        Err(e) => {
            diagnostics.push(format!(
                "mcp config ({label}, {}): read failed: {e}",
                path.display()
            ));
            None
        }
    }
}

async fn connect_one(s: &ServerConfig) -> Result<Vec<Arc<dyn AgentTool>>> {
    let args: Vec<&str> = s.args.iter().map(|x| x.as_str()).collect();
    let transport = StdioTransport::spawn(&s.command, &args).await?;
    let client = Arc::new(McpClient::new(Arc::new(transport)));
    client.initialize("pie-coding-agent").await?;
    let tools = client.tools_list().await?;
    let mut out: Vec<Arc<dyn AgentTool>> = Vec::with_capacity(tools.len());
    for tool in &tools {
        let adapter = McpAgentTool::new(client.clone(), tool);
        out.push(Arc::new(adapter));
    }
    Ok(out)
}
