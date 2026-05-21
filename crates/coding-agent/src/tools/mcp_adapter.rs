//! Adapter that wraps an MCP-server-side tool as a pie_agent_core::AgentTool.
//!
//! One `McpAgentTool` corresponds to one tool name on one server. Tool calls are dispatched
//! through the supplied `Arc<McpClient>`; the result's `content` blocks are mapped to the
//! agent's `UserContentBlock` set. Errors from the MCP server become AgentToolError so the
//! agent loop synthesizes a clean tool-error response.

use std::sync::Arc;

use async_trait::async_trait;
use pie_agent_core::{
    AgentTool, AgentToolError, AgentToolResult, AgentToolUpdate, ToolExecutionMode,
};
use pie_ai::{Tool, UserContentBlock};
use pie_mcp::McpClient;
use pie_mcp::protocol::{McpTool, ToolContent};
use tokio_util::sync::CancellationToken;

pub struct McpAgentTool {
    client: Arc<McpClient>,
    definition: Tool,
}

impl McpAgentTool {
    /// Build an adapter for one server-side tool. The pie_ai::Tool definition is constructed
    /// from the MCP catalog entry; the input schema is forwarded verbatim.
    pub fn new(client: Arc<McpClient>, tool: &McpTool) -> Self {
        Self {
            client,
            definition: Tool {
                name: tool.name.clone(),
                description: tool.description.clone().unwrap_or_default(),
                parameters: tool.input_schema.clone(),
            },
        }
    }
}

#[async_trait]
impl AgentTool for McpAgentTool {
    fn definition(&self) -> &Tool {
        &self.definition
    }
    fn label(&self) -> &str {
        &self.definition.name
    }
    fn execution_mode(&self) -> Option<ToolExecutionMode> {
        // MCP tool calls are individually cheap; let them run in parallel by default.
        Some(ToolExecutionMode::Parallel)
    }

    async fn execute(
        &self,
        _id: &str,
        params: serde_json::Value,
        cancel: CancellationToken,
        _on_update: Option<AgentToolUpdate>,
    ) -> Result<AgentToolResult, AgentToolError> {
        let call = self.client.tools_call(&self.definition.name, Some(params));
        let result = tokio::select! {
            r = call => r.map_err(|e| AgentToolError::Message(format!("mcp call: {e}")))?,
            _ = cancel.cancelled() => {
                return Err(AgentToolError::Message("cancelled".into()));
            }
        };

        let mut content = Vec::with_capacity(result.content.len());
        for block in &result.content {
            match block {
                ToolContent::Text { text } => content.push(UserContentBlock::text(text.clone())),
                ToolContent::Image { data, mime_type } => {
                    content.push(UserContentBlock::Image(pie_ai::ImageContent {
                        data: data.clone(),
                        mime_type: mime_type.clone(),
                    }));
                }
                ToolContent::Resource { resource } => {
                    // No first-class "resource" block on the user side; render as a JSON
                    // text snippet so the LLM still sees what the server returned.
                    let s = serde_json::to_string(resource).unwrap_or_else(|_| "(resource)".into());
                    content.push(UserContentBlock::text(format!("<resource>{s}</resource>")));
                }
            }
        }
        if result.is_error {
            return Err(AgentToolError::Message(
                content
                    .into_iter()
                    .filter_map(|b| match b {
                        UserContentBlock::Text(t) => Some(t.text),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ));
        }
        Ok(AgentToolResult {
            content,
            details: serde_json::json!({ "name": self.definition.name, "isError": false }),
            terminate: None,
        })
    }
}
