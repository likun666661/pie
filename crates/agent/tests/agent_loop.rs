//! End-to-end Agent loop test. Uses a synthetic `StreamFn` (via the AssistantMessageEventStream
//! sender API exposed by pie-ai) to drive the loop deterministically — no LLM calls.

use std::sync::Arc;

use pie_agent_core::{Agent, AgentEvent, AgentMessage, AgentOptions, AgentState, AgentTool};
use pie_ai::{
    AssistantMessage, AssistantMessageEvent, AssistantMessageEventStream, AssistantRole,
    ContentBlock, DoneReason, ModelCost, StopReason, ToolCall, Usage,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

fn faux_model() -> pie_ai::Model {
    pie_ai::Model {
        id: "faux".into(),
        name: "Faux".into(),
        api: pie_ai::Api::from("faux"),
        provider: pie_ai::Provider::from("faux"),
        base_url: String::new(),
        reasoning: false,
        thinking_level_map: None,
        input: vec![],
        cost: ModelCost::default(),
        context_window: 0,
        max_tokens: 0,
        headers: None,
        compat: None,
    }
}

fn assistant_with(content: Vec<ContentBlock>, stop_reason: StopReason) -> AssistantMessage {
    AssistantMessage {
        role: AssistantRole::Assistant,
        content,
        api: pie_ai::Api::from("faux"),
        provider: pie_ai::Provider::from("faux"),
        model: "faux".into(),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::default(),
        stop_reason,
        error_message: None,
        timestamp: 0,
    }
}

fn faux_stream_fn_with(responses: Arc<Mutex<Vec<AssistantMessage>>>) -> pie_agent_core::StreamFn {
    Arc::new(move |_, _, _| {
        let (stream, mut sender) = AssistantMessageEventStream::new();
        let responses = responses.clone();
        tokio::spawn(async move {
            let msg = {
                let mut g = responses.lock().await;
                if g.is_empty() {
                    AssistantMessage {
                        stop_reason: StopReason::Stop,
                        ..assistant_with(vec![ContentBlock::text("done")], StopReason::Stop)
                    }
                } else {
                    g.remove(0)
                }
            };
            sender.push(AssistantMessageEvent::Start {
                partial: msg.clone(),
            });
            let reason = match msg.stop_reason {
                StopReason::ToolUse => DoneReason::ToolUse,
                StopReason::Length => DoneReason::Length,
                _ => DoneReason::Stop,
            };
            sender.push(AssistantMessageEvent::Done {
                reason,
                message: msg,
            });
        });
        stream
    })
}

#[tokio::test]
async fn single_turn_no_tools_emits_lifecycle_events() {
    let responses = Arc::new(Mutex::new(vec![assistant_with(
        vec![ContentBlock::text("hello there")],
        StopReason::Stop,
    )]));

    let mut state = AgentState::default();
    state.model = Some(faux_model());
    state.system_prompt = "be friendly".into();

    let agent = Agent::new(AgentOptions {
        initial_state: Some(state),
        stream_fn: Some(faux_stream_fn_with(responses)),
        ..Default::default()
    });

    let events = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let events_clone = events.clone();
    let _unsub = agent.subscribe(Arc::new(move |ev, _| {
        let events = events_clone.clone();
        Box::pin(async move {
            let tag = match ev {
                AgentEvent::AgentStart => "agent_start",
                AgentEvent::AgentEnd { .. } => "agent_end",
                AgentEvent::TurnStart => "turn_start",
                AgentEvent::TurnEnd { .. } => "turn_end",
                AgentEvent::MessageStart { .. } => "message_start",
                AgentEvent::MessageEnd { .. } => "message_end",
                AgentEvent::MessageUpdate { .. } => "message_update",
                AgentEvent::ToolExecutionStart { .. } => "tool_execution_start",
                AgentEvent::ToolExecutionEnd { .. } => "tool_execution_end",
                AgentEvent::ToolExecutionUpdate { .. } => "tool_execution_update",
            };
            events.lock().unwrap().push(tag.to_string());
        })
    }));

    let user = AgentMessage::Llm(pie_ai::Message::User(pie_ai::UserMessage {
        role: pie_ai::UserRole::User,
        content: pie_ai::UserContent::Text("hi".into()),
        timestamp: 0,
    }));
    agent.prompt(user).await.unwrap();

    let events = events.lock().unwrap();
    assert_eq!(events.first().map(String::as_str), Some("agent_start"));
    assert_eq!(events.last().map(String::as_str), Some("agent_end"));
    // Should contain at least one turn boundary.
    assert!(events.iter().any(|e| e == "turn_start"));
    assert!(events.iter().any(|e| e == "turn_end"));
    // Transcript should now include user + assistant.
    let g = agent.state();
    assert_eq!(g.messages.len(), 2);
}

#[tokio::test]
async fn tool_call_loops_until_non_tool_use_stop() {
    // The faux model first emits an assistant message with a tool call, then on the next call
    // emits a plain stop.
    let mut args = serde_json::Map::new();
    args.insert("x".into(), serde_json::json!(1));
    let responses = Arc::new(Mutex::new(vec![
        assistant_with(
            vec![ContentBlock::ToolCall(ToolCall {
                id: "call_1".into(),
                name: "echo".into(),
                arguments: args,
                thought_signature: None,
            })],
            StopReason::ToolUse,
        ),
        assistant_with(vec![ContentBlock::text("ok")], StopReason::Stop),
    ]));

    // Faux echo tool — returns its `x` as text.
    struct EchoTool {
        def: pie_ai::Tool,
    }
    #[async_trait::async_trait]
    impl AgentTool for EchoTool {
        fn definition(&self) -> &pie_ai::Tool {
            &self.def
        }
        fn label(&self) -> &str {
            "echo"
        }
        async fn execute(
            &self,
            _id: &str,
            params: serde_json::Value,
            _cancel: CancellationToken,
            _on_update: Option<pie_agent_core::AgentToolUpdate>,
        ) -> Result<pie_agent_core::AgentToolResult, pie_agent_core::AgentToolError> {
            let x = params.get("x").and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(pie_agent_core::AgentToolResult {
                content: vec![pie_ai::UserContentBlock::text(format!("got x={x}"))],
                details: serde_json::Value::Null,
                terminate: None,
            })
        }
    }

    let tool = Arc::new(EchoTool {
        def: pie_ai::Tool {
            name: "echo".into(),
            description: "echo".into(),
            parameters: serde_json::json!({ "type": "object" }),
        },
    });

    let mut state = AgentState::default();
    state.model = Some(faux_model());
    state.tools = vec![tool];

    let agent = Agent::new(AgentOptions {
        initial_state: Some(state),
        stream_fn: Some(faux_stream_fn_with(responses)),
        ..Default::default()
    });

    let user = AgentMessage::Llm(pie_ai::Message::User(pie_ai::UserMessage {
        role: pie_ai::UserRole::User,
        content: pie_ai::UserContent::Text("compute".into()),
        timestamp: 0,
    }));
    agent.prompt(user).await.unwrap();

    let g = agent.state();
    // user → assistant#1 (tool_use) → toolResult → assistant#2 (stop)
    assert_eq!(g.messages.len(), 4);
    let tool_result_present = g.messages.iter().any(|m| {
        matches!(m, AgentMessage::Llm(pie_ai::Message::ToolResult(tr)) if tr.tool_call_id == "call_1")
    });
    assert!(tool_result_present);
}

#[tokio::test]
async fn before_tool_call_can_veto_execution() {
    use pie_agent_core::{BeforeToolCallContext, BeforeToolCallResult};

    let mut args = serde_json::Map::new();
    args.insert("x".into(), serde_json::json!(1));
    let responses = Arc::new(Mutex::new(vec![
        assistant_with(
            vec![ContentBlock::ToolCall(ToolCall {
                id: "call_1".into(),
                name: "echo".into(),
                arguments: args,
                thought_signature: None,
            })],
            StopReason::ToolUse,
        ),
        assistant_with(vec![ContentBlock::text("done")], StopReason::Stop),
    ]));

    struct EchoTool {
        def: pie_ai::Tool,
        called: Arc<std::sync::atomic::AtomicBool>,
    }
    #[async_trait::async_trait]
    impl pie_agent_core::AgentTool for EchoTool {
        fn definition(&self) -> &pie_ai::Tool {
            &self.def
        }
        fn label(&self) -> &str {
            "echo"
        }
        async fn execute(
            &self,
            _id: &str,
            _params: serde_json::Value,
            _cancel: CancellationToken,
            _on_update: Option<pie_agent_core::AgentToolUpdate>,
        ) -> Result<pie_agent_core::AgentToolResult, pie_agent_core::AgentToolError> {
            self.called.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(pie_agent_core::AgentToolResult::default())
        }
    }

    let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let tool = Arc::new(EchoTool {
        def: pie_ai::Tool {
            name: "echo".into(),
            description: "echo".into(),
            parameters: serde_json::json!({ "type": "object" }),
        },
        called: called.clone(),
    });

    let veto_hook: pie_agent_core::BeforeToolCallHook =
        Arc::new(|_ctx: BeforeToolCallContext, _cancel: CancellationToken| {
            Box::pin(async move {
                BeforeToolCallResult {
                    block: true,
                    reason: Some("policy: no echo".into()),
                }
            })
        });

    let mut state = pie_agent_core::AgentState::default();
    state.model = Some(faux_model());
    state.tools = vec![tool];

    let agent = Agent::new(AgentOptions {
        initial_state: Some(state),
        stream_fn: Some(faux_stream_fn_with(responses)),
        before_tool_call: Some(veto_hook),
        ..Default::default()
    });

    let user = AgentMessage::Llm(pie_ai::Message::User(pie_ai::UserMessage {
        role: pie_ai::UserRole::User,
        content: pie_ai::UserContent::Text("go".into()),
        timestamp: 0,
    }));
    agent.prompt(user).await.unwrap();

    assert!(
        !called.load(std::sync::atomic::Ordering::SeqCst),
        "tool must not run when hook blocks"
    );
    let g = agent.state();
    // The synthesized tool result should be is_error=true with the hook reason.
    let synth = g
        .messages
        .iter()
        .find_map(|m| match m {
            AgentMessage::Llm(pie_ai::Message::ToolResult(tr)) => Some(tr),
            _ => None,
        })
        .expect("synth tool result");
    assert!(synth.is_error);
    let text = match &synth.content[0] {
        pie_ai::UserContentBlock::Text(t) => t.text.clone(),
        _ => panic!("expected text"),
    };
    assert!(text.contains("policy: no echo"));
}

#[tokio::test]
async fn parallel_tools_execute_concurrently() {
    let mut args = serde_json::Map::new();
    args.insert("id".into(), serde_json::json!(1));
    let mut args2 = serde_json::Map::new();
    args2.insert("id".into(), serde_json::json!(2));
    let responses = Arc::new(Mutex::new(vec![
        assistant_with(
            vec![
                ContentBlock::ToolCall(ToolCall {
                    id: "a".into(),
                    name: "slow".into(),
                    arguments: args,
                    thought_signature: None,
                }),
                ContentBlock::ToolCall(ToolCall {
                    id: "b".into(),
                    name: "slow".into(),
                    arguments: args2,
                    thought_signature: None,
                }),
            ],
            StopReason::ToolUse,
        ),
        assistant_with(vec![ContentBlock::text("done")], StopReason::Stop),
    ]));

    // Sleep 200ms per call — under parallel, total ≈200ms; sequential would be ≈400ms.
    struct SlowTool {
        def: pie_ai::Tool,
    }
    #[async_trait::async_trait]
    impl pie_agent_core::AgentTool for SlowTool {
        fn definition(&self) -> &pie_ai::Tool {
            &self.def
        }
        fn label(&self) -> &str {
            "slow"
        }
        async fn execute(
            &self,
            _id: &str,
            _params: serde_json::Value,
            _cancel: CancellationToken,
            _on_update: Option<pie_agent_core::AgentToolUpdate>,
        ) -> Result<pie_agent_core::AgentToolResult, pie_agent_core::AgentToolError> {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            Ok(pie_agent_core::AgentToolResult::default())
        }
    }
    let tool = Arc::new(SlowTool {
        def: pie_ai::Tool {
            name: "slow".into(),
            description: "sleep".into(),
            parameters: serde_json::json!({ "type": "object" }),
        },
    });

    let mut state = pie_agent_core::AgentState::default();
    state.model = Some(faux_model());
    state.tools = vec![tool];
    // tool_execution defaults to Parallel.

    let agent = Agent::new(AgentOptions {
        initial_state: Some(state),
        stream_fn: Some(faux_stream_fn_with(responses)),
        ..Default::default()
    });

    let user = AgentMessage::Llm(pie_ai::Message::User(pie_ai::UserMessage {
        role: pie_ai::UserRole::User,
        content: pie_ai::UserContent::Text("go".into()),
        timestamp: 0,
    }));
    let start = std::time::Instant::now();
    agent.prompt(user).await.unwrap();
    let elapsed = start.elapsed();
    // Parallel should finish in well under 400ms; allow 350ms for scheduler slack.
    assert!(
        elapsed < std::time::Duration::from_millis(350),
        "expected parallel tool exec, took {:?}",
        elapsed
    );
}
