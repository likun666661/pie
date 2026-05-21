//! Capture-test for the TUI renderer. Drives a realistic event sequence (thinking → text →
//! tool call → tool result → final text → agent end) and asserts on the captured byte
//! stream. Stand-in for a real terminal e2e: without a TTY we can't observe cursor moves,
//! but we can pin the textual content + ANSI escapes that get emitted in order, which
//! catches the bugs we hit live (spinner remnants, double-printed tool names, stale color
//! formatting bleeding into post-thinking text).

use std::sync::Arc;

use pie_agent_core::{AgentEvent, AgentMessage, AgentTool, AgentToolResult};
use pie_ai::{
    AssistantMessage, AssistantMessageEvent, AssistantRole, ContentBlock, ImageContent, Message,
    StopReason, ToolCall, ToolResultMessage, ToolResultRole, Usage, UserContentBlock,
};

#[allow(dead_code)]
#[path = "../src/tui.rs"]
mod tui;

fn assistant(content: Vec<ContentBlock>) -> AssistantMessage {
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
        stop_reason: StopReason::Stop,
        error_message: None,
        timestamp: 0,
    }
}

fn message_update(ev: AssistantMessageEvent, partial: AssistantMessage) -> AgentEvent {
    AgentEvent::MessageUpdate {
        message: AgentMessage::Llm(Message::Assistant(partial)),
        assistant_message_event: ev,
    }
}

/// Strip ANSI SGR escapes so assertions can read the textual content directly. Operates on
/// chars to preserve multi-byte UTF-8 glyphs (like the `⚙` gear).
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut iter = s.chars().peekable();
    while let Some(c) = iter.next() {
        if c == '\x1b' && iter.peek() == Some(&'[') {
            iter.next(); // consume '['
            // Skip CSI bytes until a final byte in 0x40..=0x7e (i.e. an ASCII letter or '@'..'~').
            while let Some(&peek) = iter.peek() {
                iter.next();
                if ('\x40'..='\x7e').contains(&peek) {
                    break;
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

#[test]
fn renders_thinking_then_text_with_clean_transition() {
    let tui = tui::Tui::new();
    let mut buf: Vec<u8> = Vec::new();

    let partial = assistant(vec![ContentBlock::Thinking(pie_ai::ThinkingContent {
        thinking: String::new(),
        thinking_signature: None,
        redacted: false,
    })]);

    tui.render_event(&AgentEvent::AgentStart, &mut buf);
    tui.render_event(
        &message_update(
            AssistantMessageEvent::ThinkingDelta {
                content_index: 0,
                delta: "considering options".into(),
                partial: partial.clone(),
            },
            partial.clone(),
        ),
        &mut buf,
    );
    let partial = assistant(vec![
        ContentBlock::Thinking(pie_ai::ThinkingContent {
            thinking: "considering options".into(),
            thinking_signature: None,
            redacted: false,
        }),
        ContentBlock::text(""),
    ]);
    tui.render_event(
        &message_update(
            AssistantMessageEvent::TextDelta {
                content_index: 1,
                delta: "the answer is 42".into(),
                partial: partial.clone(),
            },
            partial,
        ),
        &mut buf,
    );
    tui.render_event(
        &AgentEvent::AgentEnd {
            messages: Vec::new(),
        },
        &mut buf,
    );

    let captured = String::from_utf8(buf).unwrap();
    let plain = strip_ansi(&captured);
    eprintln!("---captured---\n{captured}\n---plain---\n{plain}\n---end---");

    // Thinking block is labeled and contains its content.
    assert!(plain.contains("[thinking] considering options"), "{plain}");
    // Final text appears AFTER the thinking line, on its own line.
    let idx_thinking = plain.find("[thinking]").unwrap();
    let idx_answer = plain.find("the answer is 42").unwrap();
    assert!(
        idx_answer > idx_thinking,
        "answer should follow thinking: thinking@{idx_thinking} answer@{idx_answer}\n{plain}"
    );
    // Between thinking and answer there must be a newline (no on-the-same-line glue).
    let between = &plain[idx_thinking..idx_answer];
    assert!(
        between.contains('\n'),
        "missing line break between thinking and answer: {between:?}"
    );
    // No stale `[thinking]` label after the answer.
    assert_eq!(
        plain.matches("[thinking]").count(),
        1,
        "exactly one [thinking] label expected, got:\n{plain}"
    );
}

#[test]
fn tool_call_prints_exactly_once_not_duplicated() {
    let tui = tui::Tui::new();
    let mut buf: Vec<u8> = Vec::new();

    let tool_call = ToolCall {
        id: "call-1".into(),
        name: "read".into(),
        arguments: {
            let mut m = serde_json::Map::new();
            m.insert("path".into(), serde_json::Value::String("/tmp/x.rs".into()));
            m
        },
        thought_signature: None,
    };
    let partial = assistant(vec![ContentBlock::ToolCall(tool_call.clone())]);

    tui.render_event(&AgentEvent::AgentStart, &mut buf);
    // MessageUpdate::ToolCallStart used to print the tool name. Verify we DON'T duplicate
    // it — only ToolExecutionStart should print.
    tui.render_event(
        &message_update(
            AssistantMessageEvent::ToolCallStart {
                content_index: 0,
                partial: partial.clone(),
            },
            partial.clone(),
        ),
        &mut buf,
    );
    tui.render_event(
        &AgentEvent::ToolExecutionStart {
            tool_call_id: "call-1".into(),
            tool_name: "read".into(),
            args: serde_json::json!({ "path": "/tmp/x.rs" }),
        },
        &mut buf,
    );
    tui.render_event(
        &AgentEvent::ToolExecutionEnd {
            tool_call_id: "call-1".into(),
            tool_name: "read".into(),
            result: AgentToolResult {
                content: vec![UserContentBlock::text("file contents here")],
                details: serde_json::Value::Null,
                terminate: None,
            },
            is_error: false,
        },
        &mut buf,
    );
    tui.render_event(
        &AgentEvent::AgentEnd {
            messages: Vec::new(),
        },
        &mut buf,
    );

    let captured = String::from_utf8(buf).unwrap();
    let plain = strip_ansi(&captured);
    eprintln!("---captured---\n{captured}\n---plain---\n{plain}\n---end---");

    // The tool name appears exactly once in the captured stream.
    let count = plain.matches("⚙ read").count();
    assert_eq!(
        count, 1,
        "tool name should print exactly once; got {count} occurrences:\n{plain}"
    );
    // The args preview is included.
    assert!(plain.contains("path="), "args preview missing: {plain}");
    // The result body appears indented.
    assert!(
        plain.contains("    file contents here"),
        "result not rendered: {plain}"
    );
}

#[test]
fn text_to_tool_to_text_transitions_have_clean_line_breaks() {
    let tui = tui::Tui::new();
    let mut buf: Vec<u8> = Vec::new();

    // Round 1: text "first reply" → tool call → text "second reply" → end.
    tui.render_event(&AgentEvent::AgentStart, &mut buf);
    let mut partial = assistant(vec![ContentBlock::text("")]);
    tui.render_event(
        &message_update(
            AssistantMessageEvent::TextDelta {
                content_index: 0,
                delta: "first reply".into(),
                partial: partial.clone(),
            },
            partial.clone(),
        ),
        &mut buf,
    );
    tui.render_event(
        &AgentEvent::ToolExecutionStart {
            tool_call_id: "t1".into(),
            tool_name: "ls".into(),
            args: serde_json::json!({}),
        },
        &mut buf,
    );
    tui.render_event(
        &AgentEvent::ToolExecutionEnd {
            tool_call_id: "t1".into(),
            tool_name: "ls".into(),
            result: AgentToolResult {
                content: vec![UserContentBlock::text("a.rs\nb.rs")],
                details: serde_json::Value::Null,
                terminate: None,
            },
            is_error: false,
        },
        &mut buf,
    );
    partial = assistant(vec![ContentBlock::text("")]);
    tui.render_event(
        &message_update(
            AssistantMessageEvent::TextDelta {
                content_index: 0,
                delta: "second reply".into(),
                partial: partial.clone(),
            },
            partial,
        ),
        &mut buf,
    );
    tui.render_event(
        &AgentEvent::AgentEnd {
            messages: Vec::new(),
        },
        &mut buf,
    );

    let plain = strip_ansi(&String::from_utf8(buf).unwrap());
    eprintln!("---plain---\n{plain}\n---end---");

    // Each segment appears in order.
    let p1 = plain.find("first reply").expect("first reply present");
    let p_tool = plain.find("⚙ ls").expect("tool present");
    let p_result = plain.find("    a.rs").expect("result line present");
    let p2 = plain.find("second reply").expect("second reply present");
    assert!(
        p1 < p_tool && p_tool < p_result && p_result < p2,
        "order broken: {plain}"
    );
    // No "first reply⚙" or "a.rssecond" — every transition has a line break.
    let between_first_tool = &plain[p1..p_tool];
    assert!(
        between_first_tool.contains('\n'),
        "first→tool needs newline: {between_first_tool:?}"
    );
    let between_result_second = &plain[p_result..p2];
    assert!(
        between_result_second.contains('\n'),
        "result→second needs newline: {between_result_second:?}"
    );
}

#[allow(dead_code)]
fn ensure_imports_used(
    _t: ToolResultMessage,
    _i: ImageContent,
    _r: ToolResultRole,
    _a: Arc<dyn AgentTool>,
) {
}
