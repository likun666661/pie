//! Terminal output helpers + AgentEvent renderer. Modeled on the TS interactive mode but kept
//! deliberately spartan: no widgets, no scrollback, just colored line-stream output.
//!
//! All formatting goes through crossterm so it's cross-platform and degrades gracefully on
//! non-TTY targets. We never enable raw mode; the REPL uses plain `stdin().lock().read_line`,
//! which means Ctrl-C interrupts the whole process — fine for a "simple" agent.

use std::io::Write as _;
use std::sync::Arc;

use crossterm::style::{Color, Print, ResetColor, SetAttribute, SetForegroundColor, Attribute};
use crossterm::{ExecutableCommand, QueueableCommand};
use parking_lot::Mutex;
use pie_agent_core::{AgentEvent, AgentListener, AgentMessage};
use pie_ai::{AssistantMessageEvent, ContentBlock, ImageContent, Message, UserContent, UserContentBlock};

#[derive(Default)]
struct RenderState {
    /// Streamed text emitted so far this turn (so we can decide when to insert a newline).
    text_open: bool,
    /// True while a thinking block is being streamed.
    thinking_open: bool,
}

#[derive(Clone)]
pub struct Tui {
    state: Arc<Mutex<RenderState>>,
}

impl Tui {
    pub fn new() -> Self {
        Self { state: Arc::new(Mutex::new(RenderState::default())) }
    }

    pub fn banner(&self, model: &pie_ai::Model, session_id: &str, resumed: bool) {
        let mut out = std::io::stdout();
        let _ = out.execute(SetForegroundColor(Color::Magenta));
        let _ = out.execute(Print("──────── pie-coding-agent ────────\n"));
        let _ = out.execute(ResetColor);
        println!(
            "model:   {} ({}/{})",
            model.name, model.provider.0, model.id
        );
        println!("session: {session_id}{}", if resumed { "  [resumed]" } else { "" });
        println!("tools:   read, write, bash, ls, memory");
        println!("type a message and press Enter. Ctrl-C to quit.\n");
    }

    pub fn user_prompt_marker(&self) {
        let mut out = std::io::stdout();
        let _ = out.execute(SetForegroundColor(Color::Cyan));
        let _ = out.execute(Print("\nyou> "));
        let _ = out.execute(ResetColor);
        let _ = out.flush();
    }

    pub fn system_line(&self, text: &str) {
        let mut out = std::io::stdout();
        let _ = out.execute(SetForegroundColor(Color::DarkGrey));
        let _ = out.execute(Print(format!("[{text}]\n")));
        let _ = out.execute(ResetColor);
    }

    pub fn error_line(&self, text: &str) {
        let mut out = std::io::stdout();
        let _ = out.execute(SetForegroundColor(Color::Red));
        let _ = out.execute(Print(format!("[error] {text}\n")));
        let _ = out.execute(ResetColor);
    }

    /// Build an `AgentListener` that prints lifecycle events. Holds onto `self` (cheaply
    /// clonable Arc state) so deltas accumulate across events.
    pub fn listener(&self) -> AgentListener {
        let me = self.clone();
        Arc::new(move |event, _cancel| {
            let me = me.clone();
            Box::pin(async move {
                me.handle_event(&event);
            })
        })
    }

    fn handle_event(&self, event: &AgentEvent) {
        let mut out = std::io::stdout();
        match event {
            AgentEvent::AgentStart => {
                self.state.lock().text_open = false;
                self.state.lock().thinking_open = false;
                let _ = out.queue(SetForegroundColor(Color::Green));
                let _ = out.queue(Print("\npi> "));
                let _ = out.queue(ResetColor);
                let _ = out.flush();
            }
            AgentEvent::AgentEnd { .. } => {
                let _ = out.execute(Print("\n"));
            }
            AgentEvent::MessageUpdate { assistant_message_event, .. } => {
                match assistant_message_event {
                    AssistantMessageEvent::TextDelta { delta, .. } => {
                        if self.state.lock().thinking_open {
                            let _ = out.queue(SetAttribute(Attribute::Reset));
                            let _ = out.queue(Print("\n"));
                            self.state.lock().thinking_open = false;
                        }
                        self.state.lock().text_open = true;
                        let _ = out.execute(Print(delta));
                    }
                    AssistantMessageEvent::ThinkingDelta { delta, .. } => {
                        if !self.state.lock().thinking_open {
                            let _ = out.queue(SetForegroundColor(Color::DarkGrey));
                            let _ = out.queue(SetAttribute(Attribute::Italic));
                            let _ = out.queue(Print("\n[thinking] "));
                            self.state.lock().thinking_open = true;
                        }
                        let _ = out.execute(Print(delta));
                    }
                    AssistantMessageEvent::ToolCallStart { partial, content_index } => {
                        if self.state.lock().thinking_open {
                            let _ = out.queue(SetAttribute(Attribute::Reset));
                            let _ = out.queue(Print("\n"));
                            self.state.lock().thinking_open = false;
                        }
                        if let Some(ContentBlock::ToolCall(tc)) = partial.content.get(*content_index) {
                            let _ = out.queue(SetForegroundColor(Color::Yellow));
                            let _ = out.queue(Print(format!("\n⚙ {}", tc.name)));
                            let _ = out.queue(ResetColor);
                            let _ = out.flush();
                        }
                    }
                    _ => {}
                }
            }
            AgentEvent::ToolExecutionStart { tool_name, args, .. } => {
                let arg_preview = preview(args);
                let _ = out.queue(SetForegroundColor(Color::Yellow));
                let _ = out.queue(Print(format!("\n  ⚙ {tool_name}{arg_preview}\n")));
                let _ = out.queue(ResetColor);
                let _ = out.flush();
            }
            AgentEvent::ToolExecutionEnd { tool_name: _, result, is_error, .. } => {
                let color = if *is_error { Color::Red } else { Color::DarkGreen };
                let _ = out.queue(SetForegroundColor(color));
                for block in &result.content {
                    if let UserContentBlock::Text(t) = block {
                        for line in t.text.lines() {
                            let _ = out.queue(Print(format!("    {line}\n")));
                        }
                    }
                }
                let _ = out.queue(ResetColor);
                let _ = out.flush();
            }
            _ => {}
        }
    }
}

fn preview(args: &serde_json::Value) -> String {
    if let Some(obj) = args.as_object() {
        let mut parts = Vec::new();
        for (k, v) in obj.iter().take(3) {
            let val = match v {
                serde_json::Value::String(s) => {
                    let mut s = s.replace('\n', "\\n");
                    if s.len() > 60 {
                        s.truncate(60);
                        s.push('…');
                    }
                    format!("\"{s}\"")
                }
                _ => {
                    let mut s = v.to_string();
                    if s.len() > 60 {
                        s.truncate(60);
                        s.push('…');
                    }
                    s
                }
            };
            parts.push(format!("{k}={val}"));
        }
        if obj.len() > 3 {
            parts.push("…".into());
        }
        format!("({})", parts.join(", "))
    } else {
        String::new()
    }
}

/// Render a persisted user/assistant message from a session replay. Used when --resume hands
/// us a transcript to redisplay before opening the REPL.
pub fn render_persisted(message: &AgentMessage) {
    let mut out = std::io::stdout();
    match message {
        AgentMessage::Llm(Message::User(u)) => {
            let _ = out.execute(SetForegroundColor(Color::Cyan));
            let _ = out.execute(Print("\nyou> "));
            let _ = out.execute(ResetColor);
            match &u.content {
                UserContent::Text(s) => {
                    let _ = out.execute(Print(format!("{s}\n")));
                }
                UserContent::Blocks(blocks) => {
                    for b in blocks {
                        match b {
                            UserContentBlock::Text(t) => {
                                let _ = out.execute(Print(format!("{}\n", t.text)));
                            }
                            UserContentBlock::Image(ImageContent { mime_type, .. }) => {
                                let _ = out.execute(Print(format!("<image {mime_type}>\n")));
                            }
                        }
                    }
                }
            }
        }
        AgentMessage::Llm(Message::Assistant(a)) => {
            let _ = out.execute(SetForegroundColor(Color::Green));
            let _ = out.execute(Print("\npi> "));
            let _ = out.execute(ResetColor);
            for b in &a.content {
                match b {
                    ContentBlock::Text(t) => {
                        let _ = out.execute(Print(format!("{}\n", t.text)));
                    }
                    ContentBlock::Thinking(t) => {
                        let _ = out.execute(SetForegroundColor(Color::DarkGrey));
                        let _ = out.execute(SetAttribute(Attribute::Italic));
                        let _ = out.execute(Print(format!("[thinking] {}\n", t.thinking)));
                        let _ = out.execute(SetAttribute(Attribute::Reset));
                        let _ = out.execute(ResetColor);
                    }
                    ContentBlock::ToolCall(tc) => {
                        let _ = out.execute(SetForegroundColor(Color::Yellow));
                        let _ = out.execute(Print(format!("⚙ {}({})\n", tc.name, preview(&serde_json::Value::Object(tc.arguments.clone())))));
                        let _ = out.execute(ResetColor);
                    }
                    ContentBlock::Image(_) => {}
                }
            }
        }
        AgentMessage::Llm(Message::ToolResult(tr)) => {
            let color = if tr.is_error { Color::Red } else { Color::DarkGreen };
            let _ = out.execute(SetForegroundColor(color));
            let _ = out.execute(Print(format!("  ⤷ {} →\n", tr.tool_name)));
            for b in &tr.content {
                if let UserContentBlock::Text(t) = b {
                    for line in t.text.lines() {
                        let _ = out.execute(Print(format!("    {line}\n")));
                    }
                }
            }
            let _ = out.execute(ResetColor);
        }
        AgentMessage::Custom(_) => {}
    }
}
