//! pie-coding-agent — minimal coding agent CLI on top of pie-agent-core.
//!
//! Modeled on `packages/coding-agent/` (the TS implementation) in spirit: same tools
//! (`read`/`write`/`bash`/`ls` + `memory`), same `--resume` semantics scoped by cwd hash, same
//! "interactive TUI" mode. Trimmed scope: no extensions, no skills loader, no themes, no
//! print/rpc/json modes.

mod agent_session;
mod config;
mod model;
mod session;
mod tools;
mod tui;

use std::io::{BufRead as _, Write as _};

use anyhow::{Context, Result, bail};
use clap::Parser;
use pie_agent_core::{
    AgentHarness, AgentHarnessOptions, AgentMessage, JsonlSessionRepo, Session, ThinkingLevel,
};
use pie_ai::Message as PiMessage;

#[derive(Parser, Debug)]
#[command(
    name = "pie",
    version,
    about = "Simple coding agent on top of pie-agent-core"
)]
struct Cli {
    /// Provider id (anthropic, openai, openrouter, …). When unset, auto-detected from env.
    #[arg(long)]
    provider: Option<String>,
    /// Model id within the provider's catalog.
    #[arg(long)]
    model: Option<String>,
    /// Thinking level (off | minimal | low | medium | high | xhigh).
    #[arg(long, default_value = "off")]
    thinking: String,

    /// Resume the most recent session for this cwd (or pass --resume-id for a specific one).
    #[arg(long)]
    resume: bool,
    /// Resume a specific session by id (full UUIDv7 or a unique prefix).
    #[arg(long, value_name = "ID")]
    resume_id: Option<String>,

    /// List sessions for this cwd and exit.
    #[arg(long)]
    list_sessions: bool,
    /// Delete a session by id and exit.
    #[arg(long, value_name = "ID")]
    delete_session: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cwd = std::env::current_dir().context("getting cwd")?;
    let repo = session::open_repo(&cwd).await;

    if cli.list_sessions {
        return list_sessions_cmd(&repo).await;
    }
    if let Some(id) = &cli.delete_session {
        return delete_session_cmd(&repo, id).await;
    }

    run_repl(cli, cwd, repo).await
}

async fn list_sessions_cmd(repo: &JsonlSessionRepo) -> Result<()> {
    let entries = session::list_entries(repo).await?;
    if entries.is_empty() {
        println!("(no sessions for this cwd)");
        return Ok(());
    }
    println!("sessions in {}:", repo.root().display());
    for e in entries {
        let preview = e.preview.as_deref().unwrap_or("");
        println!(
            "  {}  {}  {}",
            &e.id[..16.min(e.id.len())],
            e.created_at,
            preview
        );
    }
    Ok(())
}

async fn delete_session_cmd(repo: &JsonlSessionRepo, id: &str) -> Result<()> {
    let path = session::delete_by_id(repo, id).await?;
    println!("deleted {}", path.display());
    Ok(())
}

async fn run_repl(cli: Cli, cwd: std::path::PathBuf, repo: JsonlSessionRepo) -> Result<()> {
    let model = model::auto_detect_model(cli.provider.as_deref(), cli.model.as_deref())?;
    let thinking = parse_thinking(&cli.thinking)?;

    // Resolve / create the session.
    let (session, resumed) = if cli.resume || cli.resume_id.is_some() {
        let s = session::resume(&repo, cli.resume_id.as_deref()).await?;
        (s, true)
    } else {
        let s = session::create(&repo, &cwd).await?;
        (s, false)
    };
    let session_id = session
        .storage()
        .get_metadata_json()
        .await?
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("?")
        .to_string();

    // Build the harness.
    let memory_dir = config::memory_dir();
    let tools = tools::default_tools(memory_dir.clone());
    let memory_block = tools::memory::load_memory_block(&memory_dir).await;
    let system_prompt = compose_system_prompt(&cwd, &memory_block);

    let mut opts = AgentHarnessOptions::new(model.clone(), session.clone());
    opts.system_prompt = system_prompt;
    opts.thinking_level = thinking;
    opts.tools = tools;
    let harness = std::sync::Arc::new(AgentHarness::new(opts));
    let session_runner =
        agent_session::AgentSession::new(harness.clone(), agent_session::RetrySettings::default());

    // Banner + replay (if --resume).
    let tui = tui::Tui::new();
    tui.banner(&model, &session_id, resumed);
    if resumed {
        replay_transcript(&session, &harness, &tui).await?;
    }

    // Wire the TUI listener so each prompt's events stream live.
    let _unsub = harness.agent().subscribe(tui.listener());

    // REPL.
    let stdin = std::io::stdin();
    loop {
        tui.user_prompt_marker();
        let mut line = String::new();
        if stdin.lock().read_line(&mut line)? == 0 {
            tui.system_line("eof — exiting");
            break;
        }
        let input = line.trim();
        if input.is_empty() {
            continue;
        }
        if input == "/quit" || input == "/exit" || input == "/q" {
            tui.system_line("bye");
            break;
        }
        if input == "/help" {
            print_help();
            continue;
        }
        if input == "/clear" {
            // Clear screen via ANSI; conversation state is unchanged.
            print!("\x1b[2J\x1b[H");
            let _ = std::io::stdout().flush();
            continue;
        }
        if let Err(e) = session_runner.prompt(input.to_string()).await {
            tui.error_line(&format!("{e}"));
        }
    }
    Ok(())
}

fn print_help() {
    println!();
    println!("Commands:");
    println!("  /help          show this help");
    println!("  /clear         clear screen (keeps history)");
    println!("  /quit | /q     exit");
    println!();
    println!("Anything else is sent as a prompt to the agent.");
    println!();
}

fn parse_thinking(s: &str) -> Result<ThinkingLevel> {
    Ok(match s {
        "off" => ThinkingLevel::Off,
        "minimal" => ThinkingLevel::Minimal,
        "low" => ThinkingLevel::Low,
        "medium" => ThinkingLevel::Medium,
        "high" => ThinkingLevel::High,
        "xhigh" => ThinkingLevel::Xhigh,
        other => bail!("invalid thinking level: {other}"),
    })
}

fn compose_system_prompt(cwd: &std::path::Path, memory: &str) -> String {
    let mut s = String::new();
    s.push_str(BASE_PROMPT);
    s.push_str("\n\n");
    s.push_str(&format!("Current working directory: {}\n", cwd.display()));
    if !memory.is_empty() {
        s.push('\n');
        s.push_str(memory);
        s.push('\n');
    }
    s
}

const BASE_PROMPT: &str = "You are pie-coding-agent, a minimal coding assistant running in a terminal. You have access to filesystem and shell tools (read, write, bash, ls) plus a persistent cross-session memory tool. \
Prefer running a tool over guessing. When making file changes, use `read` first to confirm current contents, then `write`. Keep responses concise.";

/// Re-render a resumed session's transcript to stdout so the user sees the context they're
/// continuing from. The harness's `state.messages` already contains the replayed transcript
/// (we set it explicitly here from the session); we walk it for display.
async fn replay_transcript(
    session: &Session,
    harness: &AgentHarness,
    tui: &tui::Tui,
) -> Result<()> {
    let ctx = session.build_context().await?;
    if ctx.messages.is_empty() {
        return Ok(());
    }
    tui.system_line(&format!(
        "resumed — replaying {} messages",
        ctx.messages.len()
    ));
    // Hydrate the Agent state so the next prompt continues from this transcript.
    {
        let mut state = harness.agent().state();
        state.messages = ctx.messages.clone();
    }
    for m in &ctx.messages {
        tui::render_persisted(m);
    }
    // Skip custom variants (compaction_summary etc.); they aren't model-visible here. But the
    // harness uses them via convert_to_llm filtering — that's already handled by pie-agent-core.
    drop_unused(&ctx.messages);
    Ok(())
}

fn drop_unused(_: &[AgentMessage]) {}

/// Helper for callers that want to feed a Message (raw pie-ai role variant) into the agent. Not
/// directly used by the REPL but kept here for the tests.
pub fn user_message(text: &str) -> AgentMessage {
    AgentMessage::Llm(PiMessage::User(pie_ai::UserMessage {
        role: pie_ai::UserRole::User,
        content: pie_ai::UserContent::Text(text.into()),
        timestamp: chrono::Utc::now().timestamp_millis(),
    }))
}
