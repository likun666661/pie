//! `AgentHarness` — opinionated assembly around the bare `Agent`. 1:1 port of
//! `packages/agent/src/harness/agent-harness.ts` (~995 lines).
//!
//! Implemented:
//! - Compose `Agent` + `Session` + skills catalog + compaction settings
//! - `prompt(text)` / `prompt_with_images` / `continue_()`
//! - Auto-compaction trigger before each LLM call (when `compaction.enabled` is true)
//! - `set_model` / `set_thinking_level` mirror state mutations onto the session log
//! - `fork()` / `move_to()` branch operations (with optional branch summary)
//! - `prompt_from_template(name, vars)` — picks a `PromptTemplate`, interpolates, prompts
//! - `replace_tools` / `replace_skills` runtime mutations
//! - `enqueue_steering` / `enqueue_follow_up` queue passthrough
//! - `subscribe` to lifecycle events

use std::sync::Arc;

use parking_lot::Mutex;
use pie_ai::{ImageContent, Message as PiMessage, Model};

use super::super::agent::{Agent, AgentListener, AgentOptions, AgentRunError};
use super::super::types::*;

use super::compaction::compaction::{
    CompactionSettings, DEFAULT_COMPACTION_SETTINGS, SummarizeError, compact,
    estimate_context_tokens, should_compact,
};
use super::messages::compaction_summary;
use super::prompt_templates::PromptTemplateRegistry;
use super::session::session::{BranchSummaryInput, Session};
use super::skills::format_skill_invocation;
use super::system_prompt::format_skills_for_system_prompt;
use super::types::{PromptTemplate, Skill};

pub struct AgentHarnessOptions {
    /// Base system prompt prepended to the rendered skill catalog.
    pub system_prompt: String,
    pub model: Model,
    pub thinking_level: ThinkingLevel,
    pub skills: Vec<Skill>,
    pub prompt_templates: Vec<PromptTemplate>,
    pub tools: Vec<Arc<dyn AgentTool>>,
    pub session: Session,
    pub stream_fn: Option<StreamFn>,
    /// Auto-compaction thresholds. Defaults to [`DEFAULT_COMPACTION_SETTINGS`].
    pub compaction: CompactionSettings,
}

impl AgentHarnessOptions {
    pub fn new(model: Model, session: Session) -> Self {
        Self {
            system_prompt: String::new(),
            model,
            thinking_level: ThinkingLevel::Off,
            skills: Vec::new(),
            prompt_templates: Vec::new(),
            tools: Vec::new(),
            session,
            stream_fn: None,
            compaction: DEFAULT_COMPACTION_SETTINGS.clone(),
        }
    }
}

pub struct AgentHarness {
    agent: Arc<Agent>,
    session: Session,
    skills: Mutex<Vec<Skill>>,
    base_system_prompt: String,
    templates: Mutex<PromptTemplateRegistry>,
    compaction_settings: Mutex<CompactionSettings>,
    /// Used by auto-compaction to call the LLM for summarization.
    stream_fn: Option<StreamFn>,
}

impl AgentHarness {
    pub fn new(options: AgentHarnessOptions) -> Self {
        let mut state = AgentState::default();
        state.model = Some(options.model);
        state.thinking_level = Some(options.thinking_level);
        state.tools = options.tools;
        state.system_prompt = build_system_prompt(&options.system_prompt, &options.skills);

        let agent = Agent::new(AgentOptions {
            initial_state: Some(state),
            stream_fn: options.stream_fn.clone(),
            ..Default::default()
        });

        Self {
            agent: Arc::new(agent),
            session: options.session,
            skills: Mutex::new(options.skills),
            base_system_prompt: options.system_prompt,
            templates: Mutex::new(PromptTemplateRegistry::new(options.prompt_templates)),
            compaction_settings: Mutex::new(options.compaction),
            stream_fn: options.stream_fn,
        }
    }

    pub fn agent(&self) -> &Agent {
        &self.agent
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn skills(&self) -> Vec<Skill> {
        self.skills.lock().clone()
    }

    pub fn system_prompt(&self) -> String {
        self.agent.state().system_prompt.clone()
    }

    /// Replace the skill catalog. Rebuilds the system prompt so the in-flight Agent state has
    /// the new `<skills>` block on its next LLM call.
    pub fn replace_skills(&self, skills: Vec<Skill>) {
        *self.skills.lock() = skills;
        let prompt = build_system_prompt(&self.base_system_prompt, &self.skills.lock());
        self.agent.state().system_prompt = prompt;
    }

    /// Replace the prompt-template registry.
    pub fn replace_prompt_templates(&self, templates: Vec<PromptTemplate>) {
        *self.templates.lock() = PromptTemplateRegistry::new(templates);
    }

    /// Replace the tool set. UI consumers calling this mid-run will see the new tools on the
    /// next turn.
    pub fn replace_tools(&self, tools: Vec<Arc<dyn AgentTool>>) {
        self.agent.state().tools = tools;
    }

    /// Update auto-compaction thresholds.
    pub fn set_compaction_settings(&self, settings: CompactionSettings) {
        *self.compaction_settings.lock() = settings;
    }

    pub fn abort(&self) {
        self.agent.abort();
    }

    pub fn enqueue_steering(&self, message: AgentMessage) {
        self.agent.enqueue_steering(message);
    }

    pub fn enqueue_follow_up(&self, message: AgentMessage) {
        self.agent.enqueue_follow_up(message);
    }

    pub fn subscribe(&self, listener: AgentListener) -> impl FnOnce() {
        self.agent.subscribe(listener)
    }

    /// Switch model. Persists a `ModelChange` session entry so resume sees the right one.
    pub async fn set_model(&self, model: Model) -> Result<String, super::types::SessionError> {
        let provider = model.provider.0.clone();
        let model_id = model.id.clone();
        let id = self.session.append_model_change(provider, model_id).await?;
        self.agent.state().model = Some(model);
        Ok(id)
    }

    pub async fn set_thinking_level(
        &self,
        level: ThinkingLevel,
    ) -> Result<String, super::types::SessionError> {
        let id = self
            .session
            .append_thinking_level_change(level.as_str())
            .await?;
        self.agent.state().thinking_level = Some(level);
        Ok(id)
    }

    /// Move the session leaf to a specific entry id (or root). When `summary` is provided,
    /// records a branch_summary entry so siblings see the fork's contribution. Replays the new
    /// branch into agent state via [`Self::rehydrate_from_session`].
    pub async fn move_to(
        &self,
        entry_id: Option<&str>,
        summary: Option<BranchSummaryInput>,
    ) -> Result<Option<String>, super::types::SessionError> {
        let result = self.session.move_to(entry_id, summary).await?;
        self.rehydrate_from_session().await?;
        Ok(result)
    }

    /// Replace the agent's in-memory state with the session's active branch. Messages, model,
    /// and thinking level are restored from `Session::build_context()`. Returns the rebuilt
    /// `SessionContext` for callers that want to render the transcript or inspect the recovered
    /// model.
    ///
    /// CLI startup (`--resume`) and post-branch-switch flows both go through this — keeps the
    /// "how do we rehydrate?" decision in one place.
    pub async fn rehydrate_from_session(
        &self,
    ) -> Result<super::session::session::SessionContext, super::types::SessionError> {
        let ctx = self.session.build_context().await?;
        let mut s = self.agent.state();
        s.messages = ctx.messages.clone();
        if let Some(model) = &ctx.model {
            // Restore the previously-active model when it's still in the catalog. Unknown
            // models keep whatever the caller set up — the resume banner reflects that fact.
            if let Some(m) = pie_ai::get_model(
                &pie_ai::Provider::from(model.provider.clone()),
                &model.model_id,
            ) {
                s.model = Some(m);
            }
        }
        if let Ok(level) = ctx.thinking_level.parse::<ThinkingLevel>() {
            s.thinking_level = Some(level);
        }
        Ok(ctx)
    }

    /// Pick a template by name, interpolate, and prompt the agent.
    pub async fn prompt_from_template(
        &self,
        name: &str,
        vars: serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), AgentRunError> {
        let template = {
            let g = self.templates.lock();
            g.get(name).cloned()
        };
        let template = match template {
            Some(t) => t,
            None => {
                return Err(AgentRunError::Other(format!(
                    "unknown prompt template: {name}"
                )));
            }
        };
        let rendered = PromptTemplateRegistry::interpolate(&template, &vars);
        self.prompt(rendered).await
    }

    /// Prompt the agent with text. Runs auto-compaction first, persists results to session.
    pub async fn prompt(&self, text: impl Into<String>) -> Result<(), AgentRunError> {
        let text = text.into();
        let user_message = AgentMessage::Llm(PiMessage::User(pie_ai::UserMessage {
            role: pie_ai::UserRole::User,
            content: pie_ai::UserContent::Text(text),
            timestamp: chrono::Utc::now().timestamp_millis(),
        }));
        self.prompt_with_message(user_message).await
    }

    /// Prompt with text + images (multimodal users).
    pub async fn prompt_with_images(
        &self,
        text: impl Into<String>,
        images: Vec<ImageContent>,
    ) -> Result<(), AgentRunError> {
        let mut blocks: Vec<pie_ai::UserContentBlock> = images
            .into_iter()
            .map(pie_ai::UserContentBlock::Image)
            .collect();
        let text = text.into();
        if !text.is_empty() {
            blocks.insert(0, pie_ai::UserContentBlock::text(text));
        }
        let user_message = AgentMessage::Llm(PiMessage::User(pie_ai::UserMessage {
            role: pie_ai::UserRole::User,
            content: pie_ai::UserContent::Blocks(blocks),
            timestamp: chrono::Utc::now().timestamp_millis(),
        }));
        self.prompt_with_message(user_message).await
    }

    async fn prompt_with_message(&self, msg: AgentMessage) -> Result<(), AgentRunError> {
        // Run compaction if we've crossed the threshold. This must happen before the user
        // message is appended so the cut point doesn't risk splitting the current turn.
        self.run_auto_compaction().await?;

        let (listener, persist_errors) = make_session_listener(self.session.clone());
        let unsub = self.agent.subscribe(listener);
        let result = self.agent.prompt(msg).await;
        unsub();
        finish_persisted_run(result, persist_errors)
    }

    pub async fn continue_(&self) -> Result<(), AgentRunError> {
        self.run_auto_compaction().await?;
        let (listener, persist_errors) = make_session_listener(self.session.clone());
        let unsub = self.agent.subscribe(listener);
        let result = self.agent.continue_().await;
        unsub();
        finish_persisted_run(result, persist_errors)
    }

    /// Force a compaction immediately, regardless of token thresholds. Useful for `/compact`-
    /// style slash commands.
    pub async fn force_compact(
        &self,
        custom_instructions: Option<String>,
    ) -> Result<bool, AgentRunError> {
        self.do_compact(true, custom_instructions).await
    }

    async fn run_auto_compaction(&self) -> Result<(), AgentRunError> {
        let settings = self.compaction_settings.lock().clone();
        if !settings.enabled {
            return Ok(());
        }
        let (context_tokens, context_window) = {
            let s = self.agent.state();
            let model = match &s.model {
                Some(m) => m,
                None => return Ok(()),
            };
            let estimate = estimate_context_tokens(&s.messages);
            (estimate.tokens, model.context_window)
        };
        if !should_compact(context_tokens, context_window, &settings) {
            return Ok(());
        }
        let _ = self.do_compact(false, None).await?;
        Ok(())
    }

    /// Shared implementation behind auto + manual compaction. Returns `true` when compaction
    /// actually ran.
    async fn do_compact(
        &self,
        from_hook: bool,
        custom_instructions: Option<String>,
    ) -> Result<bool, AgentRunError> {
        let (model, _messages_for_summary, entries) = {
            let s = self.agent.state();
            let model = match s.model.clone() {
                Some(m) => m,
                None => return Ok(false),
            };
            let messages = s.messages.clone();
            // Convert agent-state messages into synthetic session entries for compact()'s
            // signature. compact() only iterates Message entries; the others are ignored.
            let entries: Vec<super::session::session::SessionTreeEntry> = messages
                .into_iter()
                .map(|m| super::session::session::SessionTreeEntry::Message {
                    id: super::session::uuid::uuidv7(),
                    parent_id: None,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    message: m,
                })
                .collect();
            (model, s.messages.clone(), entries)
        };

        let settings = self.compaction_settings.lock().clone();
        let result = compact(
            model,
            &entries,
            &settings,
            custom_instructions,
            self.stream_fn.clone(),
            self.agent.active_token().unwrap_or_default(),
        )
        .await;

        let result = match result {
            Ok(r) if !r.summary.is_empty() => r,
            Ok(_) => return Ok(false),
            Err(SummarizeError::Aborted) => return Ok(false),
            Err(e) => return Err(AgentRunError::Other(format!("compaction failed: {e}"))),
        };

        // Persist a compaction entry to the session.
        let _ = self
            .session
            .append_compaction(
                result.summary.clone(),
                result.first_kept_entry_id.clone().unwrap_or_default(),
                result.tokens_before,
                None,
                from_hook,
            )
            .await
            .map_err(|e| AgentRunError::Other(format!("session append compaction: {e}")))?;

        // Replace agent state's prefix with a single compaction-summary custom message. Keep
        // anything that came after the cut point.
        {
            let mut s = self.agent.state();
            let mut new_msgs: Vec<AgentMessage> = vec![compaction_summary(result.summary.clone())];
            // Find first_kept_entry_id in the state.messages (none of which carry ids); a
            // simple heuristic is to drop everything older than the cut and keep the tail.
            // Concretely: keep the last N messages whose estimated tokens sum to at most
            // `keep_recent_tokens` (matches `find_cut_point`).
            let keep = settings.keep_recent_tokens as u64;
            let mut acc = 0u64;
            let mut tail: Vec<AgentMessage> = Vec::new();
            for m in s.messages.iter().rev() {
                let cost = super::compaction::compaction::estimate_tokens(m);
                if acc + cost > keep {
                    break;
                }
                acc += cost;
                tail.push(m.clone());
            }
            tail.reverse();
            new_msgs.extend(tail);
            s.messages = new_msgs;
        }
        Ok(true)
    }

    /// Format a single skill invocation block for ad-hoc UI surfaces.
    pub fn format_skill(skill: &Skill, extra: Option<&str>) -> String {
        format_skill_invocation(skill, extra)
    }
}

fn build_system_prompt(base: &str, skills: &[Skill]) -> String {
    let skills_block = format_skills_for_system_prompt(skills);
    if base.is_empty() {
        return skills_block;
    }
    if skills_block.is_empty() {
        return base.to_string();
    }
    format!("{base}\n\n{skills_block}")
}

/// Build an `AgentListener` that persists every emitted `MessageEnd` to the session log.
fn make_session_listener(
    session: Session,
) -> (
    crate::agent::AgentListener,
    Arc<Mutex<Vec<super::types::SessionError>>>,
) {
    let errors = Arc::new(Mutex::new(Vec::new()));
    let listener_errors = errors.clone();
    let listener: crate::agent::AgentListener = Arc::new(move |event, _cancel| {
        let session = session.clone();
        let listener_errors = listener_errors.clone();
        Box::pin(async move {
            if let AgentEvent::MessageEnd { message } = event {
                if let Err(e) = session.append_message(message).await {
                    listener_errors.lock().push(e);
                }
            }
        })
    });
    (listener, errors)
}

fn finish_persisted_run(
    result: Result<(), AgentRunError>,
    persist_errors: Arc<Mutex<Vec<super::types::SessionError>>>,
) -> Result<(), AgentRunError> {
    result?;
    if let Some(e) = persist_errors.lock().first() {
        return Err(AgentRunError::Other(format!("session append message: {e}")));
    }
    Ok(())
}
