//! End-to-end AgentHarness test. Wires Agent + Session + a synthetic StreamFn and verifies the
//! prompt → assistant → session-persist cycle.

use std::sync::Arc;

use pie_agent_core::{
    AgentHarness, AgentHarnessOptions, MemorySessionStorage, Session, SessionStorage, Skill,
    StreamFn, ThinkingLevel,
};
use pie_ai::{
    AssistantMessage, AssistantMessageEvent, AssistantMessageEventStream, AssistantRole,
    ContentBlock, DoneReason, ModelCost, StopReason, Usage,
};

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

fn faux_stream_fn(text: &'static str) -> StreamFn {
    Arc::new(move |_, _, _| {
        let (stream, mut sender) = AssistantMessageEventStream::new();
        tokio::spawn(async move {
            let msg = AssistantMessage {
                role: AssistantRole::Assistant,
                content: vec![ContentBlock::text(text)],
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
            };
            sender.push(AssistantMessageEvent::Start {
                partial: msg.clone(),
            });
            sender.push(AssistantMessageEvent::Done {
                reason: DoneReason::Stop,
                message: msg,
            });
        });
        stream
    })
}

#[tokio::test]
async fn prompt_persists_user_and_assistant_to_session() {
    let storage = Arc::new(MemorySessionStorage::new());
    let session = Session::new(storage.clone() as Arc<dyn SessionStorage>);

    let mut opts = AgentHarnessOptions::new(faux_model(), session.clone());
    opts.system_prompt = "You are helpful.".into();
    opts.stream_fn = Some(faux_stream_fn("hello world"));
    let harness = AgentHarness::new(opts);

    assert!(harness.system_prompt().starts_with("You are helpful."));
    harness.prompt("hi").await.unwrap();

    let entries = session.entries().await.unwrap();
    // Should contain: user message + assistant message (both AgentMessage::Llm).
    assert!(
        entries.len() >= 2,
        "expected at least 2 entries, got {}",
        entries.len()
    );
    let has_assistant = entries.iter().any(|e| {
        matches!(
            e,
            pie_agent_core::SessionTreeEntry::Message {
                message: pie_agent_core::AgentMessage::Llm(pie_ai::Message::Assistant(_)),
                ..
            }
        )
    });
    assert!(has_assistant);
}

#[tokio::test]
async fn skills_block_appears_in_system_prompt() {
    let storage = Arc::new(MemorySessionStorage::new());
    let session = Session::new(storage as Arc<dyn SessionStorage>);

    let skill = Skill {
        name: "my-skill".into(),
        description: "does things".into(),
        file_path: "/skills/my-skill/SKILL.md".into(),
        content: "the body".into(),
        disable_model_invocation: false,
    };
    let mut opts = AgentHarnessOptions::new(faux_model(), session);
    opts.system_prompt = "Base.".into();
    opts.thinking_level = ThinkingLevel::Medium;
    opts.skills = vec![skill];
    opts.stream_fn = Some(faux_stream_fn("ok"));
    let harness = AgentHarness::new(opts);

    let prompt = harness.system_prompt();
    assert!(prompt.starts_with("Base."));
    assert!(prompt.contains("<skills>"));
    assert!(prompt.contains("- name: my-skill"));
}

#[tokio::test]
async fn set_model_persists_to_session() {
    let storage = Arc::new(MemorySessionStorage::new());
    let session = Session::new(storage as Arc<dyn SessionStorage>);

    let model_a = faux_model();
    let mut opts = AgentHarnessOptions::new(model_a.clone(), session.clone());
    opts.stream_fn = Some(faux_stream_fn("ok"));
    let harness = AgentHarness::new(opts);

    let mut model_b = faux_model();
    model_b.id = "faux-v2".into();
    harness.set_model(model_b.clone()).await.unwrap();
    harness
        .set_thinking_level(pie_agent_core::ThinkingLevel::Medium)
        .await
        .unwrap();

    let entries = session.entries().await.unwrap();
    assert!(entries.iter().any(|e| matches!(e,
        pie_agent_core::SessionTreeEntry::ModelChange { model_id, .. } if model_id == "faux-v2"
    )));
    assert!(entries.iter().any(|e| matches!(e,
        pie_agent_core::SessionTreeEntry::ThinkingLevelChange { thinking_level, .. } if thinking_level == "medium"
    )));
}

#[tokio::test]
async fn prompt_from_template_interpolates_and_runs() {
    use pie_agent_core::PromptTemplate;
    let storage = Arc::new(MemorySessionStorage::new());
    let session = Session::new(storage as Arc<dyn SessionStorage>);

    let mut opts = AgentHarnessOptions::new(faux_model(), session.clone());
    opts.stream_fn = Some(faux_stream_fn("template-resp"));
    opts.prompt_templates = vec![PromptTemplate {
        name: "greet".into(),
        description: None,
        content: "Say hi to {{name}}".into(),
        file_path: "/tpl/greet.md".into(),
    }];
    let harness = AgentHarness::new(opts);

    let mut vars = serde_json::Map::new();
    vars.insert("name".into(), serde_json::json!("world"));
    harness.prompt_from_template("greet", vars).await.unwrap();

    // First persisted user message should have the interpolated text.
    let entries = session.entries().await.unwrap();
    let has_interpolated = entries.iter().any(|e| match e {
        pie_agent_core::SessionTreeEntry::Message {
            message: pie_agent_core::AgentMessage::Llm(pie_ai::Message::User(u)),
            ..
        } => matches!(&u.content, pie_ai::UserContent::Text(s) if s == "Say hi to world"),
        _ => false,
    });
    assert!(
        has_interpolated,
        "expected interpolated user message; entries={:#?}",
        entries
    );
}
