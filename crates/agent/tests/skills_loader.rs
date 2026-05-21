//! End-to-end skill discovery against a real tempdir. Validates the SKILL.md walking, YAML
//! frontmatter parsing, name/parent-dir matching, and the system-prompt block format.

use pie_agent_core::{
    NativeEnv, SkillDiagnosticCode, format_skills_for_system_prompt, load_skills,
};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn discovers_skill_with_matching_parent_dir() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let skill_dir = root.join("my-skill");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: my-skill\ndescription: tells you things\n---\nBody body.",
    )
    .unwrap();

    let env = NativeEnv::new(root.to_string_lossy().to_string());
    let out = load_skills(&env, &[root.to_str().unwrap()], CancellationToken::new()).await;

    assert!(
        out.diagnostics.is_empty(),
        "unexpected diagnostics: {:?}",
        out.diagnostics
    );
    assert_eq!(out.skills.len(), 1);
    let s = &out.skills[0];
    assert_eq!(s.name, "my-skill");
    assert_eq!(s.description, "tells you things");
    assert_eq!(s.content, "Body body.");
}

#[tokio::test]
async fn missing_description_emits_diagnostic_and_skips() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let skill_dir = root.join("nodesc");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(skill_dir.join("SKILL.md"), "---\nname: nodesc\n---\nBody.").unwrap();

    let env = NativeEnv::new(root.to_string_lossy().to_string());
    let out = load_skills(&env, &[root.to_str().unwrap()], CancellationToken::new()).await;
    assert!(
        out.skills.is_empty(),
        "skills without description should be skipped"
    );
    assert!(
        out.diagnostics
            .iter()
            .any(|d| d.code == SkillDiagnosticCode::InvalidMetadata
                && d.message.contains("description is required")),
        "expected invalid_metadata diagnostic; got {:?}",
        out.diagnostics
    );
}

#[tokio::test]
async fn name_must_match_parent_dir() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let skill_dir = root.join("real-name");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(
        skill_dir.join("SKILL.md"),
        "---\nname: different\ndescription: x\n---\nBody.",
    )
    .unwrap();

    let env = NativeEnv::new(root.to_string_lossy().to_string());
    let out = load_skills(&env, &[root.to_str().unwrap()], CancellationToken::new()).await;
    // skill still loads (TS keeps it; only emits a warning), but a diagnostic flags the mismatch.
    assert!(
        out.diagnostics
            .iter()
            .any(|d| d.message.contains("does not match parent directory")),
        "expected name-mismatch diagnostic; got {:?}",
        out.diagnostics
    );
}

#[tokio::test]
async fn system_prompt_block_lists_each_skill() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    for name in ["alpha", "beta"] {
        let d = root.join(name);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(
            d.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: it does {name}\n---\nbody"),
        )
        .unwrap();
    }
    let env = NativeEnv::new(root.to_string_lossy().to_string());
    let out = load_skills(&env, &[root.to_str().unwrap()], CancellationToken::new()).await;
    assert_eq!(out.skills.len(), 2);
    let block = format_skills_for_system_prompt(&out.skills);
    assert!(block.starts_with("<skills>\n"));
    assert!(block.contains("- name: alpha"));
    assert!(block.contains("- name: beta"));
    assert!(block.ends_with("</skills>"));
}
