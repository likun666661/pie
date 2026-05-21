//! End-to-end tool tests. The tools are simple enough that we can exercise them directly
//! through their `AgentTool::execute` method without going through the agent loop.

use pie_agent_core::AgentTool;
use std::sync::Arc;
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

// Pull tool types out of the binary crate by including the source. Test-only.
#[path = "../src/tools/mod.rs"]
#[allow(dead_code)]
mod tools;

#[tokio::test]
async fn read_writes_then_reads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("hello.txt");

    let write = tools::write::WriteTool;
    let read = tools::read::ReadTool;

    write
        .execute(
            "w1",
            serde_json::json!({ "path": path.to_str().unwrap(), "content": "hi\nthere\n" }),
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();

    let r = read
        .execute(
            "r1",
            serde_json::json!({ "path": path.to_str().unwrap() }),
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
    let text = match &r.content[0] {
        pie_ai::UserContentBlock::Text(t) => t.text.clone(),
        _ => panic!("expected text"),
    };
    assert!(text.contains("hi"));
    assert!(text.contains("there"));
}

#[tokio::test]
async fn ls_lists_entries() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), "a").unwrap();
    std::fs::create_dir(dir.path().join("sub")).unwrap();

    let ls = tools::ls::LsTool;
    let r = ls
        .execute(
            "l1",
            serde_json::json!({ "path": dir.path().to_str().unwrap() }),
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
    let text = match &r.content[0] {
        pie_ai::UserContentBlock::Text(t) => t.text.clone(),
        _ => panic!("expected text"),
    };
    assert!(text.contains("a.txt"));
    assert!(text.contains("sub/"));
}

#[tokio::test]
async fn bash_captures_stdout_and_exit() {
    let bash = tools::bash::BashTool;
    let r = bash
        .execute(
            "b1",
            serde_json::json!({ "command": "echo hello && exit 0" }),
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
    let text = match &r.content[0] {
        pie_ai::UserContentBlock::Text(t) => t.text.clone(),
        _ => panic!("expected text"),
    };
    assert!(text.contains("hello"));
    assert!(text.contains("[exit 0]"));
}

#[tokio::test]
async fn bash_reports_nonzero_exit() {
    let bash = tools::bash::BashTool;
    let r = bash
        .execute(
            "b2",
            serde_json::json!({ "command": "exit 3" }),
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
    let text = match &r.content[0] {
        pie_ai::UserContentBlock::Text(t) => t.text.clone(),
        _ => panic!("expected text"),
    };
    assert!(text.contains("[exit 3]"));
}

#[tokio::test]
async fn memory_save_then_load_block() {
    let dir = tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();
    let mem: Arc<dyn AgentTool> = Arc::new(tools::memory::MemoryTool::new(dir_path.clone()));

    mem.execute(
        "m1",
        serde_json::json!({
            "action": "save",
            "name": "User Likes Tabs",
            "description": "indentation preference",
            "content": "The user prefers tabs over spaces.",
            "type": "user",
        }),
        CancellationToken::new(),
        None,
    )
    .await
    .unwrap();

    let block = tools::memory::load_memory_block(&dir_path).await;
    assert!(block.contains("<memory>"));
    assert!(block.contains("tabs"));
    assert!(block.contains("</memory>"));
}
