//! End-to-end test for the web_fetch tool. Spins up a tiny TCP listener that speaks just
//! enough HTTP/1.1 to serve a single response, then drives WebFetchTool::execute against it.
//! Asserts the rendered output contains stripped text + the expected status/content-type
//! header line.

use std::sync::Arc;

use pie_agent_core::{AgentTool, ToolExecutionMode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[path = "../src/tools/web_fetch.rs"]
mod web_fetch;

async fn spawn_http(
    body: &'static str,
    content_type: &'static str,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}/", addr);

    let handle = tokio::spawn(async move {
        if let Ok((mut sock, _)) = listener.accept().await {
            // Drain the request — just enough to consume headers; we don't parse it.
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                content_type,
                body
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.shutdown().await;
        }
    });
    (url, handle)
}

#[tokio::test]
async fn web_fetch_strips_html_to_text() {
    let html = "<html><body><h1>Hi</h1><p>Hello &amp; <b>world</b></p><script>evil()</script></body></html>";
    let (url, _server) = spawn_http(html, "text/html; charset=utf-8").await;
    let tool = web_fetch::WebFetchTool;
    let result = tool
        .execute(
            "call-1",
            serde_json::json!({ "url": url }),
            CancellationToken::new(),
            None,
        )
        .await
        .expect("web_fetch should succeed");
    let body = match &result.content[0] {
        pie_ai::UserContentBlock::Text(t) => t.text.clone(),
        _ => panic!("expected text content"),
    };
    assert!(body.contains("status: 200"), "status header line: {body}");
    assert!(body.contains("text/html"), "ctype header line: {body}");
    assert!(body.contains("Hi"), "missing heading: {body}");
    assert!(
        body.contains("Hello & world"),
        "missing decoded entity: {body}"
    );
    assert!(
        !body.contains("evil()"),
        "script body must be stripped: {body}"
    );
}

#[tokio::test]
async fn web_fetch_returns_plain_text_unchanged() {
    let txt = "raw plain text\nline two\n";
    let (url, _server) = spawn_http(txt, "text/plain").await;
    let tool = web_fetch::WebFetchTool;
    let result = tool
        .execute(
            "call-2",
            serde_json::json!({ "url": url }),
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap();
    let body = match &result.content[0] {
        pie_ai::UserContentBlock::Text(t) => t.text.clone(),
        _ => panic!("expected text content"),
    };
    assert!(body.contains("raw plain text"));
    assert!(body.contains("line two"));
}

#[tokio::test]
async fn web_fetch_missing_url_errors() {
    let tool = web_fetch::WebFetchTool;
    let err = tool
        .execute(
            "call-3",
            serde_json::json!({}),
            CancellationToken::new(),
            None,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("missing required arg"));
}

#[allow(dead_code)]
fn _exec_mode_is_parallel(t: &web_fetch::WebFetchTool) -> Option<ToolExecutionMode> {
    Arc::new(t).execution_mode()
}
