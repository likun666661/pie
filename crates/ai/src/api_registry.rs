//! Provider registry. 1:1 port of `packages/ai/src/api-registry.ts`.
//!
//! Per Q2:A the registry stores trait objects (`Box<dyn ApiProvider>`). Each provider declares
//! the `api` string it serves, and a mismatched call (`model.api != provider.api`) returns an
//! error stream (not a panic) — matching the TS `wrapStream` wrapper.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use async_trait::async_trait;

use crate::types::{Api, Context, Model, SimpleStreamOptions, StreamOptions};
use crate::utils::event_stream::AssistantMessageEventStream;

/// Provider trait. Each wire-protocol implementation is a `Box<dyn ApiProvider>` registered in
/// the global registry. `async_trait` lets us write async methods; the `Send + Sync` bounds
/// come from the `#[async_trait]` macro's expansion.
#[async_trait]
pub trait ApiProvider: Send + Sync {
    /// The wire-protocol identifier this provider serves (e.g. `"anthropic-messages"`).
    fn api(&self) -> &str;

    /// Provider-specific streaming entry point. Caller passes `StreamOptions` whose
    /// `provider_extras` map may include vendor-specific knobs.
    fn stream(
        &self,
        model: &Model,
        context: &Context,
        options: Option<&StreamOptions>,
    ) -> AssistantMessageEventStream;

    /// Universal streaming entry point. Each provider translates `SimpleStreamOptions` into
    /// whatever its own knobs are.
    fn stream_simple(
        &self,
        model: &Model,
        context: &Context,
        options: Option<&SimpleStreamOptions>,
    ) -> AssistantMessageEventStream;
}

struct RegisteredProvider {
    provider: Box<dyn ApiProvider>,
    source_id: Option<String>,
}

struct Registry {
    entries: HashMap<String, RegisteredProvider>,
}

fn registry() -> &'static Mutex<Registry> {
    static CELL: OnceLock<Mutex<Registry>> = OnceLock::new();
    CELL.get_or_init(|| {
        Mutex::new(Registry {
            entries: HashMap::new(),
        })
    })
}

pub fn register_api_provider(provider: Box<dyn ApiProvider>, source_id: Option<String>) {
    let mut reg = registry().lock().expect("registry poisoned");
    reg.entries.insert(
        provider.api().to_string(),
        RegisteredProvider {
            provider,
            source_id,
        },
    );
}

/// Lookup. Returns a handle that delegates to the registered provider while holding no lock —
/// we clone-by-reference using a shim because trait objects in a `MutexGuard` can't outlive the
/// guard. The shim takes a function pointer that re-acquires the lock for each call. This is
/// the Rust equivalent of TS returning the function reference directly.
pub fn get_api_provider(api: &Api) -> Option<RegisteredHandle> {
    let reg = registry().lock().expect("registry poisoned");
    if reg.entries.contains_key(&api.0) {
        Some(RegisteredHandle { api: api.0.clone() })
    } else {
        None
    }
}

pub fn unregister_api_providers(source_id: &str) {
    let mut reg = registry().lock().expect("registry poisoned");
    reg.entries
        .retain(|_, entry| entry.source_id.as_deref() != Some(source_id));
}

pub fn clear_api_providers() {
    let mut reg = registry().lock().expect("registry poisoned");
    reg.entries.clear();
}

/// Snapshot of currently-registered api ids. The TS `getApiProviders()` returns the internal
/// shim objects; we return ids to keep the lock scope tight.
pub fn list_api_ids() -> Vec<String> {
    let reg = registry().lock().expect("registry poisoned");
    reg.entries.keys().cloned().collect()
}

/// Handle returned by [`get_api_provider`]. Operations re-acquire the registry lock; this
/// matches the TS semantics where unregister-while-streaming is allowed (the in-flight stream
/// keeps working off the captured function reference). Here we keep it simple: each call must
/// re-resolve, panicking only if the provider has been removed between resolve and call.
pub struct RegisteredHandle {
    api: String,
}

impl RegisteredHandle {
    pub fn stream(
        &self,
        model: &Model,
        context: &Context,
        options: Option<&StreamOptions>,
    ) -> AssistantMessageEventStream {
        let reg = registry().lock().expect("registry poisoned");
        let entry = reg
            .entries
            .get(&self.api)
            .expect("provider removed while handle was held");
        // Mismatch guard: same as TS `wrapStream`.
        if model.api.0 != entry.provider.api() {
            return error_stream(format!(
                "Mismatched api: {} expected {}",
                model.api.0,
                entry.provider.api()
            ));
        }
        entry.provider.stream(model, context, options)
    }

    pub fn stream_simple(
        &self,
        model: &Model,
        context: &Context,
        options: Option<&SimpleStreamOptions>,
    ) -> AssistantMessageEventStream {
        let reg = registry().lock().expect("registry poisoned");
        let entry = reg
            .entries
            .get(&self.api)
            .expect("provider removed while handle was held");
        if model.api.0 != entry.provider.api() {
            return error_stream(format!(
                "Mismatched api: {} expected {}",
                model.api.0,
                entry.provider.api()
            ));
        }
        entry.provider.stream_simple(model, context, options)
    }
}

/// Construct an instantly-errored stream. Per the TS contract, providers must encode failures
/// in the returned stream rather than throw — same applies to the registry-level guard.
pub(crate) fn error_stream(message: String) -> AssistantMessageEventStream {
    use crate::types::*;
    let (stream, mut sender) = AssistantMessageEventStream::new();
    let err = AssistantMessage {
        role: AssistantRole::Assistant,
        content: vec![],
        api: Api::from(""),
        provider: Provider::from(""),
        model: String::new(),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::default(),
        stop_reason: StopReason::Error,
        error_message: Some(message),
        timestamp: chrono::Utc::now().timestamp_millis(),
    };
    sender.push(AssistantMessageEvent::Error {
        reason: ErrorReason::Error,
        error: err,
    });
    stream
}
