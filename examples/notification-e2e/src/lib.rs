//! Phase-1 protocol/demo smoke for the external-notification trigger work.
//!
//! IMPORTANT: This crate is intentionally isolated from the production trigger types that will
//! land in RFC 1 (issue #20). The `DemoTrigger` here is phase-1-only and must NOT be treated
//! as a production schema commitment. Phase 2 will replace it with `pie_agent_core::Trigger`.

pub mod envelope;
pub mod redaction;
