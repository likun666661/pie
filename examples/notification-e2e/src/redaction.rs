//! Redaction assertions.
//!
//! Phase 1 acceptance includes: a fixed fake hub token MUST never leak to stdout, stderr,
//! logs, or any serialized envelope. This module centralizes the assertion so all binaries
//! and tests use the same guard.

/// The single fake token used across phase-1 examples and tests. If it ever shows up in
/// output, redaction is broken.
pub const FAKE_HUB_TOKEN: &str = "fake-hub-token-should-not-leak";

/// Asserts that the given haystack does NOT contain `FAKE_HUB_TOKEN`. Use this on captured
/// stdout, captured stderr, serialized envelopes, and any other observable output.
///
/// Panics with a clear message on leak; this is what the tests are looking for.
pub fn assert_no_token_leak(haystack: &str, context: &str) {
    if haystack.contains(FAKE_HUB_TOKEN) {
        panic!(
            "redaction breach in {}: fake token bytes appeared in observable output. \
             Output had {} chars; first occurrence at byte offset {:?}.",
            context,
            haystack.len(),
            haystack.find(FAKE_HUB_TOKEN)
        );
    }
}

/// Returns a redacted display string for a token value. The on-wire frame should not contain
/// the raw token; this helper is only for places where we need to print or log a "this is a
/// token, do not show contents" placeholder.
pub fn redact(_token: &str) -> &'static str {
    "<redacted>"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_returns_placeholder() {
        assert_eq!(redact(FAKE_HUB_TOKEN), "<redacted>");
    }

    #[test]
    fn assert_no_token_leak_passes_when_clean() {
        assert_no_token_leak("safe content with no token", "test");
    }

    #[test]
    #[should_panic(expected = "redaction breach")]
    fn assert_no_token_leak_fails_when_present() {
        assert_no_token_leak(&format!("contains {}", FAKE_HUB_TOKEN), "test");
    }
}
