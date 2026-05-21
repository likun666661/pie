//! Bedrock provider entry. 1:1 stub of `packages/ai/src/bedrock-provider.ts`. Separate from
//! `providers::amazon_bedrock` because Bedrock has a non-standard credential-resolution flow
//! (AWS sigv4) that warrants its own subpath export.

pub fn register() {
    // TODO: bedrock signing + credential chain wiring.
}
