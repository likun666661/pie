//! Prompt-template discovery + interpolation. TODO: 1:1 port of
//! `packages/agent/src/harness/prompt-templates.ts` (~267 lines).
//!
//! Templates are `.md` files with YAML frontmatter. Same precedence model as skills (project →
//! user → builtin). Body uses `{{var}}` placeholder interpolation.

use super::types::PromptTemplate;

pub struct PromptTemplateRegistry {
    templates: Vec<PromptTemplate>,
}

impl PromptTemplateRegistry {
    pub fn new(templates: Vec<PromptTemplate>) -> Self {
        Self { templates }
    }

    pub fn list(&self) -> &[PromptTemplate] {
        &self.templates
    }

    pub fn get(&self, name: &str) -> Option<&PromptTemplate> {
        self.templates.iter().find(|t| t.name == name)
    }

    /// Interpolate `{{var}}` placeholders. Missing keys leave the placeholder verbatim.
    pub fn interpolate(
        template: &PromptTemplate,
        vars: &serde_json::Map<String, serde_json::Value>,
    ) -> String {
        let mut out = template.content.clone();
        for (k, v) in vars {
            let needle = format!("{{{{{k}}}}}");
            let value = match v {
                serde_json::Value::String(s) => s.clone(),
                _ => v.to_string(),
            };
            out = out.replace(&needle, &value);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interpolates_known_vars_and_leaves_unknown() {
        let t = PromptTemplate {
            name: "t".into(),
            description: None,
            content: "hi {{who}} — {{missing}}".into(),
            file_path: "/x".into(),
        };
        let mut vars = serde_json::Map::new();
        vars.insert("who".into(), serde_json::json!("world"));
        assert_eq!(
            PromptTemplateRegistry::interpolate(&t, &vars),
            "hi world — {{missing}}"
        );
    }
}
