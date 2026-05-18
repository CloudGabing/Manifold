use serde::{Deserialize, Serialize};
use serde_yaml::Value as YamlValue;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub name: String,
    pub engine: String,
    pub code: String,
    #[serde(default)]
    pub inputs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub name: String,
    #[serde(default)]
    pub nodes: Vec<NodeConfig>,
}

const MAX_CONFIG_SIZE_BYTES: usize = 2 * 1024 * 1024;
const MAX_CONFIG_DEPTH: usize = 20;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to parse pipeline config: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("pipeline config payload too large: {0} bytes exceeds {1} byte limit")]
    PayloadTooLarge(usize, usize),
    #[error("pipeline config exceeds maximum nesting depth of {0}")]
    DepthExceeded(usize),
    #[error("duplicate node name: {0}")]
    DuplicateNode(String),
    #[error("invalid engine: {0}")]
    InvalidEngine(String),
    #[error("cycle detected in pipeline")]
    CycleDetected,
}

impl PipelineConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut node_names = HashSet::new();
        for node in &self.nodes {
            if node.name.trim().is_empty() {
                return Err(ConfigError::InvalidEngine(node.name.clone()));
            }

            if !node_names.insert(node.name.clone()) {
                return Err(ConfigError::DuplicateNode(node.name.clone()));
            }

            let engine = node.engine.trim();
            if engine.is_empty() {
                return Err(ConfigError::InvalidEngine(node.engine.clone()));
            }

            let allowed = ["uiua", "rust", "wasm"];
            if !allowed.contains(&engine) {
                return Err(ConfigError::InvalidEngine(node.engine.clone()));
            }
        }

        let mut incoming = HashMap::new();
        let mut outbound = HashMap::new();

        for node in &self.nodes {
            incoming.insert(node.name.as_str(), 0usize);
            outbound.insert(node.name.as_str(), Vec::new());
        }

        for node in &self.nodes {
            for input in &node.inputs {
                if incoming.contains_key(input.as_str()) {
                    outbound.get_mut(input.as_str()).unwrap().push(node.name.as_str());
                    *incoming.get_mut(node.name.as_str()).unwrap() += 1;
                }
            }
        }

        let mut ready: Vec<&str> = incoming
            .iter()
            .filter(|(_, &count)| count == 0)
            .map(|(name, _)| *name)
            .collect();

        let mut processed = 0;
        while let Some(name) = ready.pop() {
            processed += 1;
            for dependent in &outbound[name] {
                let count = incoming.get_mut(dependent).unwrap();
                *count -= 1;
                if *count == 0 {
                    ready.push(dependent);
                }
            }
        }

        if processed != self.nodes.len() {
            return Err(ConfigError::CycleDetected);
        }

        Ok(())
    }
}

fn validate_yaml_depth(value: &YamlValue, depth: usize) -> Result<(), ConfigError> {
    if depth > MAX_CONFIG_DEPTH {
        return Err(ConfigError::DepthExceeded(MAX_CONFIG_DEPTH));
    }

    match value {
        YamlValue::Sequence(sequence) => {
            for item in sequence {
                validate_yaml_depth(item, depth + 1)?;
            }
        }
        YamlValue::Mapping(mapping) => {
            for (key, value) in mapping {
                validate_yaml_depth(key, depth + 1)?;
                validate_yaml_depth(value, depth + 1)?;
            }
        }
        _ => {}
    }

    Ok(())
}

pub fn parse_config(yaml_str: &str) -> Result<PipelineConfig, ConfigError> {
    if yaml_str.len() > MAX_CONFIG_SIZE_BYTES {
        return Err(ConfigError::PayloadTooLarge(yaml_str.len(), MAX_CONFIG_SIZE_BYTES));
    }

    let yaml_value: YamlValue = serde_yaml::from_str(yaml_str)?;
    validate_yaml_depth(&yaml_value, 0)?;
    let config: PipelineConfig = serde_yaml::from_value(yaml_value)?;
    config.validate()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_validate_pipeline_config() {
        let yaml = r#"
name: demo
nodes:
  - name: generate
    engine: uiua
    code: |
      [1 2 3]
    inputs: []
  - name: sum
    engine: uiua
    code: |
      +
    inputs:
      - generate
"#;

        let config = parse_config(yaml).expect("config must parse");
        assert_eq!(config.name, "demo");
        assert_eq!(config.nodes.len(), 2);
        assert_eq!(config.nodes[0].name, "generate");
        assert_eq!(config.nodes[1].inputs, vec!["generate".to_string()]);
    }

    #[test]
    fn reject_duplicate_node_names() {
        let yaml = r#"
name: bad
nodes:
  - name: duplicate
    engine: uiua
    code: "1"
    inputs: []
  - name: duplicate
    engine: uiua
    code: "2"
    inputs: []
"#;

        let error = parse_config(yaml).unwrap_err();
        assert!(matches!(error, ConfigError::DuplicateNode(_)));
    }

    #[test]
    fn reject_payload_over_limit() {
        let mut yaml = String::from("name: big\nnodes: []\n");
        // This line is where we replace repeat().take() with repeat_n()
        yaml.extend(std::iter::repeat_n('a', MAX_CONFIG_SIZE_BYTES + 1));
        let error = parse_config(&yaml).unwrap_err();
        assert!(matches!(error, ConfigError::PayloadTooLarge(_, _)));
    }

    #[test]
    fn reject_too_deep_yaml() {
        let mut yaml = String::new();
        for depth in 0..(MAX_CONFIG_DEPTH + 2) {
            yaml.push_str(&"  ".repeat(depth));
            yaml.push_str("nested:\n");
        }
        let error = parse_config(&yaml).unwrap_err();
        assert!(matches!(error, ConfigError::DepthExceeded(_)));
    }
}
