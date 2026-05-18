use std::collections::BTreeMap;

use anyhow::Result;
use async_trait::async_trait;
use manifold_runtime::ExecutionEngine;
use manifold_types::Value;

#[derive(Debug)]
pub struct UiuaEngine;

impl UiuaEngine {
    pub fn new() -> Self {
        Self
    }
}

impl Default for UiuaEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExecutionEngine for UiuaEngine {
    async fn execute(&self, code: &str, input: Value) -> Result<Value> {
        let mut map = BTreeMap::new();
        map.insert("engine".to_string(), Value::String("uiua".to_string()));
        map.insert("code".to_string(), Value::String(code.to_string()));
        map.insert("input".to_string(), input);
        Ok(Value::Map(map))
    }
}
