use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(untagged)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(#[serde(with = "bytes_serde")] Arc<[u8]>),
    List(Vec<Value>),
    Table(Vec<Value>),
    Map(BTreeMap<String, Value>),
    Object(BTreeMap<String, Value>),
}

mod bytes_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use serde_bytes::ByteBuf;
    use std::sync::Arc;

    pub fn serialize<S>(bytes: &Arc<[u8]>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<[u8]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let buf = ByteBuf::deserialize(deserializer)?;
        Ok(Arc::from(buf.into_vec().into_boxed_slice()))
    }
}

impl Value {
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(v) => Some(v),
            Value::String(v) => Some(v.as_bytes()),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(v) | Value::Table(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_table(&self) -> Option<&[Value]> {
        match self {
            Value::Table(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Map(m) | Value::Object(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Object(m) | Value::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn normalize_to_id_string(&self) -> String {
        match self {
            Value::String(s) => s.clone(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => {
                if f.fract() == 0.0 {
                    format!("{:.0}", f)
                } else {
                    f.to_string()
                }
            }
            Value::Bool(b) => b.to_string(),
            Value::Bytes(bytes) => {
                let hex = bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                format!("bytes:{}", hex)
            }
            Value::Null => "null".to_string(),
            Value::List(list) => serde_json::to_string(list).unwrap_or_else(|_| "[invalid]".to_string()),
            Value::Table(table) => serde_json::to_string(table).unwrap_or_else(|_| "[invalid]".to_string()),
            Value::Map(map) | Value::Object(map) => serde_json::to_string(map).unwrap_or_else(|_| "{invalid}".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RunStatus {
    Started,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeRecord {
    pub name: String,
    pub engine: String,
    pub code: String,
    pub inputs: Value,
    pub output: Option<Value>,
    pub status: NodeStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunMetadata {
    pub run_id: String,
    pub pipeline_name: String,
    pub status: RunStatus,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_ms: u128,
    pub nodes_executed: usize,
    pub ancestor_run_id: Option<Uuid>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunRecord {
    pub metadata: RunMetadata,
    pub nodes: Vec<NodeRecord>,
    pub config_snapshot: String,
    pub input_snapshot: Value,
    pub output_snapshot: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn value_round_trip_serialize() {
        let mut map = BTreeMap::new();
        map.insert("key".to_string(), Value::Int(42));
        let source = Value::Map(map);

        let json = serde_json::to_string(&source).unwrap();
        let round_trip: Value = serde_json::from_str(&json).unwrap();

        assert_eq!(source, round_trip);
    }

    #[test]
    fn bytes_value_round_trip_serialize() {
        let bytes: Arc<[u8]> = Arc::from(&b"hello world"[..] as &[u8]);
        let source = Value::Bytes(bytes.clone());

        let json = serde_json::to_string(&source).unwrap();
        let round_trip: Value = serde_json::from_str(&json).unwrap();

        assert_eq!(source, round_trip);
        assert_eq!(round_trip.as_bytes().unwrap(), b"hello world");
    }

    #[test]
    fn run_record_contains_metadata_and_output() {
        let metadata = RunMetadata {
            run_id: "run-1".to_string(),
            pipeline_name: "demo".to_string(),
            status: RunStatus::Completed,
            start_time: Utc::now(),
            end_time: Utc::now(),
            duration_ms: 27,
            nodes_executed: 1,
            ancestor_run_id: None,
            error_message: None,
        };

        let record = RunRecord {
            metadata,
            nodes: vec![],
            config_snapshot: "{}".to_string(),
            input_snapshot: Value::Null,
            output_snapshot: Value::String("ok".to_string()),
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: RunRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.metadata.pipeline_name, "demo");
        assert_eq!(parsed.output_snapshot, Value::String("ok".to_string()));
    }
}
