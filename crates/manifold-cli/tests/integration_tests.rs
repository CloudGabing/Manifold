#![cfg(feature = "rocksdb")]

use std::{env, fs, process::Command};

use anyhow::Result;
use manifold_store::RocksDbStore;
use manifold_types::{RunMetadata, RunRecord, RunStatus, Value};
use tempfile::TempDir;
use uuid::Uuid;

fn cli_binary_path() -> String {
    env::var("CARGO_BIN_EXE_manifold_cli")
        .or_else(|_| env::var("CARGO_BIN_EXE_manifold-cli"))
        .expect("expected cargo to expose the manifold-cli binary path")
}

#[tokio::test]
async fn integration_run_and_persist_lineage() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("db");
    let config_path = temp_dir.path().join("pipeline.yaml");
    let input_path = temp_dir.path().join("input.json");

    let config_yaml = r#"name: integration-pipeline
nodes:
  - name: generate
    engine: uiua
    code: "[1 2 3 4]"
    inputs: []
  - name: process_sum
    engine: uiua
    code: "+"
    inputs:
      - generate
"#;

    fs::write(&config_path, config_yaml)?;
    fs::write(&input_path, "{}")?;

    let output = Command::new(cli_binary_path())
        .arg("--db")
        .arg(db_path.to_str().unwrap())
        .arg("run")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .arg("--input")
        .arg(input_path.to_str().unwrap())
        .output()?;

    assert!(output.status.success(), "CLI run failed: {}", String::from_utf8_lossy(&output.stderr));

    let run_record: RunRecord = serde_json::from_slice(&output.stdout)?;
    assert_eq!(run_record.metadata.status, RunStatus::Completed);
    assert!(run_record.metadata.error_message.is_none());
    assert!(run_record.metadata.ancestor_run_id.is_none());
    let outputs = run_record
        .output_snapshot
        .as_map()
        .expect("expected output_snapshot to be a map");
    let process_sum_output = outputs
        .get("process_sum")
        .expect("expected process_sum output to be present");
    assert!(process_sum_output.as_map().is_some(), "expected process_sum output to be a map");

    let store = RocksDbStore::open(db_path.to_str().unwrap())?;
    let persisted_record = store
        .get_run(&run_record.metadata.run_id)?
        .expect("expected persisted run record");

    assert_eq!(persisted_record.metadata.status, RunStatus::Completed);
    assert!(persisted_record.metadata.error_message.is_none());
    assert!(persisted_record.metadata.ancestor_run_id.is_none());
    assert!(persisted_record
        .output_snapshot
        .as_map()
        .unwrap()
        .contains_key("process_sum"));

    Ok(())
}

#[tokio::test]
async fn integration_rerun_preserves_ancestor_chain() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("db");
    let original_id = Uuid::new_v4().to_string();

    let config_yaml = r#"name: rerun-pipeline
nodes:
  - name: generate
    engine: uiua
    code: "[1 2 3 4]"
    inputs: []
"#;

    let original_record = RunRecord {
        metadata: manifold_types::RunMetadata {
            run_id: original_id.clone(),
            pipeline_name: "rerun-pipeline".to_string(),
            status: RunStatus::Completed,
            start_time: chrono::Utc::now(),
            end_time: chrono::Utc::now(),
            duration_ms: 1,
            nodes_executed: 1,
            ancestor_run_id: None,
            error_message: None,
        },
        nodes: vec![],
        config_snapshot: config_yaml.to_string(),
        input_snapshot: Value::Map(Default::default()),
        output_snapshot: Value::Map(Default::default()),
    };

    let store = RocksDbStore::open(db_path.to_str().unwrap())?;
    store.put_run(original_record.clone())?;

    let output = Command::new(cli_binary_path())
        .arg("--db")
        .arg(db_path.to_str().unwrap())
        .arg("rerun")
        .arg(&original_id)
        .output()?;

    assert!(output.status.success(), "CLI rerun failed: {}", String::from_utf8_lossy(&output.stderr));

    let new_run_id = String::from_utf8(output.stdout)?.trim().to_string();
    assert_ne!(new_run_id, original_id);
    assert!(Uuid::parse_str(&new_run_id).is_ok());

    let rerun_record = store
        .get_run(&new_run_id)?
        .expect("expected rerun record to exist");
    assert_eq!(rerun_record.metadata.status, RunStatus::Completed);
    assert_eq!(rerun_record.metadata.ancestor_run_id, Some(Uuid::parse_str(&original_id)?));

    Ok(())
}
