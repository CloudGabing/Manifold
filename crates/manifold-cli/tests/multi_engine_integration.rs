#![cfg(feature = "rocksdb")]

use anyhow::Result;
use std::{env, fs, process::Command};
use tempfile::TempDir;
use uuid::Uuid;

use manifold_store::RocksDbStore;
use manifold_types::{RunRecord, RunStatus, Value};

fn cli_binary_path() -> String {
    env::var("CARGO_BIN_EXE_manifold_cli")
        .or_else(|_| env::var("CARGO_BIN_EXE_manifold-cli"))
        .expect("expected cargo to expose the manifold-cli binary path")
}

#[tokio::test]
async fn multi_engine_pipeline_executes_and_persists() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("db");
    let config_path = temp_dir.path().join("pipeline.yaml");

    let config_yaml = r#"name: multi-engine
nodes:
  - name: native_node
    engine: native
    code: native-op
    inputs: []
  - name: uiua_node
    engine: uiua
    code: +
    inputs:
      - native_node
"#;

    fs::write(&config_path, config_yaml)?;

    let output = Command::new(cli_binary_path())
        .arg("--db")
        .arg(db_path.to_str().unwrap())
        .arg("run")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .output()?;

    assert!(output.status.success(), "CLI run failed: {}", String::from_utf8_lossy(&output.stderr));

    let run_record: RunRecord = serde_json::from_slice(&output.stdout)?;
    assert_eq!(run_record.metadata.status, RunStatus::Completed);
    assert!(run_record.metadata.error_message.is_none());

    let outputs = run_record.output_snapshot.as_map().unwrap();
    assert!(outputs.contains_key("native_node"));
    assert!(outputs.contains_key("uiua_node"));

    // Verify persisted in RocksDB
    let store = RocksDbStore::open(db_path.to_str().unwrap())?;
    let persisted = store.get_run(&run_record.metadata.run_id)?.expect("persisted run exists");
    assert_eq!(persisted.metadata.status, RunStatus::Completed);
    Ok(())
}

#[tokio::test]
async fn missing_engine_records_partial_lineage() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("db");
    let config_path = temp_dir.path().join("bad_pipeline.yaml");

    let config_yaml = r#"name: bad-engine
nodes:
  - name: broken
    engine: does_not_exist
    code: x
    inputs: []
"#;

    fs::write(&config_path, config_yaml)?;

    let output = Command::new(cli_binary_path())
        .arg("--db")
        .arg(db_path.to_str().unwrap())
        .arg("run")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .output()?;

    // Expect non-zero exit due to execution failure
    assert!(!output.status.success());

    // Open store and find run entries
    let store = RocksDbStore::open(db_path.to_str().unwrap())?;
    let runs = store.list_runs()?;
    assert!(!runs.is_empty(), "expected at least one persisted run");

    // find a failed run
    let failed = runs
        .into_iter()
        .filter(|m| m.status == RunStatus::Failed)
        .next()
        .expect("expected a failed run");

    let rec = store.get_run(&failed.run_id)?.expect("run record present");
    assert_eq!(rec.metadata.status, RunStatus::Failed);
    assert!(rec.metadata.error_message.is_some());
    assert!(rec.metadata.error_message.unwrap().contains("is not registered"));

    Ok(())
}
