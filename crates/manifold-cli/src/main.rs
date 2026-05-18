use std::{collections::{BTreeSet, HashMap}, fs, path::{Path, PathBuf}, sync::Arc};

use anyhow::Context;
use clap::{Parser, Subcommand};
use colored::*;
use manifold_config::parse_config;
use manifold_runtime::Runtime as ManifoldRuntime;
use memmap2::MmapOptions;
use wasmparser::{Parser as WasmParser, Payload};
use manifold_runtime::{AssetHost, NativeEngine, GuestModule};
use manifold_store::{InMemoryStore, Store};
use manifold_types::{NodeStatus, RunRecord, RunStatus, Value};
use manifold_uiua::UiuaEngine;
use tabled::{Table, Style, Tabled};
use toml::Value as TomlValue;
use thiserror::Error;
use uuid::Uuid;

#[cfg(feature = "rocksdb")]
use manifold_store::RocksDbStore;

#[derive(Parser)]
#[command(name = "manifold")]
#[command(about = "Local computational pipeline environment", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, default_value = ".manifold_storage/db", value_name = "DB_PATH")]
    db: PathBuf,
}

#[derive(Subcommand)]
enum Commands {
    Run {
        #[arg(value_name = "DIRECTORY")] 
        directory: Option<PathBuf>,

        #[arg(long, value_name = "CONFIG")]
        config: Option<PathBuf>,

        #[arg(long, value_name = "INPUT_JSON")]
        input: Option<PathBuf>,
        #[arg(long, help = "Enable debug logging and stream guest stdout/stderr")] 
        debug: bool,
    },
    Runs {
        #[command(subcommand)]
        subcommand: RunsCommand,
    },
    Diff {
        #[arg(value_name = "ID_1")]
        id_1: String,

        #[arg(value_name = "ID_2")]
        id_2: String,
    },
    Rerun {
        #[arg(value_name = "ID")]
        id: String,
    },
    Debug {
        #[arg(long, value_name = "TABLE_NAME", help = "Inspect entries from a stored table prefix")]
        table: Option<String>,

        #[arg(long, value_name = "TARGET_ID", help = "Find relations for a target vertex id")]
        find_relations: Option<String>,

        #[arg(long, default_value_t = 50, help = "Maximum rows to return")]
        limit: usize,

        #[arg(long, default_value_t = 0, help = "Pagination offset")]
        offset: usize,
    },
    Check {
        #[arg(value_name = "DIRECTORY")]
        directory: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum RunsCommand {
    Show {
        #[arg(value_name = "ID")]
        id: String,

        #[arg(long)]
        raw: bool,
    },
}

#[derive(Debug, Error)]
enum CliError {
    #[error("store error: {0}")]
    Store(#[from] manifold_store::StoreError),
    #[error("configuration error: {0}")]
    Config(#[from] manifold_config::ConfigError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("run not found: {0}")]
    RunNotFound(String),
    #[error("execution failure: {0}")]
    ExecutionFailed(String),
    #[error("execution error: {0}")]
    Anyhow(#[from] anyhow::Error),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(err) = run_cli(cli).await {
        eprintln!("[ERROR][CLI] {}", err);
        std::process::exit(1);
    }
}

async fn run_cli(cli: Cli) -> Result<(), CliError> {
    let store = open_store(&cli.db)?;

    match cli.command {
        Commands::Run { directory, config, input, debug } => run_command(store, directory, config, input, debug).await,
        Commands::Runs { subcommand } => match subcommand {
            RunsCommand::Show { id, raw } => show_command(store, id, raw),
        },
        Commands::Diff { id_1, id_2 } => diff_command(store, id_1, id_2),
        Commands::Rerun { id } => rerun_command(store, id).await,
        Commands::Debug { table, find_relations, limit, offset } => debug_command(store, table, find_relations, limit, offset).await,
        Commands::Check { directory } => check_command(directory).await,
    }
}

fn open_store(path: &Path) -> Result<Arc<dyn Store>, CliError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    #[cfg(feature = "rocksdb")]
    {
        let store = RocksDbStore::open(path.to_str().ok_or_else(|| CliError::Validation("invalid db path".to_string()))?)?;
        Ok(Arc::new(store))
    }

    #[cfg(not(feature = "rocksdb"))]
    {
        Ok(Arc::new(InMemoryStore::new()))
    }
}

async fn run_command(
    store: Arc<dyn Store>,
    directory: Option<PathBuf>,
    config_path: Option<PathBuf>,
    input_path: Option<PathBuf>,
    debug: bool,
) -> Result<(), CliError> {
    // Resolve target directory: prefer provided, else auto-discover in cwd
    let target_dir = if let Some(dir) = directory {
        // lightweight preflight check (synchronous filesystem call)
        let meta = std::fs::metadata(&dir);
        match meta {
            Ok(m) if m.is_dir() => dir,
            _ => {
                eprintln!("[ERROR][Orchestrator] Provided path '{}' is not a valid directory. Suggestion: ensure the path exists or omit it to auto-discover.", dir.display());
                std::process::exit(1);
            }
        }
    } else {
        // Auto-discover in current working directory
        let cwd = std::env::current_dir().map_err(CliError::Io)?;
        // prefer manifold.toml, then dist/public
        let manifest = cwd.join("manifold.toml");
        if manifest.is_file() || cwd.join("dist").is_dir() || cwd.join("public").is_dir() {
            cwd
        } else {
            eprintln!("[ERROR][Orchestrator] No target directory provided and no auto-discoverable workspace found in current directory. Suggestion: run 'manifold run <path>' or create 'manifold.toml' in the workspace.");
            std::process::exit(1);
        }
    };

    let app_id = determine_app_id(&target_dir)?;
    let pipeline_path = locate_pipeline_config(&target_dir, config_path)?;
    let config_source = fs::read_to_string(&pipeline_path).map_err(CliError::Io)?;
    let pipeline = parse_config(&config_source)?;
    let inputs = load_inputs(input_path)?;

    let asset_root = determine_asset_root(&target_dir);
    let (log_tx, log_rx) = if debug {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let asset_host = Arc::new(AssetHost::start(asset_root, log_tx.clone()).map_err(CliError::Anyhow)?);
    let asset_url = asset_host.url();

    // Spawn the optional log consumer (retain handle to abort on shutdown)
    let log_task = log_rx.map(|mut rx| tokio::spawn(async move {
        while let Some(line) = rx.recv().await {
            println!("{}", line);
        }
    }));

    let mut runtime = ManifoldRuntime::new_with_namespace(store.clone(), &app_id);
    runtime.register_engine("uiua", Arc::new(UiuaEngine::new()));
    runtime.register_engine("native", Arc::new(NativeEngine::new()));
    let wasm_engine = if debug {
        Arc::new(GuestModule::new_with_logger_and_asset_host(log_tx.clone(), Arc::clone(&asset_host)))
    } else {
        Arc::new(GuestModule::new_with_asset_host(Arc::clone(&asset_host)))
    };
    runtime.register_engine("wasm", wasm_engine);

    let run_record = execute_runtime_pipeline(store.clone(), &mut runtime, pipeline, inputs, None).await?;

    println!("[INFO][Orchestrator] Booting dApp workspace");
    println!("[INFO][Orchestrator] Application ID : app:{}", app_id);
    println!("[INFO][Orchestrator] Storage Sandboxing : ENABLED (InstanceNamespace prefix)");
    println!("[INFO][Orchestrator] Virtual Sandboxing: ACTIVE (fuel & memory caps)");
    println!("[INFO][Orchestrator] Frontend Endpoint : {}", asset_url);
    println!("[INFO][Orchestrator] Run ID             : {}", run_record.metadata.run_id);
    println!("[INFO][Orchestrator] Waiting for Ctrl-C to terminate the orchestrator...");

    tokio::signal::ctrl_c()
        .await
            .map_err(CliError::Io)?;
    println!("[INFO][Orchestrator] Shutdown signal received; performing graceful shutdown...");

    // 1) Abort logging task
    if let Some(handle) = log_task {
        handle.abort();
    }

    // 2) Flush RocksDB if present
    #[cfg(feature = "rocksdb")]
    {
        if let Some(rocks_store) = store.as_any().downcast_ref::<RocksDbStore>() {
            let _ = rocks_store.flush();
        }
    }

    // 3) Shutdown AssetHost and release port
    asset_host.shutdown();

    println!("[INFO][Orchestrator] Shutdown complete. Exiting.");
    Ok(())
}

fn show_command(store: Arc<dyn Store>, id: String, raw: bool) -> Result<(), CliError> {
    validate_run_id(&id)?;
    let record = get_run(store, &id)?;

    if raw {
        println!("{}", serde_json::to_string_pretty(&record)?);
        return Ok(());
    }

    print_run_summary(&record);
    Ok(())
}

fn diff_command(store: Arc<dyn Store>, id_1: String, id_2: String) -> Result<(), CliError> {
    validate_run_id(&id_1)?;
    validate_run_id(&id_2)?;

    let left = get_run(store.clone(), &id_1)?;
    let right = get_run(store, &id_2)?;
    let report = generate_diff_report(&left, &right);
    eprintln!("{}", report);
    Ok(())
}

async fn rerun_command(store: Arc<dyn Store>, id: String) -> Result<(), CliError> {
    validate_run_id(&id)?;
    let original = get_run(store.clone(), &id)?;
    let pipeline = parse_config(&original.config_snapshot)?;
    pipeline.validate().map_err(CliError::Config)?;
    let inputs = extract_inputs(original.input_snapshot.clone())?;

    let mut runtime = ManifoldRuntime::new(store.clone());
    runtime.register_engine("uiua", Arc::new(UiuaEngine::new()));
    runtime.register_engine("native", Arc::new(NativeEngine::new()));
    runtime.register_engine("wasm", Arc::new(GuestModule::new()));

    let new_record = execute_runtime_pipeline(
        store,
        &mut runtime,
        pipeline,
        inputs,
        Some(Uuid::parse_str(&original.metadata.run_id).map_err(|_| CliError::Validation(format!("invalid ancestor run id: {}", original.metadata.run_id)))?),
    )
    .await?;
    println!("{}", new_record.metadata.run_id);
    Ok(())
}

async fn debug_command(
    store: Arc<dyn Store>,
    table: Option<String>,
    find_relations: Option<String>,
    limit: usize,
    offset: usize,
) -> Result<(), CliError> {
    if table.is_none() && find_relations.is_none() {
        return Err(CliError::Validation(
            "debug requires either --table or --find-relations".to_string(),
        ));
    }
    if table.is_some() && find_relations.is_some() {
        return Err(CliError::Validation(
            "debug accepts only one of --table or --find-relations".to_string(),
        ));
    }

    if let Some(table_name) = table {
        let entries = store.scan_prefix(&format!("table:{}:", table_name), limit, offset)?;
        if entries.is_empty() {
            println!("No table entries found for the requested prefix.");
            return Ok(());
        }
        let rows: Vec<DebugRow> = entries
            .into_iter()
            .map(|(key, value)| DebugRow {
                key,
                value: format_value(&value),
            })
            .collect();
        println!("{}", safe_render_table(Table::new(rows).with(Style::modern())));
        return Ok(());
    }

    if let Some(target_id) = find_relations {
        let runtime = ManifoldRuntime::new(store.clone());
        let relations = runtime
            .host_query_vertex_relations(&target_id)
            .await
            .map_err(CliError::Anyhow)?;

        let rows = relations
            .as_table()
            .unwrap_or_default()
            .iter()
            .map(|row| {
                let row_map = row.as_map();
                let edge_type = row_map
                    .and_then(|map| map.get("edge_type"))
                    .and_then(Value::as_str)
                    .unwrap_or("<unknown>")
                    .to_string();
                let edge_id = row_map
                    .and_then(|map| map.get("edge_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("<unknown>")
                    .to_string();
                let target = row_map
                    .and_then(|map| map.get("target_vertex"))
                    .and_then(Value::as_str)
                    .unwrap_or(&target_id)
                    .to_string();
                let members = row_map
                    .and_then(|map| map.get("members"))
                    .and_then(Value::as_list)
                    .map(|members: &[Value]| {
                        members
                            .iter()
                            .filter_map(Value::as_str)
                            .map(String::from)
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .unwrap_or_else(|| "[]".to_string());

                RelationRow {
                    edge_type,
                    edge_id,
                    target,
                    members,
                }
            })
            .collect::<Vec<_>>();

        if rows.is_empty() {
            println!("No relations found for target_id {}.", target_id);
            return Ok(());
        }

        println!("{}", safe_render_table(Table::new(rows).with(Style::modern())));
        return Ok(());
    }

    Ok(())
}

#[derive(Tabled)]
struct DebugRow {
    key: String,
    value: String,
}

#[derive(Tabled)]
struct RelationRow {
    edge_type: String,
    edge_id: String,
    target: String,
    members: String,
}

fn safe_render_table(table: Table) -> String {
    std::panic::catch_unwind(|| table.to_string()).unwrap_or_else(|_| {
        "unable to render table output cleanly; consider using a smaller limit or inspecting raw data".to_string()
    })
}

fn validate_run_id(id: &str) -> Result<Uuid, CliError> {
    Uuid::parse_str(id).map_err(|_| CliError::Validation(format!("invalid run id format: {}", id)))
}

fn get_run(store: Arc<dyn Store>, id: &str) -> Result<RunRecord, CliError> {
    store
        .get_run(id)?
        .ok_or_else(|| CliError::RunNotFound(id.to_string()))
}

fn load_inputs(input_path: Option<PathBuf>) -> Result<HashMap<String, Value>, CliError> {
    if let Some(path) = input_path {
        let source = fs::read_to_string(&path).context("failed to read input file")?;
        let map: HashMap<String, Value> = serde_json::from_str(&source).context("failed to parse JSON input file")?;
        Ok(map)
    } else {
        Ok(HashMap::new())
    }
}

fn locate_pipeline_config(root: &Path, explicit: Option<PathBuf>) -> Result<PathBuf, CliError> {
    if let Some(path) = explicit {
        if path.is_file() {
            return Ok(path);
        }
        return Err(CliError::Validation(format!(
            "config file not found: {}",
            path.display()
        )));
    }

    let names = ["pipeline.yaml", "pipeline.yml", "manifold.yaml", "manifold.yml"];
    for name in names {
        let candidate = root.join(name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    Err(CliError::Validation(format!(
        "pipeline config not found in {}. expected one of: {}",
        root.display(),
        names.join(", ")
    )))
}

fn determine_asset_root(root: &Path) -> PathBuf {
    for candidate in ["dist", "public"] {
        let candidate_path = root.join(candidate);
        if candidate_path.is_dir() {
            return candidate_path;
        }
    }
    root.to_path_buf()
}

fn determine_app_id(root: &Path) -> Result<String, CliError> {
    let manifest_path = root.join("manifold.toml");
    if manifest_path.is_file() {
        let source = fs::read_to_string(&manifest_path).context("failed to read manifold.toml")?;
        let parsed: TomlValue = toml::from_str(&source).map_err(|err| CliError::Validation(format!("failed to parse manifold.toml: {}", err)))?;

        if let Some(app_id) = parsed
            .get("app")
            .and_then(|app| app.get("id"))
            .and_then(|value| value.as_str())
        {
            return Ok(sanitize_app_id(app_id));
        }

        if let Some(app_id) = parsed.get("app_id").and_then(|value| value.as_str()) {
            return Ok(sanitize_app_id(app_id));
        }
    }

    let name = root
        .file_name()
        .and_then(|segment| segment.to_str())
        .map(|value| value.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let sanitized = sanitize_app_id(&name);
    if sanitized.is_empty() {
        Ok(Uuid::new_v4().to_string())
    } else {
        Ok(sanitized)
    }
}

fn sanitize_app_id(raw: &str) -> String {
    let mut sanitized = raw
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            ' ' => '_',
            _ => '_',
        })
        .collect::<String>();

    while sanitized.starts_with(['_', '-']) {
        sanitized.remove(0);
    }
    while sanitized.ends_with(['_', '-']) {
        sanitized.pop();
    }

    if sanitized.is_empty() {
        Uuid::new_v4().to_string()
    } else {
        sanitized.to_lowercase()
    }
}

fn extract_inputs(value: Value) -> Result<HashMap<String, Value>, CliError> {
    match value {
        Value::Map(map) | Value::Object(map) => Ok(map.into_iter().collect()),
        Value::Null => Ok(HashMap::new()),
        other => Err(CliError::Validation(format!(
            "stored inputs are not a map or null: {}",
            format_value(&other)
        ))),
    }
}

async fn check_command(directory: Option<PathBuf>) -> Result<(), CliError> {
    let target_dir = resolve_check_target(directory)?;
    let manifest_path = target_dir.join("manifold.toml");
    if !manifest_path.is_file() {
        eprintln!("[ERROR][Validator] No manifest found in {}. Expected manifold.toml in the dApp root.", target_dir.display());
        return Err(CliError::Validation("manifest missing".to_string()));
    }

    let manifest = validate_manifest_architecture(&manifest_path)?;
    let frontend_dir = target_dir.join(&manifest.frontend_assets_dir);
    if !frontend_dir.is_dir() {
        eprintln!("[ERROR][Validator] Frontend assets directory '{}' is missing or invalid. Please ensure the path exists and is declared in manifold.toml.", manifest.frontend_assets_dir);
        return Err(CliError::Validation("frontend assets directory validation failed".to_string()));
    }

    let wasm_path = target_dir.join(&manifest.backend_wasm_path);
    if !wasm_path.is_file() {
        eprintln!("[ERROR][Validator] Backend WASM module '{}' is missing or invalid. Please ensure the path exists and is declared in manifold.toml.", manifest.backend_wasm_path);
        return Err(CliError::Validation("backend wasm path validation failed".to_string()));
    }

    let warnings = inspect_wasm_imports(&wasm_path)?;
    if !warnings.is_empty() {
        for warning in warnings {
            eprintln!("[WARN][Validator] {}", warning);
        }
        return Err(CliError::Validation("wasm import validation failed".to_string()));
    }

    println!("[INFO][Validator] dApp architecture passes v3.0 enforcement specs. Ready for execution.");
    Ok(())
}

fn resolve_check_target(directory: Option<PathBuf>) -> Result<PathBuf, CliError> {
    let root = if let Some(dir) = directory {
        if !dir.is_dir() {
            return Err(CliError::Validation(format!(
                "directory not found: {}",
                dir.display()
            )));
        }
        dir
    } else {
        std::env::current_dir().map_err(CliError::Io)?
    };
    Ok(root)
}

struct ManifestLayout {
    frontend_assets_dir: String,
    backend_wasm_path: String,
}

fn validate_manifest_architecture(manifest_path: &Path) -> Result<ManifestLayout, CliError> {
    let source = fs::read_to_string(manifest_path).map_err(CliError::Io)?;
    let parsed: TomlValue = toml::from_str(&source).map_err(|err| {
        eprintln!("[ERROR][Validator] failed to parse manifold.toml: {}", err);
        CliError::Validation("manifest parse failed".to_string())
    })?;

    let frontend_assets_dir = parsed
        .get("frontend")
        .and_then(|frontend| frontend.get("assets_dir"))
        .and_then(|value| value.as_str())
        .map(String::from)
        .or_else(|| parsed.get("assets_dir").and_then(|value| value.as_str()).map(String::from));

    let backend_wasm_path = parsed
        .get("backend")
        .and_then(|backend| backend.get("wasm_path"))
        .and_then(|value| value.as_str())
        .map(String::from)
        .or_else(|| parsed.get("wasm").and_then(|value| value.as_str()).map(String::from));

    let frontend_assets_dir = frontend_assets_dir.ok_or_else(|| {
        eprintln!("[ERROR][Validator] Missing required manifest field `frontend.assets_dir` or `assets_dir`.");
        CliError::Validation("frontend assets declaration missing".to_string())
    })?;

    let backend_wasm_path = backend_wasm_path.ok_or_else(|| {
        eprintln!("[ERROR][Validator] Missing required manifest field `backend.wasm_path` or `wasm`.");
        CliError::Validation("backend wasm declaration missing".to_string())
    })?;

    Ok(ManifestLayout {
        frontend_assets_dir,
        backend_wasm_path,
    })
}

fn inspect_wasm_imports(wasm_path: &Path) -> Result<Vec<String>, CliError> {
    let file = fs::File::open(wasm_path).map_err(CliError::Io)?;
    let metadata = file.metadata().map_err(CliError::Io)?;
    if metadata.len() > 5 * 1024 * 1024 {
        return Err(CliError::Validation(
            "WASM module exceeds the 5MB static validation limit".to_string(),
        ));
    }

    let mmap = unsafe { MmapOptions::new().map(&file).map_err(|err| CliError::Anyhow(err.into()))? };
    let bytes = &mmap[..];
    let mut warnings = Vec::new();

    let forbidden_imports: HashMap<&str, &str> = [
        ("sock_accept", "Muted raw socket detected. Please route traffic through the AssetHost local loopback layer instead."),
        ("sock_connect", "Muted raw socket detected. Please route traffic through the AssetHost local loopback layer instead."),
        ("sock_send", "Raw socket send is restricted. Please route traffic through AssetHost."),
        ("sock_recv", "Raw socket receive is restricted. Please route traffic through AssetHost."),
        ("open_file", "Direct filesystem access is restricted. Use sandboxed store APIs instead."),
        ("read_file", "Direct filesystem access is restricted. Use sandboxed store APIs instead."),
        ("write_file", "Direct filesystem access is restricted. Use sandboxed store APIs instead."),
        ("remove_file", "Direct filesystem access is restricted. Use sandboxed store APIs instead."),
        ("rename_file", "Direct filesystem access is restricted. Use sandboxed store APIs instead."),
        ("create_dir", "Direct filesystem access is restricted. Use sandboxed store APIs instead."),
        ("remove_dir", "Direct filesystem access is restricted. Use sandboxed store APIs instead."),
        ("spawn_process", "Process execution is restricted inside the sandbox."),
        ("exec", "Process execution is restricted inside the sandbox."),
    ]
    .into_iter()
    .collect();

    let parser = WasmParser::new(0);
    for payload in parser.parse_all(bytes) {
        let payload = payload.map_err(|err: wasmparser::BinaryReaderError| CliError::Anyhow(err.into()))?;
        if let Payload::ImportSection(reader) = payload {
            for import in reader {
                let import = import.map_err(|err: wasmparser::BinaryReaderError| CliError::Anyhow(err.into()))?;
                let import_name = import.name;
                let import_key = format!("{}::{}", import.module, import_name);

                if import.module.starts_with("wasi") {
                    warnings.push(format!(
                        "Unauthorized WASI host import '{}'. Manifold v3.0 enforces a hardened sandbox boundary.",
                        import_key
                    ));
                }

                if let Some(message) = forbidden_imports.get(import_name) {
                    warnings.push(format!(
                        "Forbidden host import '{}': {}",
                        import_key, message
                    ));
                }

                if import.module == "env" && import_name == "sock_connect" {
                    warnings.push("Unauthorized raw socket connect import detected. Please use AssetHost instead.".to_string());
                }
            }
        }
    }

    Ok(warnings)
}

async fn execute_runtime_pipeline(
    store: Arc<dyn Store>,
    runtime: &mut ManifoldRuntime,
    config: manifold_config::PipelineConfig,
    inputs: HashMap<String, Value>,
    ancestor_run_id: Option<Uuid>,
) -> Result<RunRecord, CliError> {
    let run_record = runtime.execute_pipeline(&config, inputs, ancestor_run_id).await?;

    #[cfg(feature = "rocksdb")]
    if let Some(rocks_store) = store.as_any().downcast_ref::<RocksDbStore>() {
        rocks_store.put_run_with_key(&format!("run:{}", run_record.metadata.run_id), run_record.clone())?;
    }

    #[cfg(not(feature = "rocksdb"))]
    let _ = &store;

    if run_record.metadata.status == RunStatus::Failed {
        let error_message = run_record
            .nodes
            .iter()
            .find(|node| node.status == NodeStatus::Failed)
            .and_then(|node| node.error.clone())
            .unwrap_or_else(|| "pipeline execution failed".to_string());

        return Err(CliError::ExecutionFailed(error_message));
    }

    Ok(run_record)
}

#[derive(Tabled)]
struct SummaryRow {
    key: String,
    value: String,
}

fn print_run_summary(record: &RunRecord) {
    let status_label = match record.metadata.status {
        RunStatus::Completed => "Success".green().bold().to_string(),
        RunStatus::Failed => "Failure".red().bold().to_string(),
        RunStatus::Started => "Started".yellow().bold().to_string(),
    };

    let mut table_rows = vec![
        SummaryRow {
            key: "Run ID".to_string(),
            value: record.metadata.run_id.clone(),
        },
        SummaryRow {
            key: "Pipeline".to_string(),
            value: record.metadata.pipeline_name.clone(),
        },
        SummaryRow {
            key: "Status".to_string(),
            value: status_label,
        },
        SummaryRow {
            key: "Duration".to_string(),
            value: format!("{}ms", record.metadata.duration_ms),
        },
        SummaryRow {
            key: "Nodes".to_string(),
            value: record.metadata.nodes_executed.to_string(),
        },
        SummaryRow {
            key: "Started".to_string(),
            value: record.metadata.start_time.to_rfc3339(),
        },
    ];

    if let Some(ancestor) = &record.metadata.ancestor_run_id {
        table_rows.push(SummaryRow {
            key: "Ancestor".to_string(),
            value: ancestor.to_string(),
        });
    }

    if let Some(error_message) = &record.metadata.error_message {
        table_rows.push(SummaryRow {
            key: "Error".to_string(),
            value: error_message.clone(),
        });
    }

    eprintln!("{}", Table::new(table_rows).with(Style::modern()));
    eprintln!();

    eprintln!("{}", "INPUTS".bold());
    print_snapshot(&record.input_snapshot);
    eprintln!();

    eprintln!("{}", "OUTPUTS".bold());
    print_snapshot(&record.output_snapshot);
    eprintln!();

    eprintln!("{}", "CONFIG SNAPSHOT".bold());
    eprintln!("{}", record.config_snapshot.trim());
}

fn print_snapshot(snapshot: &Value) {
    match snapshot {
        Value::Map(values) | Value::Object(values) if values.is_empty() => {
            eprintln!("  {}", "<none>".dimmed())
        }
        Value::Map(values) | Value::Object(values) => {
            for (key, value) in values {
                eprintln!("  {:20} {}", key, format_value(value));
            }
        }
        Value::Table(rows) | Value::List(rows) if rows.is_empty() => {
            eprintln!("  {}", "<none>".dimmed())
        }
        Value::Table(rows) | Value::List(rows) => {
            for value in rows {
                eprintln!("  - {}", format_value(value));
            }
        }
        other => eprintln!("  {}", format_value(other)),
    }
}

fn generate_diff_report(left: &RunRecord, right: &RunRecord) -> String {
    let mut lines = Vec::new();
    lines.push(format!("{}", "RUN DIFF".bold().underline()));
    lines.push(format!("{:20} {}", "Left ID:", left.metadata.run_id));
    lines.push(format!("{:20} {}", "Right ID:", right.metadata.run_id));
    if left.config_snapshot != right.config_snapshot {
        lines.push(String::new());
        lines.push(format!(
            "{} {}",
            "⚠ Warning:".yellow().bold(),
            "Pipeline configurations have diverged between these runs!".yellow()
        ));
    }
    lines.push(String::new());

    lines.push(String::from("INPUTS"));
    lines.extend(compare_snapshot_maps(&left.input_snapshot, &right.input_snapshot));
    lines.push(String::new());

    lines.push(String::from("OUTPUTS"));
    lines.extend(compare_snapshot_maps(&left.output_snapshot, &right.output_snapshot));
    lines.push(String::new());

    lines.push(String::from("PERFORMANCE"));
    lines.extend(compare_performance(left, right));
    lines.push(String::new());

    if left.config_snapshot != right.config_snapshot {
        lines.push(String::from("CONFIG SNAPSHOT"));
        lines.push(String::from("  Pipeline configuration content differs between runs."));
    }

    lines.join("\n")
}

fn compare_snapshot_maps(left: &Value, right: &Value) -> Vec<String> {
    if left == right {
        return vec![format!("  {}", "unchanged".cyan())];
    }

    match (left, right) {
        (Value::Map(left_map), Value::Map(right_map)) => {
            let mut output = Vec::new();
            let mut keys = BTreeSet::new();
            for key in left_map.keys().chain(right_map.keys()) {
                keys.insert(key);
            }

            for key in keys.iter() {
                match (left_map.get(*key), right_map.get(*key)) {
                    (Some(left_val), Some(right_val)) if left_val == right_val => {
                        output.push(format!("  {} {}", "=".cyan(), (*key)));
                    }
                    (Some(left_val), None) => {
                        output.push(format!(
                            "  {} {}: {}",
                            "-".red(),
                            key.red(),
                            format_value(left_val).red()
                        ));
                    }
                    (None, Some(right_val)) => {
                        output.push(format!(
                            "  {} {}: {}",
                            "+".green(),
                            key.green(),
                            format_value(right_val).green()
                        ));
                    }
                    (Some(left_val), Some(right_val)) => {
                        output.push(format!(
                            "  {} {}: {} -> {}",
                            "~".cyan(),
                            key.cyan(),
                            format_value(left_val),
                            format_value(right_val),
                        ));
                    }
                    _ => {}
                }
            }

            output
        }
        _ => vec![format!(
            "  {} {} -> {}",
            "~".cyan(),
            format_value(left),
            format_value(right)
        )],
    }
}

fn compare_performance(left: &RunRecord, right: &RunRecord) -> Vec<String> {
    let mut output = Vec::new();
    let left_ms = left.metadata.duration_ms;
    let right_ms = right.metadata.duration_ms;

    if left_ms == right_ms {
        output.push(format!("  {} {}ms", "= unchanged".cyan(), left_ms));
        return output;
    }

    let delta = right_ms.abs_diff(left_ms);

    if right_ms < left_ms {
        output.push(format!("  {} {} -> {} ({}ms faster)", "✔".green(), left_ms, right_ms, delta));
    } else {
        output.push(format!("  {} {} -> {} ({}ms slower)", "✖".red(), left_ms, right_ms, delta));
    }

    output
}

fn format_value(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("{:?}", value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use manifold_runtime::Runtime;
    use manifold_store::InMemoryStore;
    use manifold_types::{RunMetadata, RunRecord, RunStatus, Value};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use async_trait::async_trait;

    #[derive(Debug)]
    struct DummyEngine;

    #[async_trait]
    impl manifold_runtime::ExecutionEngine for DummyEngine {
        async fn execute(&self, code: &str, input: Value) -> anyhow::Result<Value> {
            let mut result = BTreeMap::new();
            result.insert("code".to_string(), Value::String(code.to_string()));
            result.insert("input".to_string(), input);
            Ok(Value::Map(result))
        }
    }

    #[test]
    fn diff_report_detects_mutation() {
        colored::control::set_override(false);

        let mut left_output = BTreeMap::new();
        left_output.insert("value".to_string(), Value::String("old".to_string()));
        let mut right_output = BTreeMap::new();
        right_output.insert("value".to_string(), Value::String("new".to_string()));

        let a = make_run_record("a", 10, Value::Map(BTreeMap::new()), Value::Map(left_output));
        let b = make_run_record("b", 12, Value::Map(BTreeMap::new()), Value::Map(right_output));
        let report = generate_diff_report(&a, &b);

        assert!(report.contains("value: \"old\" -> \"new\""));
        assert!(report.contains("slower"));
    }

    #[test]
    fn rerun_preserves_ancestor_chain() {
        let store = Arc::new(InMemoryStore::new());
        let original_id = Uuid::new_v4().to_string();
        let original = make_run_record(
            &original_id,
            5,
            Value::Map(BTreeMap::new()),
            Value::Null,
        );
        store.put_run(original.clone()).unwrap();

        let mut runtime = Runtime::new(store.clone());
        runtime.register_engine("uiua", Arc::new(DummyEngine {}));
        let config = parse_config(&original.config_snapshot).unwrap();
        let inputs = extract_inputs(original.input_snapshot.clone()).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let new_record = rt
            .block_on(runtime.execute_pipeline(&config, inputs, Some(
                Uuid::parse_str(&original_id).unwrap(),
            )))
            .unwrap();
        assert_eq!(new_record.metadata.ancestor_run_id, Some(Uuid::parse_str(&original_id).unwrap()));
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn cli_run_command_persists_run_to_rocksdb() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = std::env::temp_dir().join(format!("manifold_cli_integration_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir)?;

        let db_path = temp_dir.join("db");
        let store = Arc::new(RocksDbStore::open(db_path.to_str().unwrap())?);

        let config_yaml = r#"name: test-pipeline
nodes:
  - name: first
    engine: uiua
    code: first
    inputs: []
  - name: second
    engine: uiua
    code: second
    inputs:
      - first
"#;
        let config_path = temp_dir.join("pipeline.yaml");
        fs::write(&config_path, config_yaml)?;

        run_command(store.clone(), temp_dir.clone(), Some(config_path), None).await?;

        let runs = store.list_runs()?;
        assert_eq!(runs.len(), 1);

        let run_id = &runs[0].run_id;
        let persisted = store.get_run(run_id)?.expect("run record should exist");
        assert_eq!(persisted.metadata.run_id, *run_id);
        assert_eq!(persisted.metadata.status, RunStatus::Completed);

        Ok(())
    }

    fn make_run_record(id: &str, duration_ms: u128, input_snapshot: Value, output_snapshot: Value) -> RunRecord {
        RunRecord {
            metadata: RunMetadata {
                run_id: id.to_string(),
                pipeline_name: "demo".to_string(),
                status: RunStatus::Completed,
                start_time: chrono::Utc::now(),
                end_time: chrono::Utc::now(),
                duration_ms,
                nodes_executed: 0,
                ancestor_run_id: None,
                error_message: None,
            },
            nodes: vec![],
            config_snapshot: "name: demo\nnodes: []\n".to_string(),
            input_snapshot,
            output_snapshot,
        }
    }
}

