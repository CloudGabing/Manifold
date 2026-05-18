use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fmt::Debug,
    panic::AssertUnwindSafe,

    path::{Path, PathBuf},
    sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex as StdMutex},
    thread,
    time::{Duration, Instant},
};
use async_trait::async_trait;

use anyhow::{anyhow, Context, Result};
use futures::{FutureExt, stream::{FuturesUnordered, StreamExt}};
use manifold_config::{NodeConfig, PipelineConfig};
use manifold_store::{Store, Result as StoreResult};
use manifold_types::{NodeRecord, NodeStatus, RunMetadata, RunRecord, RunStatus, Value};
use tokio::{sync::{Mutex, RwLock, Semaphore, mpsc::UnboundedSender}};
use uuid::Uuid;
use wasmtime::{Config, Engine as WasmEngineHost, Memory, Module, Store as WasmStore, StoreLimitsBuilder, Trap, TypedFunc, Linker, Caller};
// `wat` is used directly where needed; avoid single-component import lint by referring to it via path

#[async_trait]
pub trait ExecutionEngine: Send + Sync + Debug {
    async fn execute(&self, code: &str, input: Value) -> Result<Value>;
}

#[derive(Debug)]
pub struct NativeEngine;

impl NativeEngine {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NativeEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExecutionEngine for NativeEngine {
    async fn execute(&self, code: &str, input: Value) -> Result<Value> {
        let mut map = BTreeMap::new();
        map.insert("engine".to_string(), Value::String("native".to_string()));
        map.insert("code".to_string(), Value::String(code.to_string()));
        map.insert("input".to_string(), input);
        Ok(Value::Map(map))
    }
}

#[derive(Default, Debug)]
pub struct IdMapper {
    ids: Vec<String>,
    forward: HashMap<String, usize>,
    reverse: HashMap<usize, String>,
}

impl IdMapper {
    pub fn register_id(&mut self, id: &str) -> usize {
        if let Some(&index) = self.forward.get(id) {
            return index;
        }
        let index = self.ids.len();
        self.ids.push(id.to_string());
        self.forward.insert(id.to_string(), index);
        self.reverse.insert(index, id.to_string());
        index
    }

    pub fn resolve_id(&self, id: &str) -> Option<usize> {
        self.forward.get(id).copied()
    }

    pub fn resolve_index(&self, index: usize) -> Option<&str> {
        self.reverse.get(&index).map(String::as_str)
    }

    pub fn remove_id(&mut self, id: &str) -> bool {
        if let Some(index) = self.forward.remove(id) {
            self.ids.remove(index);
            self.rebuild_indices();
            true
        } else {
            false
        }
    }

    fn rebuild_indices(&mut self) {
        self.forward.clear();
        self.reverse.clear();
        for (index, id) in self.ids.iter().enumerate() {
            self.forward.insert(id.clone(), index);
            self.reverse.insert(index, id.clone());
        }
    }
}

pub struct InstanceNamespace {
    inner: Arc<dyn Store>,
    prefix: String,
}

impl InstanceNamespace {
    pub fn new(inner: Arc<dyn Store>, dapp_id: &str) -> Self {
        let prefix = format!("app:{}:", dapp_id);
        Self { inner, prefix }
    }

    fn namespace_key(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }

    fn namespace_segment(&self, segment: &str) -> String {
        format!("{}{}", self.prefix, segment)
    }

    fn strip_namespace(&self, text: &str) -> String {
        text.strip_prefix(&self.prefix).map(|suffix| suffix.to_string()).unwrap_or_else(|| text.to_string())
    }
}

impl Store for InstanceNamespace {
    fn put_run(&self, record: RunRecord) -> StoreResult<()> {
        self.inner.put_run(record)
    }

    fn get_run(&self, run_id: &str) -> StoreResult<Option<RunRecord>> {
        self.inner.get_run(run_id)
    }

    fn list_runs(&self) -> StoreResult<Vec<RunMetadata>> {
        self.inner.list_runs()
    }

    fn put_edge(&self, edge_type: &str, edge_id: &str, targets: &[String]) -> StoreResult<()> {
        let namespaced_type = self.namespace_segment(edge_type);
        let namespaced_targets = targets.iter().map(|target| self.namespace_segment(target)).collect::<Vec<_>>();
        self.inner.put_edge(&namespaced_type, edge_id, &namespaced_targets)
    }

    fn get_edge_targets(&self, edge_type: &str, edge_id: &str) -> StoreResult<Option<Vec<String>>> {
        let namespaced_type = self.namespace_segment(edge_type);
        let targets = self.inner.get_edge_targets(&namespaced_type, edge_id)?;
        Ok(targets.map(|list| list.into_iter().map(|target| self.strip_namespace(&target)).collect()))
    }

    fn get_vertex_edges(&self, target_id: &str) -> StoreResult<Vec<String>> {
        let namespaced_target = self.namespace_segment(target_id);
        let refs = self.inner.get_vertex_edges(&namespaced_target)?;
        Ok(refs.into_iter().map(|edge_ref| self.strip_namespace(&edge_ref)).collect())
    }

    fn put_entry(&self, key: &str, value: Value) -> StoreResult<()> {
        let namespaced_key = self.namespace_key(key);
        self.inner.put_entry(&namespaced_key, value)
    }

    fn get_entry(&self, key: &str) -> StoreResult<Option<Value>> {
        let namespaced_key = self.namespace_key(key);
        self.inner.get_entry(&namespaced_key)
    }

    fn delete_entry(&self, key: &str) -> StoreResult<()> {
        let namespaced_key = self.namespace_key(key);
        self.inner.delete_entry(&namespaced_key)
    }

    fn scan_prefix(&self, prefix: &str, limit: usize, offset: usize) -> StoreResult<Vec<(String, Value)>> {
        let namespaced_prefix = self.namespace_key(prefix);
        let results = self.inner.scan_prefix(&namespaced_prefix, limit, offset)?;
        Ok(results
            .into_iter()
            .map(|(key, value)| (self.strip_namespace(&key), value))
            .collect())
    }

    fn delete_edge(&self, edge_type: &str, edge_id: &str) -> StoreResult<()> {
        let namespaced_type = self.namespace_segment(edge_type);
        self.inner.delete_edge(&namespaced_type, edge_id)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct AssetHost {
    // in-memory connection sender; sending a host-side `DuplexStream` into this channel
    conn_tx: Arc<std::sync::Mutex<Option<std::sync::mpsc::Sender<tokio::io::DuplexStream>>>>,
    running: Arc<AtomicBool>,
    handle: StdMutex<Option<thread::JoinHandle<()>>>,
}

impl AssetHost {
    pub fn start(root: PathBuf, logger: Option<UnboundedSender<String>>) -> Result<Self> {
        let root = root
            .canonicalize()
            .context("failed to resolve static frontend directory")?;

        let (tx, rx) = std::sync::mpsc::channel::<tokio::io::DuplexStream>();
        let conn_tx = Arc::new(std::sync::Mutex::new(Some(tx)));

        let running = Arc::new(AtomicBool::new(true));
        let running_thread = running.clone();

        let handle = thread::spawn(move || {
            // create a tokio runtime to process async DuplexStreams
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime for AssetHost");

            // run a blocking loop receiving host-side streams and hand them to the runtime for async handling
            loop {
                if !running_thread.load(Ordering::SeqCst) {
                    break;
                }
                match rx.recv() {
                    Ok(mut stream) => {
                        let root_clone = root.clone();
                        let logger_clone = logger.clone();
                        let handle = rt.handle().clone();
                        handle.spawn(async move {
                            let _ = handle_asset_request_async(&mut stream, &root_clone, logger_clone.as_ref()).await;
                        });
                    }
                    Err(_) => {
                        // sender dropped, shut down
                        break;
                    }
                }
            }
        });

        Ok(Self { conn_tx, running, handle: StdMutex::new(Some(handle)) })
    }

    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        if let Ok(mut guard) = self.conn_tx.lock() {
            *guard = None; // drop sender to signal worker thread to exit
        }
        if let Ok(mut guard) = self.handle.lock() {
            if let Some(h) = guard.take() {
                let _ = h.join();
            }
        }
    }

    /// Return a human-readable in-memory endpoint identifier.
    pub fn url(&self) -> String {
        "in-memory://asset-host".to_string()
    }

    /// Create an in-memory duplex connection pair and queue the host-side for processing by the AssetHost.
    /// Returns the guest-side `DuplexStream` which the guest can use to send a raw HTTP request.
    pub fn connect(&self) -> Result<tokio::io::DuplexStream> {
        let (guest, host) = tokio::io::duplex(16 * 1024);
        let guard = self.conn_tx.lock().map_err(|_| anyhow!("asset host channel lock poisoned"))?;
        if let Some(sender) = guard.as_ref() {
            sender.send(host).map_err(|e| anyhow!("failed to send host stream to asset host: {}", e))?;
            Ok(guest)
        } else {
            Err(anyhow!("asset host is shutting down"))
        }
    }

    pub async fn fetch_asset_bytes(&self, path: &str) -> Result<Vec<u8>> {
        let mut guest_stream = self.connect()?;
        let request_path = path.strip_prefix('/').unwrap_or(path);
        let request = format!("GET /{} HTTP/1.1\r\nHost: asset-host\r\nConnection: close\r\n\r\n", request_path);
        tokio::io::AsyncWriteExt::write_all(&mut guest_stream, request.as_bytes())
            .await
            .context("failed to write asset request to duplex stream")?;

        let mut response = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut guest_stream, &mut response)
            .await
            .context("failed to read asset response from duplex stream")?;

        let body = if let Some(separator_index) = response.windows(4).position(|w| w == b"\r\n\r\n") {
            response.split_off(separator_index + 4)
        } else {
            response
        };

        Ok(body)
    }
}
async fn handle_asset_request_async(stream: &mut tokio::io::DuplexStream, root: &Path, logger: Option<&UnboundedSender<String>>) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buffer = vec![0u8; 4096];
    let read_len = stream.read(&mut buffer).await?;
    if read_len == 0 {
        return Ok(());
    }
    let request = String::from_utf8_lossy(&buffer[..read_len]);
    let first_line = request.lines().next().unwrap_or("");
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("/");

    if method != "GET" {
        let response = "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n";
        if let Some(tx) = logger {
            let _ = tx.send(format!("[WARN][AssetHost] {} {} - 405 Method Not Allowed", method, path));
        }
        stream.write_all(response.as_bytes()).await?;
        return Ok(());
    }

    let safe_path = path.trim_start_matches('/');
    let safe_path = if safe_path.is_empty() { "index.html" } else { safe_path };
    let file_path = root.join(Path::new(safe_path));
    let canonicalized = file_path.canonicalize().unwrap_or_else(|_| file_path.clone());

    if !canonicalized.starts_with(root) {
        let response = "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n";
        if let Some(tx) = logger {
            let _ = tx.send(format!("[WARN][AssetHost] {} {} - 403 Forbidden (path traversal)", method, path));
        }
        stream.write_all(response.as_bytes()).await?;
        return Ok(());
    }

    let file_path = if canonicalized.is_dir() {
        canonicalized.join("index.html")
    } else {
        canonicalized
    };

    match tokio::fs::read(&file_path).await {
        Ok(body) => {
            let content_type = asset_content_type(&file_path);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                content_type,
                body.len()
            );
            if let Some(tx) = logger {
                let _ = tx.send(format!("[INFO][AssetHost] GET {} - 200 OK ({} bytes)", path, body.len()));
            }
            stream.write_all(response.as_bytes()).await?;
            stream.write_all(&body).await?;
        }
        Err(_) => {
            let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
            stream.write_all(response.as_bytes()).await?;
        }
    }

    Ok(())
}

fn asset_content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|ext| ext.to_str()).unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "webp" => "image/webp",
        _ => "application/octet-stream",
    }
}

fn parse_edge_ref(edge_ref: &str) -> Option<(&str, &str)> {
    if !edge_ref.starts_with("edge:") {
        return None;
    }
    let rest = &edge_ref[5..];
    let (edge_type, edge_id) = rest.split_once(':')?;
    Some((edge_type, edge_id))
}

pub struct Runtime {
    pub version: String,
    store: Arc<dyn Store>,
    engines: HashMap<String, Arc<dyn ExecutionEngine>>,
    id_mapper: Arc<Mutex<IdMapper>>,
}

pub struct GuestModule {
    host: WasmEngineHost,
    log_sender: Option<UnboundedSender<String>>,
    asset_host: Option<Arc<AssetHost>>,
    store: Option<Arc<dyn Store>>,
}

const DEFAULT_WASM_FUEL_LIMIT: u64 = 1_000_000;
const DEFAULT_WASM_TIMEOUT_MS: u64 = 5_000;

impl GuestModule {
    fn wasm_fuel_limit() -> u64 {
        std::env::var("MANIFOLD_WASM_FUEL_LIMIT")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_WASM_FUEL_LIMIT)
    }

    fn wasm_timeout_duration() -> Duration {
        let timeout_ms = std::env::var("MANIFOLD_WASM_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_WASM_TIMEOUT_MS);
        Duration::from_millis(timeout_ms)
    }
}

impl std::fmt::Debug for GuestModule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GuestModule")
    }
}

#[allow(dead_code)]
struct WasmStoreState {
    limits: wasmtime::StoreLimits,
}

impl GuestModule {
    pub fn new() -> Self {
        const MAX_WASM_MEMORY: u64 = 128 * 1024 * 1024;
        let mut config = Config::new();
        config.async_support(true);
        config.consume_fuel(true);
        config.static_memory_maximum_size(MAX_WASM_MEMORY);

        let host = WasmEngineHost::new(&config).unwrap_or_else(|err| {
            eprintln!("[WARN][GuestModule] failed to configure wasm sandbox, falling back to default engine: {}", err);
            WasmEngineHost::default()
        });

        Self { host, log_sender: None, asset_host: None, store: None }
    }
    pub fn new_with_logger(logger: Option<UnboundedSender<String>>) -> Self {
        const MAX_WASM_MEMORY: u64 = 128 * 1024 * 1024;
        let mut config = Config::new();
        config.async_support(true);
        config.consume_fuel(true);
        config.static_memory_maximum_size(MAX_WASM_MEMORY);

        let host = WasmEngineHost::new(&config).unwrap_or_else(|err| {
            eprintln!("[WARN][GuestModule] failed to configure wasm sandbox, falling back to default engine: {}", err);
            WasmEngineHost::default()
        });

        Self { host, log_sender: logger, asset_host: None, store: None }
    }

    pub fn new_with_store(store: Arc<dyn Store>) -> Self {
        const MAX_WASM_MEMORY: u64 = 128 * 1024 * 1024;
        let mut config = Config::new();
        config.async_support(true);
        config.consume_fuel(true);
        config.static_memory_maximum_size(MAX_WASM_MEMORY);

        let host = WasmEngineHost::new(&config).unwrap_or_else(|err| {
            eprintln!("[WARN][GuestModule] failed to configure wasm sandbox, falling back to default engine: {}", err);
            WasmEngineHost::default()
        });

        Self { host, log_sender: None, asset_host: None, store: Some(store) }
    }

    pub fn new_with_logger_and_store(logger: Option<UnboundedSender<String>>, store: Arc<dyn Store>) -> Self {
        const MAX_WASM_MEMORY: u64 = 128 * 1024 * 1024;
        let mut config = Config::new();
        config.async_support(true);
        config.consume_fuel(true);
        config.static_memory_maximum_size(MAX_WASM_MEMORY);

        let host = WasmEngineHost::new(&config).unwrap_or_else(|err| {
            eprintln!("[WARN][GuestModule] failed to configure wasm sandbox, falling back to default engine: {}", err);
            WasmEngineHost::default()
        });

        Self { host, log_sender: logger, asset_host: None, store: Some(store) }
    }

    pub fn new_with_asset_host(asset_host: Arc<AssetHost>) -> Self {
        const MAX_WASM_MEMORY: u64 = 128 * 1024 * 1024;
        let mut config = Config::new();
        config.async_support(true);
        config.consume_fuel(true);
        config.static_memory_maximum_size(MAX_WASM_MEMORY);

        let host = WasmEngineHost::new(&config).unwrap_or_else(|err| {
            eprintln!("[WARN][GuestModule] failed to configure wasm sandbox, falling back to default engine: {}", err);
            WasmEngineHost::default()
        });

        Self { host, log_sender: None, asset_host: Some(asset_host), store: None }
    }

    pub fn new_with_logger_and_asset_host(logger: Option<UnboundedSender<String>>, asset_host: Arc<AssetHost>) -> Self {
        const MAX_WASM_MEMORY: u64 = 128 * 1024 * 1024;
        let mut config = Config::new();
        config.async_support(true);
        config.consume_fuel(true);
        config.static_memory_maximum_size(MAX_WASM_MEMORY);

        let host = WasmEngineHost::new(&config).unwrap_or_else(|err| {
            eprintln!("[WARN][GuestModule] failed to configure wasm sandbox, falling back to default engine: {}", err);
            WasmEngineHost::default()
        });

        Self { host, log_sender: logger, asset_host: Some(asset_host), store: None }
    }

// impl Default for GuestModule moved below the impl block to satisfy lint placement

    #[allow(dead_code)]
    fn new_store(&self) -> WasmStore<WasmStoreState> {
        const MAX_WASM_MEMORY_BYTES: usize = 128 * 1024 * 1024;
        let mut store = WasmStore::new(
            &self.host,
            WasmStoreState {
                limits: StoreLimitsBuilder::new()
                    .memory_size(MAX_WASM_MEMORY_BYTES)
                    .instances(256)
                    .tables(1024)
                    .build(),
            },
        );
        store.limiter(|state| &mut state.limits);
        store
    }

    fn parse_i32_input(input: &Value) -> Result<i32> {
        match input {
            Value::Int(i) => i32::try_from(*i).context("wasm numeric run requires a 32-bit integer input"),
            Value::String(s) => s
                .parse::<i32>()
                .context("wasm numeric run requires a string that parses as an integer"),
            other => Err(anyhow!(
                "wasm numeric run requires an integer input, got {}",
                serde_json::to_string(other).unwrap_or_else(|_| format!("{:?}", other))
            )),
        }
    }

    fn bytes_from_input(input: &Value) -> Result<Arc<[u8]>> {
        match input {
            Value::Bytes(b) => Ok(b.clone()),
            Value::String(s) => Ok(Arc::from(s.as_bytes().to_vec().into_boxed_slice())),
            Value::Int(i) => Ok(Arc::from(i.to_string().into_bytes().into_boxed_slice())),
            other => serde_json::to_vec(other)
                .map(|bytes| Arc::from(bytes.into_boxed_slice()))
                .context("failed to serialize wasm input"),
        }
    }

    fn validate_memory_bounds<'a>(
        mem: &Memory,
        store: &'a mut WasmStore<WasmStoreState>,
        ptr: i32,
        len: i32,
    ) -> Result<&'a mut [u8]> {
        let ptr: usize = ptr.try_into().context("wasm pointer must be a non-negative 32-bit integer")?;
        let len: usize = len.try_into().context("wasm buffer length must be a non-negative 32-bit integer")?;
        let data = mem.data_mut(store);
        let end = ptr
            .checked_add(len)
            .ok_or_else(|| anyhow!("wasm pointer and length overflow memory bounds"))?;
        if end > data.len() {
            return Err(anyhow!("wasm memory access out of bounds: ptr={} len={} memory_size={}", ptr, len, data.len()));
        }
        Ok(&mut data[ptr..end])
    }

    async fn call_free(
        store: &mut WasmStore<WasmStoreState>,
        free2: &Option<TypedFunc<(i32, i32), ()>>,
        free1: &Option<TypedFunc<i32, ()>>,
        ptr: i32,
        len: i32,
    ) -> Result<()> {
        if let Some(free_fn) = free2 {
            free_fn
                .call_async(store, (ptr, len))
                .await
                .map_err(|e| anyhow!(e.to_string()))
                .context("wasm free call failed")?;
        } else if let Some(free_fn) = free1 {
            free_fn
                .call_async(store, ptr)
                .await
                .map_err(|e| anyhow!(e.to_string()))
                .context("wasm free call failed")?;
        }
        Ok(())
    }

    fn is_fuel_error(err: &anyhow::Error) -> bool {
        err.downcast_ref::<Trap>()
            .map(|trap| trap.to_string().contains("all fuel consumed") || trap.to_string().contains("out of fuel"))
            .unwrap_or(false)
            || err.to_string().contains("all fuel consumed")
            || err.to_string().contains("out of fuel")
    }

    fn map_wasm_execution_error(err: anyhow::Error) -> anyhow::Error {
        if Self::is_fuel_error(&err) {
            anyhow!("WASM Execution Timeout / Fuel Exhausted")
        } else {
            err
        }
    }
}

impl Default for GuestModule {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExecutionEngine for GuestModule {
    async fn execute(&self, code: &str, input: Value) -> Result<Value> {
        let code = code.to_string();
        let input_clone = input.clone();
        let host = self.host.clone();
        let log_sender = self.log_sender.clone();
        let asset_host = self.asset_host.clone();
        let store_ref = self.store.clone();

        // compile module on blocking pool
        let wasm_bytes = wat::parse_str(&code).context("failed to parse WAT code")?;
        let host_for_compile = host.clone();
        let module = tokio::task::spawn_blocking(move || -> Result<Module> {
            Module::new(&host_for_compile, &wasm_bytes).context("failed to compile wasm module")
        })
        .await
        .context("wasm compile task aborted")??;

        let fuel_limit = GuestModule::wasm_fuel_limit();
        let execution_timeout = GuestModule::wasm_timeout_duration();
        let execution_start = Instant::now();

        let engine_log_sender = log_sender.clone();
        let engine_handle = tokio::spawn(async move {
            let mut store = WasmStore::new(
                &host,
                WasmStoreState {
                    limits: StoreLimitsBuilder::new().build(),
                },
            );
            store
                .add_fuel(fuel_limit)
                .context("failed to add wasm execution fuel")?;

            let mut linker = Linker::new(&host);
            let asset_host = asset_host.clone();
            let store_for_set = store_ref.clone();
            let store_for_get = store_ref.clone();
            let logger_asset = engine_log_sender.clone();
            let logger_store_set = engine_log_sender.clone();
            let logger_store_get = engine_log_sender.clone();
            let logger_host_log = engine_log_sender.clone();

            linker.func_wrap4_async(
                "env",
                "asset_request",
                move |mut caller: Caller<'_, WasmStoreState>, path_ptr: u32, path_len: u32, dest_ptr: u32, dest_max_len: u32| {
                    let asset_host = asset_host.clone();
                    let logger = logger_asset.clone();
                    Box::new(async move {
                        let warn = |message: String| {
                            if let Some(tx) = logger.as_ref() {
                                let _ = tx.send(format!("[WARN][GuestModule] {}", message));
                            }
                        };

                        let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
                            Some(mem) => mem,
                            None => {
                                warn("asset_request: guest memory export not available".to_string());
                                return Ok(0);
                            }
                        };

                        let path_start = path_ptr as usize;
                        let path_len = path_len as usize;
                        let path_end = match path_start.checked_add(path_len) {
                            Some(end) => end,
                            None => {
                                warn("asset_request: path pointer overflow".to_string());
                                return Ok(0);
                            }
                        };

                        let data = memory.data(&caller);
                        if path_end > data.len() {
                            warn(format!("asset_request: path buffer out of bounds ({}..{} / {})", path_start, path_end, data.len()));
                            return Ok(0);
                        }

                        let path_bytes = &data[path_start..path_end];
                        let path_str = match std::str::from_utf8(path_bytes) {
                            Ok(s) => s,
                            Err(err) => {
                                warn(format!("asset_request: invalid UTF-8 path: {}", err));
                                return Ok(0);
                            }
                        };

                        let dest_start = dest_ptr as usize;
                        let dest_max_len = dest_max_len as usize;
                        let dest_end = match dest_start.checked_add(dest_max_len) {
                            Some(end) => end,
                            None => {
                                warn("asset_request: destination pointer overflow".to_string());
                                return Ok(0);
                            }
                        };

                        if dest_end > data.len() {
                            warn(format!("asset_request: destination region out of bounds ({}..{} / {})", dest_start, dest_end, data.len()));
                            return Ok(0);
                        }

                        let bytes = if let Some(asset_host) = asset_host.as_ref() {
                            match asset_host.fetch_asset_bytes(path_str).await {
                                Ok(bytes) => bytes,
                                Err(err) => {
                                    warn(format!("asset_request: failed to fetch asset '{}': {}", path_str, err));
                                    return Ok(0);
                                }
                            }
                        } else {
                            warn("asset_request: no asset host configured".to_string());
                            return Ok(0);
                        };

                        let write_len = std::cmp::min(bytes.len(), dest_max_len);
                        let dest_slice = &mut memory.data_mut(&mut caller)[dest_start..dest_start + write_len];
                        dest_slice.copy_from_slice(&bytes[..write_len]);

                        Ok(write_len as u32)
                    })
                },
            )?;

            linker.func_wrap(
                "env",
                "store_set",
                move |mut caller: Caller<'_, WasmStoreState>, key_ptr: i32, key_len: i32, value_ptr: i32, value_len: i32| {
                    let logger = logger_store_set.clone();
                    let warn = |message: String| {
                        if let Some(tx) = logger.as_ref() {
                            let _ = tx.send(format!("[WARN][GuestModule] {}", message));
                        }
                    };

                    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
                        Some(mem) => mem,
                        None => {
                            warn("store_set: guest memory export not available".to_string());
                            return -3;
                        }
                    };

                    let data = memory.data(&caller);
                    let key_start = key_ptr as usize;
                    let key_len = key_len as usize;
                    let key_end = match key_start.checked_add(key_len) {
                        Some(end) => end,
                        None => {
                            warn("store_set: key pointer overflow".to_string());
                            return -3;
                        }
                    };
                    if key_end > data.len() {
                        warn(format!("store_set: key region out of bounds ({}..{} / {})", key_start, key_end, data.len()));
                        return -3;
                    }

                    let value_start = value_ptr as usize;
                    let value_len = value_len as usize;
                    let value_end = match value_start.checked_add(value_len) {
                        Some(end) => end,
                        None => {
                            warn("store_set: value pointer overflow".to_string());
                            return -3;
                        }
                    };
                    if value_end > data.len() {
                        warn(format!("store_set: value region out of bounds ({}..{} / {})", value_start, value_end, data.len()));
                        return -3;
                    }

                    let key = match std::str::from_utf8(&data[key_start..key_end]) {
                        Ok(key) => key,
                        Err(err) => {
                            warn(format!("store_set: invalid UTF-8 key: {}", err));
                            return -3;
                        }
                    };

                    let value_bytes = data[value_start..value_end].to_vec();
                    if let Some(store) = store_for_set.as_ref() {
                        let value = Value::Bytes(Arc::from(value_bytes));
                        if let Err(err) = store.put_entry(key, value) {
                            warn(format!("store_set: host storage error: {}", err));
                            return -4;
                        }
                        0
                    } else {
                        -1
                    }
                },
            )?;

            linker.func_wrap(
                "env",
                "store_get",
                move |mut caller: Caller<'_, WasmStoreState>, key_ptr: i32, key_len: i32, dest_ptr: i32, dest_max_len: i32| {
                    let logger = logger_store_get.clone();
                    let warn = |message: String| {
                        if let Some(tx) = logger.as_ref() {
                            let _ = tx.send(format!("[WARN][GuestModule] {}", message));
                        }
                    };

                    let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
                        Some(mem) => mem,
                        None => {
                            warn("store_get: guest memory export not available".to_string());
                            return -3;
                        }
                    };

                    let data = memory.data(&caller);
                    let key_start = key_ptr as usize;
                    let key_len = key_len as usize;
                    let key_end = match key_start.checked_add(key_len) {
                        Some(end) => end,
                        None => {
                            warn("store_get: key pointer overflow".to_string());
                            return -3;
                        }
                    };
                    if key_end > data.len() {
                        warn(format!("store_get: key region out of bounds ({}..{} / {})", key_start, key_end, data.len()));
                        return -3;
                    }

                    let key = match std::str::from_utf8(&data[key_start..key_end]) {
                        Ok(key) => key,
                        Err(err) => {
                            warn(format!("store_get: invalid UTF-8 key: {}", err));
                            return -3;
                        }
                    };

                    let dest_start = dest_ptr as usize;
                    let dest_max_len = dest_max_len as usize;
                    let dest_end = match dest_start.checked_add(dest_max_len) {
                        Some(end) => end,
                        None => {
                            warn("store_get: destination pointer overflow".to_string());
                            return -3;
                        }
                    };
                    if dest_end > data.len() {
                        warn(format!("store_get: destination region out of bounds ({}..{} / {})", dest_start, dest_end, data.len()));
                        return -3;
                    }

                    if let Some(store) = store_for_get.as_ref() {
                        match store.get_entry(key) {
                            Ok(Some(Value::Bytes(bytes))) => {
                                if bytes.len() > dest_max_len {
                                    return -10 - (bytes.len() as i32);
                                }
                                let dest_slice = &mut memory.data_mut(&mut caller)[dest_start..dest_start + bytes.len()];
                                dest_slice.copy_from_slice(&bytes);
                                bytes.len() as i32
                            }
                            Ok(Some(_)) => {
                                warn(format!("store_get: stored value for '{}' was not binary", key));
                                -4
                            }
                            Ok(None) => -2,
                            Err(err) => {
                                warn(format!("store_get: host storage error: {}", err));
                                -4
                            }
                        }
                    } else {
                        -1
                    }
                },
            )?;

            linker.func_wrap(
                "env",
                "host_log",
                move |mut caller: Caller<'_, WasmStoreState>, ptr: i32, len: i32| {
                    if let Some(mem) = caller.get_export("memory").and_then(|e| e.into_memory()) {
                        let data = mem.data(&caller);
                        let start = ptr as usize;
                        let end = start + len as usize;
                        if end <= data.len() {
                            let text = String::from_utf8_lossy(&data[start..end]).to_string();
                            if let Some(tx1) = logger_host_log.as_ref() {
                                let _ = tx1.send(format!("[INFO][GuestModule] {}", text));
                            }
                        }
                    }
                    Ok(())
                },
            )?;

            let instance = linker
                .instantiate_async(&mut store, &module)
                .await
                .context("failed to instantiate module")?;

            let memory = instance.get_memory(&mut store, "memory");
            let memory_allocated = memory
                .as_ref()
                .map(|mem| mem.size(&store) * 65536)
                .unwrap_or(0);

            let result = if let Ok(func) = instance.get_typed_func::<(i32,), i32>(&mut store, "run") {
                let arg = GuestModule::parse_i32_input(&input_clone)?;
                let result = func
                    .call_async(&mut store, (arg,))
                    .await
                    .map_err(|e| GuestModule::map_wasm_execution_error(anyhow!(e.to_string())))?;
                Ok(Value::Int(result as i64))
            } else {
                let alloc = instance.get_typed_func::<i32, i32>(&mut store, "alloc");
                let run = instance.get_typed_func::<(i32, i32), (i32, i32)>(&mut store, "run");
                let free1 = instance.get_typed_func::<i32, ()>(&mut store, "free").ok();
                let free2 = instance.get_typed_func::<(i32, i32), ()>(&mut store, "free").ok();

                if let (Ok(alloc), Ok(run)) = (alloc, run) {
                    let input_bytes = GuestModule::bytes_from_input(&input_clone)?;
                    if input_bytes.len() > i32::MAX as usize {
                        return Err(anyhow!("wasm input payload is too large"));
                    }
                    let input_len = input_bytes.len() as i32;

                    let mem: Memory = instance
                        .get_memory(&mut store, "memory")
                        .ok_or_else(|| anyhow!("wasm module missing exported memory for alloc/run interop"))?;

                    let ptr = alloc
                        .call_async(&mut store, input_len)
                        .await
                        .map_err(|e| GuestModule::map_wasm_execution_error(anyhow!(e.to_string())))?;
                    if ptr < 0 {
                        return Err(anyhow!("wasm alloc returned negative pointer"));
                    }

                    let write_slice = GuestModule::validate_memory_bounds(&mem, &mut store, ptr, input_len)?;
                    write_slice.copy_from_slice(&input_bytes);

                    let (out_ptr, out_len) = run
                        .call_async(&mut store, (ptr, input_len))
                        .await
                        .map_err(|e| GuestModule::map_wasm_execution_error(anyhow!(e.to_string())))?;

                    let output = if out_len == 0 {
                        Value::Bytes(Arc::from(Vec::<u8>::new().into_boxed_slice()))
                    } else {
                        let read_slice = GuestModule::validate_memory_bounds(&mem, &mut store, out_ptr, out_len)?;
                        match std::str::from_utf8(read_slice) {
                            Ok(text) => Value::String(text.to_string()),
                            Err(_) => Value::Bytes(Arc::from(read_slice.to_vec().into_boxed_slice())),
                        }
                    };

                    let mut freed_input = false;
                    if out_len != 0 && out_ptr == ptr && out_len == input_len {
                        if let Err(err) = GuestModule::call_free(&mut store, &free2, &free1, ptr, input_len).await {
                            return Err(err.context("failed to free shared wasm buffer"));
                        }
                        freed_input = true;
                    }

                    if out_len != 0 && !(out_ptr == ptr && out_len == input_len) {
                        if let Err(err) = GuestModule::call_free(&mut store, &free2, &free1, out_ptr, out_len).await {
                            return Err(err.context("failed to free output buffer in wasm module"));
                        }
                    }

                    if !freed_input {
                        if let Err(err) = GuestModule::call_free(&mut store, &free2, &free1, ptr, input_len).await {
                            return Err(err.context("failed to free input buffer in wasm module"));
                        }
                    }

                    Ok(output)
                } else {
                    Err(anyhow!("wasm module does not export a supported run signature"))
                }
            };

            let fuel_consumed = store.fuel_consumed().unwrap_or(0);
            let duration_ms = execution_start.elapsed().as_millis();
            Ok((result, duration_ms, fuel_consumed, memory_allocated))
        });

        tokio::pin!(engine_handle);
        let execution_result = tokio::select! {
            engine_result = &mut engine_handle => {
                match engine_result {
                    Ok(inner_result) => match inner_result {
                        Ok((result, duration_ms, fuel_consumed, memory_allocated)) => {
                            if let Some(logger) = log_sender.as_ref() {
                                let _ = logger.send(format!("[METRICS][GuestModule] duration_ms={} fuel_consumed={} memory_allocated_bytes={}", duration_ms, fuel_consumed, memory_allocated));
                            }
                            result
                        }
                        Err(err) => Err(err),
                    },
                    Err(join_err) => Err(anyhow!("Guest module task aborted or panicked: {}", join_err)),
                }
            }
            _ = tokio::time::sleep(execution_timeout) => {
                let _ = log_sender.as_ref().map(|logger| {
                    let _ = logger.send(format!("[ERROR][Runtime] Guest exceeded CPU fuel quota after {}ms", execution_timeout.as_millis()));
                });
                engine_handle.abort();
                Err(anyhow!("Guest exceeded CPU fuel quota after {}ms", execution_timeout.as_millis()))
            }
        };

        execution_result
    }
}

impl Runtime {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self {
            version: "0.1.0".to_string(),
            store,
            engines: HashMap::new(),
            id_mapper: Arc::new(Mutex::new(IdMapper::default())),
        }
    }

    pub fn new_with_namespace(store: Arc<dyn Store>, dapp_id: &str) -> Self {
        let wrapped_store = Arc::new(InstanceNamespace::new(store, dapp_id));
        Self {
            version: "0.1.0".to_string(),
            store: wrapped_store,
            engines: HashMap::new(),
            id_mapper: Arc::new(Mutex::new(IdMapper::default())),
        }
    }

    pub fn register_engine(&mut self, name: impl Into<String>, engine: Arc<dyn ExecutionEngine>) {
        self.engines.insert(name.into(), engine);
    }

    pub async fn register_vertex_id(&self, vertex_id: &str) -> usize {
        let mut mapper = self.id_mapper.lock().await;
        mapper.register_id(vertex_id)
    }

    pub async fn resolve_vertex_id(&self, vertex_id: &str) -> Option<usize> {
        let mapper = self.id_mapper.lock().await;
        mapper.resolve_id(vertex_id)
    }

    pub async fn resolve_vertex_index(&self, index: usize) -> Option<String> {
        let mapper = self.id_mapper.lock().await;
        mapper.resolve_index(index).map(String::from)
    }

    pub async fn deregister_vertex_id(&self, vertex_id: &str) -> bool {
        let mut mapper = self.id_mapper.lock().await;
        mapper.remove_id(vertex_id)
    }

    pub async fn register_hyperedge(&self, edge_type: &str, edge_id: &str, targets: &[Value]) -> Result<()> {
        let collection: Vec<String> = targets.iter().map(Value::normalize_to_id_string).collect();
        for vertex_id in collection.iter() {
            self.register_vertex_id(vertex_id).await;
        }
        self.store.put_edge(edge_type, edge_id, &collection)?;
        Ok(())
    }

    pub async fn host_query_vertex_relations(&self, target_id: &str) -> Result<Value> {
        let edge_refs = self.store.get_vertex_edges(target_id)?;
        let mut rows = Vec::new();

        for edge_ref in edge_refs {
            if let Some((edge_type, edge_id)) = parse_edge_ref(&edge_ref) {
                if let Some(targets) = self.store.get_edge_targets(edge_type, edge_id)? {
                    let mut row = BTreeMap::new();
                    row.insert("edge_type".to_string(), Value::String(edge_type.to_string()));
                    row.insert("edge_id".to_string(), Value::String(edge_id.to_string()));
                    row.insert("target_vertex".to_string(), Value::String(target_id.to_string()));
                    row.insert(
                        "members".to_string(),
                        Value::List(targets.into_iter().map(Value::String).collect()),
                    );
                    rows.push(Value::Object(row));
                }
            } else {
                let mut row = BTreeMap::new();
                row.insert("edge_reference".to_string(), Value::String(edge_ref.clone()));
                row.insert(
                    "error".to_string(),
                    Value::String("malformed edge reference".to_string()),
                );
                rows.push(Value::Object(row));
            }
        }

        Ok(Value::Table(rows))
    }

    pub async fn scan_store_prefix(&self, prefix: &str, limit: usize, offset: usize) -> Result<Vec<(String, Value)>> {
        Ok(self.store.scan_prefix(prefix, limit, offset)?)
    }

    pub async fn query_vertex_relations(&self, target_id: &str) -> Result<Vec<(String, Value)>> {
        let relations = self.host_query_vertex_relations(target_id).await?;
        Ok(relations.as_list().unwrap_or_default().iter().enumerate().map(|(idx, row)| {
            (idx.to_string(), row.clone())
        }).collect())
    }

    pub async fn execute_pipeline(
        &self,
        config: &PipelineConfig,
        inputs: HashMap<String, Value>,
        ancestor_run_id: Option<Uuid>,
    ) -> Result<RunRecord> {
        config.validate().context("pipeline configuration validation failed")?;
        self.topological_sort(&config.nodes)?;

        let node_map: HashMap<String, NodeConfig> = config
            .nodes
            .iter()
            .cloned()
            .map(|node| (node.name.clone(), node))
            .collect();

        let (dependents, mut pending_counts) = Self::build_dependency_graph(&config.nodes);
        let outputs = Arc::new(RwLock::new(inputs.clone()));
        let completed_nodes = Arc::new(Mutex::new(Vec::new()));
        let failed = Arc::new(AtomicBool::new(false));
        let failure_error = Arc::new(Mutex::new(None::<anyhow::Error>));

        let run_id = Uuid::new_v4().to_string();
        let start_time = chrono::Utc::now();
        let run_start = Instant::now();

        let concurrency_limit = Arc::new(Semaphore::new(64));
        let mut pending_tasks = FuturesUnordered::new();

        for (name, count) in pending_counts.iter() {
            if *count == 0 {
                Self::spawn_node_task(
                    name,
                    &node_map,
                    outputs.clone(),
                    self.engines.clone(),
                    concurrency_limit.clone(),
                    &mut pending_tasks,
                )
                .await?;
            }
        }

        while let Some(join_result) = pending_tasks.next().await {
            let node_result = join_result.context("node task join failed")?;

            match node_result {
                NodeExecutionResult::Success(outcome) => {
                    outputs
                        .write()
                        .await
                        .insert(outcome.node_name.clone(), outcome.output.clone());
                    completed_nodes.lock().await.push(outcome.record.clone());

                    if !failed.load(Ordering::SeqCst) {
                        if let Some(children) = dependents.get(&outcome.node_name) {
                            for child in children {
                                let count = pending_counts
                                    .get_mut(child)
                                    .expect("child should exist in counts");
                                *count = count.saturating_sub(1);
                                if *count == 0 {
                                    Self::spawn_node_task(
                                        child,
                                        &node_map,
                                        outputs.clone(),
                                        self.engines.clone(),
                                        concurrency_limit.clone(),
                                        &mut pending_tasks,
                                    )
                                    .await?;
                                }
                            }
                        }
                    }
                }
                NodeExecutionResult::Failed(failure) => {
                    completed_nodes.lock().await.push(failure.record);
                    let mut guard = failure_error.lock().await;
                    if !failed.swap(true, Ordering::SeqCst) {
                        *guard = Some(failure.error);
                    }
                }
            }
        }

        let status = if failed.load(Ordering::SeqCst) {
            RunStatus::Failed
        } else {
            RunStatus::Completed
        };

        let end_time = chrono::Utc::now();
        let duration_ms = run_start.elapsed().as_millis();
        let output_snapshot = Value::Map(
            outputs
                .read()
                .await
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        let config_snapshot = serde_yaml::to_string(config)?;

        let metadata = RunMetadata {
            run_id: run_id.clone(),
            pipeline_name: config.name.clone(),
            status,
            start_time,
            end_time,
            duration_ms,
            nodes_executed: completed_nodes.lock().await.len(),
            ancestor_run_id,
            error_message: failure_error.lock().await.as_ref().map(|err| err.to_string()),
        };

        let run_record = RunRecord {
            metadata,
            nodes: completed_nodes.lock().await.clone(),
            config_snapshot,
            input_snapshot: Value::Map(inputs.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            output_snapshot,
        };

        self.store.put_run(run_record.clone())?;
        Ok(run_record)
    }

    fn build_dependency_graph(
        nodes: &[NodeConfig],
    ) -> (HashMap<String, Vec<String>>, HashMap<String, usize>) {
        let node_names: BTreeSet<String> = nodes.iter().map(|node| node.name.clone()).collect();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
        let mut pending_counts: HashMap<String, usize> = HashMap::new();

        for node in nodes {
            pending_counts.insert(node.name.clone(), 0);
            dependents.insert(node.name.clone(), Vec::new());
        }

        for node in nodes {
            let count = node
                .inputs
                .iter()
                .filter(|input| node_names.contains(*input))
                .count();
            *pending_counts.get_mut(&node.name).unwrap() = count;

            for input in &node.inputs {
                if node_names.contains(input) {
                    dependents
                        .get_mut(input)
                        .unwrap()
                        .push(node.name.clone());
                }
            }
        }

        (dependents, pending_counts)
    }

    async fn spawn_node_task(
        node_name: &str,
        node_map: &HashMap<String, NodeConfig>,
        outputs: Arc<RwLock<HashMap<String, Value>>>,
        engines: HashMap<String, Arc<dyn ExecutionEngine>>,
        semaphore: Arc<Semaphore>,
        pending_tasks: &mut FuturesUnordered<tokio::task::JoinHandle<NodeExecutionResult>>,
    ) -> Result<()> {
        let permit = semaphore.acquire_owned().await.expect("task semaphore closed");
        let node = node_map
            .get(node_name)
            .ok_or_else(|| anyhow!("node '{}' not found during scheduling", node_name))?
            .clone();
        let outputs_clone = outputs.clone();
        // If the engine isn't registered we schedule an immediate failed outcome
        if !engines.contains_key(node.engine.as_str()) {
            let started_at = chrono::Utc::now();
            let ended_at = chrono::Utc::now();
            let record = NodeRecord {
                name: node.name.clone(),
                engine: node.engine.clone(),
                code: node.code.clone(),
                inputs: Value::Null,
                output: None,
                status: NodeStatus::Failed,
                started_at,
                ended_at,
                error: Some(format!("engine '{}' is not registered", node.engine)),
            };
            let err = anyhow!("engine '{}' is not registered", node.engine);
            let future = tokio::spawn(async move { NodeExecutionResult::Failed(NodeFailure { record, error: err }) });
            pending_tasks.push(future);
            return Ok(());
        }

        let engine = engines.get(node.engine.as_str()).unwrap().clone();

        let future = tokio::spawn(async move {
            let _permit = permit;
            let node_inputs = match Self::collect_node_inputs(&node, outputs_clone.clone()).await {
                Ok(value) => value,
                Err(err) => {
                    let started_at = chrono::Utc::now();
                    let ended_at = chrono::Utc::now();
                    let record = NodeRecord {
                        name: node.name.clone(),
                        engine: node.engine.clone(),
                        code: node.code.clone(),
                        inputs: Value::Null,
                        output: None,
                        status: NodeStatus::Failed,
                        started_at,
                        ended_at,
                        error: Some(format!("input resolution failed: {}", err)),
                    };
                    return NodeExecutionResult::Failed(NodeFailure { record, error: err });
                }
            };

            let node_start = chrono::Utc::now();
            let code = node.code.clone();
            let input_clone = node_inputs.clone();
            let engine_clone = engine.clone();

            match AssertUnwindSafe(engine_clone.execute(&code, input_clone)).catch_unwind().await {
                Ok(Ok(output)) => {
                    let node_end = chrono::Utc::now();
                    let record = NodeRecord {
                        name: node.name.clone(),
                        engine: node.engine.clone(),
                        code: node.code.clone(),
                        inputs: node_inputs,
                        output: Some(output.clone()),
                        status: NodeStatus::Completed,
                        started_at: node_start,
                        ended_at: node_end,
                        error: None,
                    };
                    NodeExecutionResult::Success(NodeOutcome { node_name: node.name.clone(), record, output })
                }
                Ok(Err(err)) => {
                    let node_end = chrono::Utc::now();
                    let record = NodeRecord {
                        name: node.name.clone(),
                        engine: node.engine.clone(),
                        code: node.code.clone(),
                        inputs: node_inputs,
                        output: None,
                        status: NodeStatus::Failed,
                        started_at: node_start,
                        ended_at: node_end,
                        error: Some(err.to_string()),
                    };
                    NodeExecutionResult::Failed(NodeFailure { record, error: err })
                }
                Err(panic_payload) => {
                    let node_end = chrono::Utc::now();
                    let error_message = if let Some(message) = panic_payload.downcast_ref::<&str>() {
                        message.to_string()
                    } else if let Some(message) = panic_payload.downcast_ref::<String>() {
                        message.clone()
                    } else {
                        "plugin panic occurred".to_string()
                    };
                    let record = NodeRecord {
                        name: node.name.clone(),
                        engine: node.engine.clone(),
                        code: node.code.clone(),
                        inputs: node_inputs,
                        output: None,
                        status: NodeStatus::Failed,
                        started_at: node_start,
                        ended_at: node_end,
                        error: Some(error_message.clone()),
                    };
                    NodeExecutionResult::Failed(NodeFailure { record, error: anyhow!("plugin panic: {}", error_message) })
                }
            }
        });

        pending_tasks.push(future);
        Ok(())
    }

    async fn collect_node_inputs(node: &NodeConfig, outputs: Arc<RwLock<HashMap<String, Value>>>) -> Result<Value> {
        let values = outputs.read().await;
        match node.inputs.len() {
            0 => Ok(Value::Null),
            1 => values
                .get(&node.inputs[0])
                .cloned()
                .ok_or_else(|| anyhow!("missing input '{}' for node '{}'", node.inputs[0], node.name)),
            _ => {
                let mut map = BTreeMap::new();
                for input_name in &node.inputs {
                    let value = values
                        .get(input_name)
                        .cloned()
                        .ok_or_else(|| anyhow!("missing input '{}' for node '{}'", input_name, node.name))?;
                    map.insert(input_name.clone(), value);
                }
                Ok(Value::Map(map))
            }
        }
    }

    fn topological_sort<'a>(&self, nodes: &'a [NodeConfig]) -> Result<Vec<&'a NodeConfig>> {
        let mut incoming: HashMap<&str, usize> = HashMap::new();
        let mut outbound: HashMap<&str, Vec<&str>> = HashMap::new();

        for node in nodes {
            incoming.insert(node.name.as_str(), 0);
            outbound.insert(node.name.as_str(), Vec::new());
        }

        for node in nodes {
            for input in &node.inputs {
                if incoming.contains_key(input.as_str()) {
                    *incoming.get_mut(node.name.as_str()).unwrap() += 1;
                    outbound
                        .get_mut(input.as_str())
                        .unwrap()
                        .push(node.name.as_str());
                }
            }
        }

        let mut ready: VecDeque<&str> = incoming
            .iter()
            .filter_map(|(&name, &count)| if count == 0 { Some(name) } else { None })
            .collect();

        let mut result = Vec::with_capacity(nodes.len());
        while let Some(name) = ready.pop_front() {
            if let Some(node) = nodes.iter().find(|node| node.name == name) {
                result.push(node);
                for dependent in &outbound[name] {
                    let count = incoming.get_mut(dependent).unwrap();
                    *count -= 1;
                    if *count == 0 {
                        ready.push_back(dependent);
                    }
                }
            }
        }

        if result.len() != nodes.len() {
            Err(anyhow!("pipeline contains a cycle or unresolved dependency"))
        } else {
            Ok(result)
        }
    }
}

struct NodeOutcome {
    node_name: String,
    record: NodeRecord,
    output: Value,
}

struct NodeFailure {
    record: NodeRecord,
    error: anyhow::Error,
}

enum NodeExecutionResult {
    Success(NodeOutcome),
    Failed(NodeFailure),
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use manifold_config::{NodeConfig, PipelineConfig};
    use manifold_store::InMemoryStore;
    use manifold_types::{Value, NodeStatus};
    use std::{collections::HashMap, sync::{Arc, atomic::{AtomicUsize, Ordering}, Mutex}};

    #[derive(Debug)]
    struct DummyEngine {
        active_count: Arc<AtomicUsize>,
        peak_count: Arc<AtomicUsize>,
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ExecutionEngine for DummyEngine {
        async fn execute(&self, code: &str, input: Value) -> Result<Value> {
            let active = self.active_count.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_count.fetch_max(active, Ordering::SeqCst);
            self.log.lock().unwrap().push(format!("started-{}", code));
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            self.log.lock().unwrap().push(format!("finished-{}", code));
            self.active_count.fetch_sub(1, Ordering::SeqCst);

            let mut result = BTreeMap::new();
            result.insert("code".to_string(), Value::String(code.to_string()));
            result.insert("input".to_string(), input);
            Ok(Value::Map(result))
        }
    }

    #[tokio::test]
    async fn async_dag_schedules_parallel_nodes() {
        let store = Arc::new(InMemoryStore::new());
        let mut runtime = Runtime::new(store.clone());

        let active_count = Arc::new(AtomicUsize::new(0));
        let peak_count = Arc::new(AtomicUsize::new(0));
        let log = Arc::new(Mutex::new(Vec::new()));

        runtime.register_engine(
            "uiua",
            Arc::new(DummyEngine {
                active_count: active_count.clone(),
                peak_count: peak_count.clone(),
                log: log.clone(),
            }),
        );

        let config = PipelineConfig {
            name: "dag-test".to_string(),
            nodes: vec![
                NodeConfig {
                    name: "A".to_string(),
                    engine: "uiua".to_string(),
                    code: "A".to_string(),
                    inputs: vec![],
                },
                NodeConfig {
                    name: "B".to_string(),
                    engine: "uiua".to_string(),
                    code: "B".to_string(),
                    inputs: vec![],
                },
                NodeConfig {
                    name: "C".to_string(),
                    engine: "uiua".to_string(),
                    code: "C".to_string(),
                    inputs: vec!["A".to_string(), "B".to_string()],
                },
            ],
        };

        let record = runtime.execute_pipeline(&config, HashMap::new(), None).await.unwrap();

        assert_eq!(record.metadata.status, RunStatus::Completed);
        assert!(peak_count.load(Ordering::SeqCst) >= 2, "A and B did not execute concurrently");

        let log_entries = log.lock().unwrap().clone();
        let c_start = log_entries.iter().position(|entry| entry == "started-C").unwrap();
        let a_finish = log_entries.iter().position(|entry| entry == "finished-A").unwrap();
        let b_finish = log_entries.iter().position(|entry| entry == "finished-B").unwrap();
        assert!(a_finish < c_start && b_finish < c_start, "C started before A and B finished");
    }

    #[derive(Debug)]
    struct DummyEngineSimple;

    #[async_trait]
    impl ExecutionEngine for DummyEngineSimple {
        async fn execute(&self, code: &str, input: Value) -> Result<Value> {
            let mut map = BTreeMap::new();
            map.insert("code".to_string(), Value::String(code.to_string()));
            map.insert("input".to_string(), input);
            Ok(Value::Map(map))
        }
    }

    #[tokio::test]
    async fn runtime_executes_pipeline_and_stores_run() {
        let store = Arc::new(InMemoryStore::new());
        let mut runtime = Runtime::new(store.clone());
        runtime.register_engine("uiua", Arc::new(DummyEngineSimple {}));

        let config = PipelineConfig {
            name: "test-pipeline".to_string(),
            nodes: vec![
                NodeConfig {
                    name: "first".to_string(),
                    engine: "uiua".to_string(),
                    code: "1".to_string(),
                    inputs: vec![],
                },
                NodeConfig {
                    name: "second".to_string(),
                    engine: "uiua".to_string(),
                    code: "2".to_string(),
                    inputs: vec!["first".to_string()],
                },
            ],
        };

        let inputs = HashMap::new();
        let record = runtime.execute_pipeline(&config, inputs, None).await.unwrap();

        assert_eq!(record.metadata.pipeline_name, "test-pipeline");
        assert_eq!(record.metadata.ancestor_run_id, None);
        assert_eq!(record.nodes.len(), 2);
        assert!(record.output_snapshot.as_map().unwrap().contains_key("second"));
    }

    #[tokio::test]
    async fn missing_engine_schedules_failed_node_and_partial_lineage() {
        let store = Arc::new(InMemoryStore::new());
        let mut runtime = Runtime::new(store.clone());

        // register an unrelated engine so the requested one is missing
        runtime.register_engine("uiua", Arc::new(DummyEngineSimple {}));

        let config = PipelineConfig {
            name: "missing-engine-test".to_string(),
            nodes: vec![NodeConfig {
                name: "only".to_string(),
                engine: "rust".to_string(),
                code: "x".to_string(),
                inputs: vec![],
            }],
        };

        let record = runtime.execute_pipeline(&config, HashMap::new(), None).await.unwrap();

        assert_eq!(record.metadata.status, RunStatus::Failed);
        assert!(record.metadata.error_message.is_some());
        let msg = record.metadata.error_message.unwrap();
        assert!(msg.contains("is not registered"));

        assert_eq!(record.nodes.len(), 1);
        assert_eq!(record.nodes[0].status, NodeStatus::Failed);
        assert!(record.nodes[0].error.is_some());
    }

    #[tokio::test]
    async fn wasm_engine_executes_run_function() {
        let store = Arc::new(InMemoryStore::new());
        let mut runtime = Runtime::new(store.clone());

        runtime.register_engine("wasm", Arc::new(GuestModule::new()));

        // WAT module that exports `run` which returns input + 1
        let wat = r#"(module
            (func $run (export "run") (param i32) (result i32)
                local.get 0
                i32.const 1
                i32.add)
        )"#;

        let config = PipelineConfig {
            name: "wasm-test".to_string(),
            nodes: vec![NodeConfig {
                name: "inc".to_string(),
                engine: "wasm".to_string(),
                code: wat.to_string(),
                inputs: vec!["x".to_string()],
            }],
        };

        let mut inputs = HashMap::new();
        inputs.insert("x".to_string(), Value::Int(41));

        let record = runtime.execute_pipeline(&config, inputs, None).await.unwrap();

        assert_eq!(record.metadata.status, RunStatus::Completed);
        // output snapshot should contain the node 'inc' with value 42
        let map = record.output_snapshot.as_map().unwrap();
        assert!(map.contains_key("inc"));
        assert_eq!(map.get("inc").unwrap(), &Value::Int(42));
    }

    #[tokio::test]
    async fn wasm_engine_memory_roundtrip_string() {
        let store = Arc::new(InMemoryStore::new());
        let mut runtime = Runtime::new(store.clone());

        runtime.register_engine("wasm", Arc::new(GuestModule::new()));

        // WAT module with memory, alloc, and run(ptr,len)->(ptr,len) that returns same bytes
        let wat = r#"(module
            (memory (export "memory") 1)
            (global $heap (mut i32) (i32.const 1024))
            (func (export "alloc") (param $n i32) (result i32)
                global.get $heap
                global.get $heap
                local.get $n
                i32.add
                global.set $heap)
            (func (export "run") (param $ptr i32) (param $len i32) (result i32 i32)
                local.get 0
                local.get 1)
        )"#;

        let config = PipelineConfig {
            name: "wasm-mem-test".to_string(),
            nodes: vec![NodeConfig {
                name: "echo".to_string(),
                engine: "wasm".to_string(),
                code: wat.to_string(),
                inputs: vec!["in".to_string()],
            }],
        };

        let mut inputs = HashMap::new();
        inputs.insert("in".to_string(), Value::String("hello".to_string()));

        let record = runtime.execute_pipeline(&config, inputs, None).await.unwrap();
        assert_eq!(record.metadata.status, RunStatus::Completed);
        let map = record.output_snapshot.as_map().unwrap();
        assert_eq!(map.get("echo").unwrap(), &Value::String("hello".to_string()));
    }

    #[test]
    fn wasm_engine_maps_fuel_exhaustion_error() {
        let err = anyhow!("all fuel consumed");
        let mapped = GuestModule::map_wasm_execution_error(err);
        assert_eq!(mapped.to_string(), "WASM Execution Timeout / Fuel Exhausted");
    }

    #[tokio::test]
    async fn wasm_engine_memory_roundtrip_string_with_free() {
        let store = Arc::new(InMemoryStore::new());
        let mut runtime = Runtime::new(store.clone());

        runtime.register_engine("wasm", Arc::new(GuestModule::new()));

        let wat = r#"(module
            (memory (export "memory") 1)
            (global $heap (mut i32) (i32.const 1024))
            (func (export "alloc") (param $n i32) (result i32)
                global.get $heap
                global.get $heap
                local.get $n
                i32.add
                global.set $heap)
            (func (export "free") (param $ptr i32) (param $len i32)
                local.get 0
                drop
                local.get 1
                drop)
            (func (export "run") (param $ptr i32) (param $len i32) (result i32 i32)
                local.get 0
                local.get 1)
        )"#;

        let config = PipelineConfig {
            name: "wasm-mem-free-test".to_string(),
            nodes: vec![NodeConfig {
                name: "echo_free".to_string(),
                engine: "wasm".to_string(),
                code: wat.to_string(),
                inputs: vec!["in".to_string()],
            }],
        };

        let mut inputs = HashMap::new();
        inputs.insert("in".to_string(), Value::String("hello".to_string()));

        let record = runtime.execute_pipeline(&config, inputs, None).await.unwrap();
        assert_eq!(record.metadata.status, RunStatus::Completed);
        let map = record.output_snapshot.as_map().unwrap();
        assert_eq!(map.get("echo_free").unwrap(), &Value::String("hello".to_string()));
    }

    #[tokio::test]
    async fn register_hyperedge_and_query_relations() {
        let store = Arc::new(InMemoryStore::new());
        let runtime = Runtime::new(store.clone());

        runtime
            .register_hyperedge(
                "dependency",
                "edge-1",
                &[
                    Value::String("vertex-A".to_string()),
                    Value::Int(42),
                    Value::Bool(true),
                ],
            )
            .await
            .unwrap();

        let relations = runtime.host_query_vertex_relations("vertex-A").await.unwrap();
        let rows = relations.as_table().unwrap();
        assert_eq!(rows.len(), 1);
        let row = rows[0].as_map().unwrap();
        assert_eq!(row.get("edge_type"), Some(&Value::String("dependency".to_string())));
        assert_eq!(row.get("edge_id"), Some(&Value::String("edge-1".to_string())));
        assert_eq!(row.get("target_vertex"), Some(&Value::String("vertex-A".to_string())));
    }
}
