//! Manifold guest SDK and plugin runtime helpers.
//!
//! This crate exposes guest-side logging, persistence, and asset request bindings
//! for Wasmtime-based plugins.

#![deny(warnings)]
#![deny(missing_docs)]

use std::{fmt, panic, sync::atomic::{AtomicBool, Ordering}};

static PDK_INITED: AtomicBool = AtomicBool::new(false);

extern "C" {
    fn host_log(ptr: u32, len: u32);
    fn store_set(key_ptr: u32, key_len: u32, value_ptr: u32, value_len: u32) -> i32;
    fn store_get(key_ptr: u32, key_len: u32, dest_ptr: u32, dest_max_len: u32) -> i32;
    fn host_asset_request(path_ptr: u32, path_len: u32, dest_ptr: u32, dest_max_len: u32) -> u32;
}

/// The PDK internal error type.
#[derive(Debug)]
pub enum PdkError {
    /// Serialization failed while preparing data for host storage.
    Serialization(String),
    /// Deserialization failed while reading data from the host store.
    Deserialization(String),
    /// The host store is not configured for this guest.
    StoreNotConfigured,
    /// The requested key is not present in the store.
    StoreKeyNotFound,
    /// The host store reported an I/O or host-side error.
    StoreHostError(String),
    /// The host store value was larger than the provided buffer.
    StoreBufferTooSmall(usize),
    /// Logging could not be delivered to the host.
    LogHostError(String),
}

impl fmt::Display for PdkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(err) => write!(f, "serialization error: {}", err),
            Self::Deserialization(err) => write!(f, "deserialization error: {}", err),
            Self::StoreNotConfigured => write!(f, "store is not configured for this guest"),
            Self::StoreKeyNotFound => write!(f, "store key not found"),
            Self::StoreHostError(err) => write!(f, "store host error: {}", err),
            Self::StoreBufferTooSmall(size) => write!(f, "store buffer too small; required {} bytes", size),
            Self::LogHostError(err) => write!(f, "log host error: {}", err),
        }
    }
}

impl std::error::Error for PdkError {}

impl From<serde_json::Error> for PdkError {
    fn from(err: serde_json::Error) -> Self {
        PdkError::Serialization(err.to_string())
    }
}

fn host_log_message(level: &str, message: &str) -> Result<(), PdkError> {
    let payload = format!("[GUEST][{}] {}", level, message);
    unsafe {
        host_log(payload.as_ptr() as u32, payload.len() as u32);
    }
    Ok(())
}

/// Initialize the guest-side PDK runtime.
///
/// This installs a panic hook that sends a structured error message to the host.
pub fn init() {
    if PDK_INITED.swap(true, Ordering::SeqCst) {
        return;
    }

    panic::set_hook(Box::new(|info| {
        let payload = if let Some(s) = info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "unknown panic payload".to_string()
        };
        let location = if let Some(location) = info.location() {
            format!("{}:{}", location.file(), location.line())
        } else {
            "unknown location".to_string()
        };
        let _ = host_log_message("ERROR", &format!("panic at {}: {}", location, payload));
    }));
}

/// Log an `INFO` message from the guest.
pub fn log_info(message: &str) {
    let _ = host_log_message("INFO", message);
}

/// Log a `WARN` message from the guest.
pub fn log_warn(message: &str) {
    let _ = host_log_message("WARN", message);
}

/// Log an `ERROR` message from the guest.
pub fn log_error(message: &str) {
    let _ = host_log_message("ERROR", message);
}

/// Request an asset from the host using the standard `env::asset_request` interface.
///
/// Returns the number of bytes written into the supplied buffer.
pub fn asset_request(path: &str, buffer: &mut [u8]) -> usize {
    let path_bytes = path.as_bytes();
    unsafe {
        host_asset_request(
            path_bytes.as_ptr() as u32,
            path_bytes.len() as u32,
            buffer.as_mut_ptr() as u32,
            buffer.len() as u32,
        ) as usize
    }
}

/// Guest persistence helpers backed by host storage.
pub mod store {
    use super::{PdkError, store_get, store_set};
    use serde::{de::DeserializeOwned, Serialize};

    fn errno_message(code: i32) -> PdkError {
        match code {
            -1 => PdkError::StoreNotConfigured,
            -2 => PdkError::StoreKeyNotFound,
            -3 => PdkError::StoreHostError("invalid guest memory reference".to_string()),
            code if code <= -10 => PdkError::StoreBufferTooSmall((-code - 10) as usize),
            code => PdkError::StoreHostError(format!("unexpected host error code {}", code)),
        }
    }

    /// Store a serializable value under the given key.
    pub fn set<T: Serialize>(key: &str, value: &T) -> Result<(), PdkError> {
        let serialized = serde_json::to_vec(value).map_err(|err| PdkError::Serialization(err.to_string()))?;
        let key_bytes = key.as_bytes();

        let result = unsafe {
            store_set(
                key_bytes.as_ptr() as u32,
                key_bytes.len() as u32,
                serialized.as_ptr() as u32,
                serialized.len() as u32,
            )
        };

        match result {
            0 => Ok(()),
            error_code => Err(errno_message(error_code)),
        }
    }

    /// Retrieve a value from the host store.
    pub fn get<T: DeserializeOwned>(key: &str) -> Result<Option<T>, PdkError> {
        let key_bytes = key.as_bytes();
        let mut buffer_size = 4096;

        loop {
            let mut buffer = vec![0u8; buffer_size];
            let result = unsafe {
                store_get(
                    key_bytes.as_ptr() as u32,
                    key_bytes.len() as u32,
                    buffer.as_mut_ptr() as u32,
                    buffer.len() as u32,
                )
            };

            match result {
                -1 => return Ok(None),
                0 => {
                    return Err(PdkError::StoreHostError(
                        "store_get returned zero length for a non-empty request".to_string(),
                    ))
                }
                code if code < 0 => {
                    if code <= -10 {
                        buffer_size = (-code - 10) as usize;
                        continue;
                    }
                    return Err(errno_message(code));
                }
                len => {
                    let len = len as usize;
                    if len > buffer.len() {
                        return Err(PdkError::StoreBufferTooSmall(len));
                    }
                    let value = serde_json::from_slice(&buffer[..len])
                        .map_err(|err| PdkError::Deserialization(err.to_string()))?;
                    return Ok(Some(value));
                }
            }
        }
    }
}

/// Log an `INFO` message from a guest plugin with `format!`-style arguments.
#[macro_export]
macro_rules! log_info {
    ($($arg:tt)+) => {{
        $crate::log_info(&format!($($arg)+));
    }};
}

/// Log a `WARN` message from a guest plugin with `format!`-style arguments.
#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)+) => {{
        $crate::log_warn(&format!($($arg)+));
    }};
}

/// Log an `ERROR` message from a guest plugin with `format!`-style arguments.
#[macro_export]
macro_rules! log_error {
    ($($arg:tt)+) => {{
        $crate::log_error(&format!($($arg)+));
    }};
}

/// Convenient helper for guest plugins to call `env::asset_request`.
#[macro_export]
macro_rules! asset_request {
    ($path:expr, $buffer:expr) => {{
        $crate::asset_request($path, $buffer)
    }};
}

#[cfg(test)]
pub use mock_host::MockHostBuilder;

#[cfg(test)]
mod mock_host {
    use std::{collections::HashMap, sync::Mutex};

    lazy_static::lazy_static! {
        static ref MOCK_HOST_STATE: Mutex<Option<MockHostState>> = Mutex::new(None);
    }

    struct MockHostState {
        logs: Vec<String>,
        store: HashMap<Vec<u8>, Vec<u8>>,
        assets: HashMap<Vec<u8>, Vec<u8>>,
    }

    /// Builder for a mock host that can satisfy guest host calls during unit tests.
    pub struct MockHostBuilder {
        state: MockHostState,
    }

    impl MockHostBuilder {
        /// Create an empty mock host.
        pub fn new() -> Self {
            Self {
                state: MockHostState {
                    logs: Vec::new(),
                    store: HashMap::new(),
                    assets: HashMap::new(),
                },
            }
        }

        /// Register a mock asset response for the given path.
        pub fn with_asset(mut self, path: &str, bytes: &[u8]) -> Self {
            self.state.assets.insert(path.as_bytes().to_vec(), bytes.to_vec());
            self
        }

        /// Seed the mock store with a binary value.
        pub fn with_store_entry(mut self, key: &str, bytes: &[u8]) -> Self {
            self.state.store.insert(key.as_bytes().to_vec(), bytes.to_vec());
            self
        }

        /// Activate the mock host for the current test.
        pub fn activate(self) -> MockHostGuard {
            let mut guard = MOCK_HOST_STATE.lock().unwrap();
            *guard = Some(self.state);
            MockHostGuard
        }
    }

    impl Default for MockHostBuilder {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MockHostBuilder {
        /// Read the captured guest logs.
        pub fn logs() -> Vec<String> {
            MOCK_HOST_STATE
                .lock()
                .unwrap()
                .as_ref()
                .map(|state| state.logs.clone())
                .unwrap_or_default()
        }
    }

    /// Guard that resets the mock host when dropped.
    pub struct MockHostGuard;

    impl Drop for MockHostGuard {
        fn drop(&mut self) {
            let mut guard = MOCK_HOST_STATE.lock().unwrap();
            *guard = None;
        }
    }

    fn current_state() -> std::sync::MutexGuard<'static, Option<MockHostState>> {
        MOCK_HOST_STATE.lock().unwrap()
    }

    #[no_mangle]
    pub extern "C" fn host_log(ptr: u32, len: u32) {
        let data = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
        let message = String::from_utf8_lossy(data).to_string();
        if let Some(state) = current_state().as_mut() {
            state.logs.push(message);
        }
    }

    #[no_mangle]
    pub extern "C" fn store_set(key_ptr: u32, key_len: u32, value_ptr: u32, value_len: u32) -> i32 {
        let key = unsafe { std::slice::from_raw_parts(key_ptr as *const u8, key_len as usize) };
        let value = unsafe { std::slice::from_raw_parts(value_ptr as *const u8, value_len as usize) };
        if let Some(state) = current_state().as_mut() {
            state.store.insert(key.to_vec(), value.to_vec());
            0
        } else {
            -1
        }
    }

    #[no_mangle]
    pub extern "C" fn store_get(key_ptr: u32, key_len: u32, dest_ptr: u32, dest_max_len: u32) -> i32 {
        let key = unsafe { std::slice::from_raw_parts(key_ptr as *const u8, key_len as usize) };
        let dest = unsafe { std::slice::from_raw_parts_mut(dest_ptr as *mut u8, dest_max_len as usize) };
        if let Some(state) = current_state().as_ref() {
            match state.store.get(key) {
                Some(value) => {
                    if value.len() > dest.len() {
                        return -10 - (value.len() as i32);
                    }
                    dest[..value.len()].copy_from_slice(value);
                    value.len() as i32
                }
                None => -2,
            }
        } else {
            -1
        }
    }

    #[no_mangle]
    pub extern "C" fn asset_request(path_ptr: u32, path_len: u32, dest_ptr: u32, dest_max_len: u32) -> u32 {
        let key = unsafe { std::slice::from_raw_parts(path_ptr as *const u8, path_len as usize) };
        let dest = unsafe { std::slice::from_raw_parts_mut(dest_ptr as *mut u8, dest_max_len as usize) };
        if let Some(state) = current_state().as_ref() {
            if let Some(asset) = state.assets.get(key) {
                let written = std::cmp::min(asset.len(), dest.len());
                dest[..written].copy_from_slice(&asset[..written]);
                return written as u32;
            }
        }
        0
    }
}
