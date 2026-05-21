#![deny(warnings)]
#![deny(missing_docs)]

//! Example Manifold guest plugin that performs a deterministic byte-level transformation.
//!
//! This plugin accepts a raw byte buffer exposed through the standard `run(ptr, len)`
//! entrypoint and returns a new allocated output buffer to the host.

use manifold_pdk::{init, log_error, run_with_bytes};

/// Standard wasm entrypoint used by the host runtime.
#[no_mangle]
pub fn run(input_ptr: *mut u8, input_len: usize) -> (i32, i32) {
    init();

    match run_with_bytes(input_ptr, input_len, |input| {
        let output = input
            .iter()
            .map(|byte| byte.wrapping_add(1))
            .collect::<Vec<u8>>();
        Ok(output)
    }) {
        Ok((ptr, len)) => (ptr as i32, len as i32),
        Err(err) => {
            log_error(&format!("transformer plugin failed: {}", err));
            (0, 0)
        }
    }
}
