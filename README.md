# Manifold — Local computational pipeline environment

Manifold is a lightweight Rust pipeline runtime with in-memory store and sandboxed WASM plugin execution.

## Quickstart

1. Create an inputs file, e.g. `inputs.json`:

```json
{
  "value": 41
}
```

Manifold
========

Manifold is an ultra-fast, cross-language binary co-processor runtime for accelerating scripting languages using sandboxed WebAssembly (WASI/wasmtime). It provides a zero-C-dependency guest-host PDK, an ergonomic CLI, and simple stdin/stdout memory-pipe integration so Python, Go, Node.js, PHP, and other languages can offload hot-code to compact, portable WASM plugins.

Core concept
------------

- **Binary co-processor**: Run a compiled WASM plugin as a fast, isolated worker that transforms or analyzes byte payloads.
- **Cross-language pipes**: Communicate with `manifold` via standard input and output pipes — no temporary files, no language-specific bindings required.
- **Safe sandbox**: Uses Wasmtime with configurable per-run timeouts, fuel, and memory caps to protect the host process.
- **PDK for guests**: A tiny guest-side SDK provides `alloc`/`free`, logging, and store/asset bindings for easy plugin development.

Quick examples
--------------

Run a single plugin reading raw bytes from stdin and writing raw bytes to stdout:

```bash
cat input.raw | manifold run --plugin transformer.wasm - --output - > output.raw
```

Run with resource caps (2s timeout, 64MB max linear memory):

```bash
cat input.raw | manifold run --plugin transformer.wasm - --timeout 2000 --max-memory 64 > output.raw
```

If you omit `--input` or provide `-`, the CLI reads raw bytes from stdin. If you omit `--output` or provide `-`, output is written to stdout — this enables composition with pipes from any language runtime.

Validation — `manifold check`
-----------------------------

`manifold check [PATH]` performs advisory validation of Wasm plugins and pipeline configs. It reports soft warnings for imports or API usage that may be non-compliant with the PDK but does not block execution — checks are advisory by design to keep developer workflows fast.

Cross-language integration
--------------------------

Check the `examples/` directory for simple, production-ready integration scripts demonstrating zero-file memory piping:

- `examples/demo.py` — Python example using `subprocess.run` with `stdin=subprocess.PIPE` and `stdout=subprocess.PIPE`.
- `examples/test_pipe.go` — Go example using `os/exec`, `StdinPipe`, and `StdoutPipe`.
- `examples/test_pipe.js` — Node.js example using `child_process.spawn` and streaming buffers.

All examples write a small byte payload to the CLI's stdin, close stdin (EOF), then read the plugin's output from stdout. Use these as templates to integrate Manifold into existing applications.

Building & running
------------------

Build the CLI and example transformer plugin (WASI target):

```bash
cargo build --release -p manifold-cli
pushd examples/transformer && cargo build --release --target wasm32-wasi && popd
```

Run tests & checks locally:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Contributing
------------

Please keep changes small and focused. The runtime intentionally enforces per-run resource caps supplied via the CLI flags to keep integrations robust across languages.

Files of interest
-----------------

- `crates/manifold-cli/src/main.rs` — CLI entrypoint, piping and resource flags.
- `crates/manifold-runtime/src/lib.rs` — Wasmtime engine glue and execution plumbing.
- `crates/manifold-pdk/src/lib.rs` — Guest-side PDK (alloc/free, run helper, logging, store helpers).
- `examples/` — Integration examples for Python, Go, Node.js and a transformer plugin.

License & authors
-----------------

See `Cargo.toml` for package info. If you plan to publish, run the test-suite and lints in CI before tagging a release.

Enjoy — Manifold teams a lightweight host with fast, portable Wasm plugins to speed up your scripts across languages.

3. Use `engine: wasm` nodes to execute sandboxed WASM modules.

## Notes

- Runtime state is in-memory only; there is no RocksDB dependency.
- WASM guest modules can use the PDK host interface for logging, host store access, and asset requests.
- Raw bytes are supported via the `alloc` / `run` convention.
