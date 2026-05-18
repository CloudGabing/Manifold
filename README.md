# Manifold — Local computational pipeline environment

## Wasm Engine Quickstart (wasmtime)

- Feature: Use the `wasm` engine name in your pipeline to run sandboxed WebAssembly code authored in WAT (.wat) or compiled wasm bytes.

Minimal example `.wat` function (exported `run(i32) -> i32`):

```wat
(module
  (func $run (export "run") (param i32) (result i32)
    local.get 0
    i32.const 1
    i32.add)
)
```

Pipeline snippet (see `examples/pipeline_wasm.yaml`):

- Add a node with `engine: wasm` and place the WAT text under `code:`.
- When the node has a single input, it is passed to `run` as a 32-bit integer.
- The returned i32 is mapped to a `Value::Int` and becomes the node's output.

Running the example:

1. Create an inputs JSON file, e.g. `inputs.json`:

```json
{
  "value": 41
}
```

2. Run the pipeline with the CLI:

```bash
manifold run --config examples/pipeline_wasm.yaml --input inputs.json
```

Memory-based interop (string/buffer):

- The `WasmEngine` supports an `alloc(len)->ptr` and `run(ptr,len)->(ptr_out,len_out)` convention for passing arbitrary bytes (strings or JSON) into a WASM module and receiving a byte slice back. The module must export a linear `memory` and follow the `alloc`/`run` convention.

Notes & limitations:
- Prototype stage: supported signatures are numeric `run(i32)->i32` and the memory `alloc`+`run` convention described above.
- Future work: richer marshalling, host functions, WASI integration, and richer examples.
