#!/usr/bin/env python3
"""
examples/demo.py

Demonstrates invoking the Manifold CLI as a high-speed binary co-processor
using stdin/stdout pipes (no temporary files on disk).

Behavior:
- Prepares a sample byte payload.
- Spawns the `manifold` CLI (prefers `./target/release/manifold-cli` if present).
- Calls: run --plugin target/wasm32-wasi/release/transformer.wasm --timeout 2000 --max-memory 64
- Writes bytes to the process's stdin, closes it, and reads transformed bytes from stdout.
- Prints original and transformed outputs.

This example is intentionally small and dependency-free (Python 3.7+).
"""

import os
import sys
import subprocess


def find_executable() -> str:
    """Return the preferred manifold executable path.

    Prefer a local release build at `target/release/manifold-cli` if it exists,
    otherwise fall back to `manifold` (expects it to be on PATH).
    """
    local = os.path.join("target", "release", "manifold-cli")
    if os.path.exists(local) and os.access(local, os.X_OK):
        return local
    return "manifold"


def main() -> None:
    # Sample payload: could be binary data produced in-memory by a Python app.
    input_bytes = b"Hello from Python via memory pipes!"

    manifold_exe = find_executable()

    # The plugin path is relative; adjust as needed for your workspace layout.
    plugin_path = os.path.join("target", "wasm32-wasi", "release", "transformer.wasm")

    # Command: no temporary files, everything via stdin/stdout pipes.
    cmd = [
        manifold_exe,
        "run",
        "--plugin",
        plugin_path,
        "--timeout",
        "2000",
        "--max-memory",
        "64",
    ]

    # Print to stderr for debugging without mixing with stdout payload.
    print("[demo] running:", " ".join(cmd), file=sys.stderr)

    try:
        # Spawn process with pipes for stdin/stdout. We also capture stderr to show runtime logs.
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        # Write input bytes and close stdin. communicate() handles closing and reading stdout.
        out_bytes, err_bytes = proc.communicate(input=input_bytes)
        return_code = proc.returncode

    except FileNotFoundError:
        print(f"[error] executable not found: {manifold_exe}\nPlease build or install the CLI.", file=sys.stderr)
        sys.exit(2)
    except Exception as exc:
        print(f"[error] execution failed: {exc}", file=sys.stderr)
        sys.exit(3)

    # Show any runtime logs from manifold on stderr (helpful for debugging plugin failures).
    if err_bytes:
        try:
            sys.stderr.write("[manifold stderr] " + err_bytes.decode("utf-8", errors="replace") + "\n")
        except Exception:
            sys.stderr.write("[manifold stderr] <binary data>\n")

    print(f"[demo] manifold exit code: {return_code}")

    # Display original and transformed payloads. Keep them as raw bytes to avoid accidental decoding errors.
    print("Original input (bytes):", input_bytes)
    print("Transformed output (bytes):", out_bytes)

    # Attempt to decode as UTF-8 for human-friendly display when plugin performs text transforms.
    try:
        print("Transformed output (utf-8):", out_bytes.decode("utf-8"))
    except Exception:
        print("Transformed output: <non-UTF8 binary data>")


if __name__ == "__main__":
    main()
