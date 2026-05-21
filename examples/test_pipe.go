// examples/test_pipe.go
//
// Simple integration test: spawn the Manifold CLI, write bytes to stdin,
// read transformed bytes from stdout, and print results.
//
// Usage:
//   go run examples/test_pipe.go
// Ensure `manifold` or `target/release/manifold-cli` is built and `transformer.wasm`
// exists at `target/wasm32-wasi/release/transformer.wasm`.

package main

import (
    "bytes"
    "fmt"
    "io"
    "os"
    "os/exec"
    "path/filepath"
)

func findExe() string {
    local := filepath.Join("target", "release", "manifold-cli")
    if _, err := os.Stat(local); err == nil {
        return local
    }
    return "manifold"
}

func main() {
    exe := findExe()
    plugin := filepath.Join("target", "wasm32-wasi", "release", "transformer.wasm")

    cmd := exec.Command(exe, "run", "--plugin", plugin, "--timeout", "2000", "--max-memory", "64")

    stdin, err := cmd.StdinPipe()
    if err != nil {
        fmt.Fprintf(os.Stderr, "failed to get stdin pipe: %v\n", err)
        os.Exit(2)
    }

    stdout, err := cmd.StdoutPipe()
    if err != nil {
        fmt.Fprintf(os.Stderr, "failed to get stdout pipe: %v\n", err)
        os.Exit(2)
    }

    if err := cmd.Start(); err != nil {
        fmt.Fprintf(os.Stderr, "failed to start command: %v\n", err)
        os.Exit(2)
    }

    input := []byte("Hello from Go via pipes!")

    // Write and close stdin to ensure the CLI sees EOF.
    if _, err := stdin.Write(input); err != nil {
        fmt.Fprintf(os.Stderr, "failed to write to stdin: %v\n", err)
    }
    stdin.Close()

    // Read all stdout until EOF.
    outBuf := new(bytes.Buffer)
    if _, err := io.Copy(outBuf, stdout); err != nil {
        fmt.Fprintf(os.Stderr, "failed to read stdout: %v\n", err)
    }

    if err := cmd.Wait(); err != nil {
        fmt.Fprintf(os.Stderr, "command failed: %v\n", err)
    }

    fmt.Printf("Original: %s\n", string(input))
    fmt.Printf("Transformed: %s\n", outBuf.String())
}
