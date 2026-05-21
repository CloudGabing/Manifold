// examples/test_pipe.js
//
// Node.js integration test: spawn the Manifold CLI, stream bytes to stdin,
// capture stdout, and print results.
//
// Usage:
//   node examples/test_pipe.js
// Ensure `manifold` or `target/release/manifold-cli` is built and `transformer.wasm`
// exists at `target/wasm32-wasi/release/transformer.wasm`.

const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

function findExe() {
  const local = path.join('target', 'release', 'manifold-cli');
  if (fs.existsSync(local)) return local;
  return 'manifold';
}

const exe = findExe();
const plugin = path.join('target', 'wasm32-wasi', 'release', 'transformer.wasm');

const child = spawn(exe, ['run', '--plugin', plugin, '--timeout', '2000', '--max-memory', '64']);

const input = Buffer.from('Hello from Node.js via pipes!');

let stdoutChunks = [];
let stderrChunks = [];

child.stdin.write(input);
child.stdin.end(); // signal EOF

child.stdout.on('data', (chunk) => stdoutChunks.push(chunk));
child.stderr.on('data', (chunk) => stderrChunks.push(chunk));

child.on('close', (code) => {
  const out = Buffer.concat(stdoutChunks);
  const err = Buffer.concat(stderrChunks).toString('utf8');
  if (err.length) console.error('[manifold stderr]', err);
  console.log('Original:', input.toString());
  console.log('Transformed:', out.toString());
  console.log('Exit code:', code);
});
