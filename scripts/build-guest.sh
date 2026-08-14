#!/usr/bin/env bash
# Rebuild the cordis-wasm test fixture from examples/wasm-guest.
#
# Verified toolchain (2026-08):
#   - stable rustc (wasm32-unknown-unknown target; no wasip3 target exists
#     and none is needed — the `plugin` world imports nothing from WASI)
#   - wit-bindgen 0.51
#   - wasm-tools 1.256.0
set -euo pipefail
cd "$(dirname "$0")/.."

rustup target add wasm32-unknown-unknown

(cd examples/wasm-guest &&
  cargo build --release --target wasm32-unknown-unknown)

WASM_TOOLS="${WASM_TOOLS:-wasm-tools}"
mkdir -p crates/cordis-wasm/tests/fixtures
"$WASM_TOOLS" component new \
  examples/wasm-guest/target/wasm32-unknown-unknown/release/cordis_guest_example.wasm \
  -o crates/cordis-wasm/tests/fixtures/hello.wasm

echo "fixture written: crates/cordis-wasm/tests/fixtures/hello.wasm"
