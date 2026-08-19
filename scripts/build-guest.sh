#!/usr/bin/env bash
# Rebuild the nucleus-wasm test fixture from examples/wasm-guest.
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
WASM=examples/wasm-guest/target/wasm32-unknown-unknown/release/nucleus_guest_example.wasm
mkdir -p crates/nucleus-wasm/tests/fixtures

# wit-bindgen 0.51 embeds the component type in an encoding that
# wasm-tools 1.256.0's `component new` cannot decode directly. We strip
# the embedded section and re-embed it via `wasm-tools component embed`,
# which produces the encoding `component new` understands.
"$WASM_TOOLS" strip -d component-type "$WASM" -o /tmp/nucleus-guest-stripped.wasm
"$WASM_TOOLS" component embed --world plugin wit/ /tmp/nucleus-guest-stripped.wasm -o /tmp/nucleus-guest-embedded.wasm
"$WASM_TOOLS" component new /tmp/nucleus-guest-embedded.wasm \
  -o crates/nucleus-wasm/tests/fixtures/hello.wasm

echo "fixture written: crates/nucleus-wasm/tests/fixtures/hello.wasm"
