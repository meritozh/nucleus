# Test fixtures

`hello.wasm` — a real nucleus guest component, compiled from
`examples/wasm-guest`. Regenerate with `../../scripts/build-guest.sh` (or
`WASM_TOOLS=/path/to/wasm-tools ../../scripts/build-guest.sh`).

Provenance: built 2026-08 with rustc stable (wasm32-unknown-unknown),
wit-bindgen 0.51, wasm-tools 1.256.0. The guest's world is `wit/plugin.wit`;
its source is the contract-correct reference for writing new guests.

If host/guest contract drift ever makes instantiation fail here, the
failing test is doing its job: bump the WIT package versions and update
both sides.
