# nucleus — cordis-rs

[![status](https://img.shields.io/badge/status-P1%2FP2%2FP3%20implemented-green)]()
[![tests](https://img.shields.io/badge/tests-27%20passed-green)]()

Rust implementation of the **cordis** paradigm — the "spatiotemporal
composability" plugin framework vendored inside
[deepseek-harness](https://github.com/deepseek-ai/deepseek-harness) — with
runtime dynamic pluggability via WASI 0.3 WebAssembly components.

Two orthogonal dimensions, one runtime:

- **Temporal composability** — every registration through `Context` is a
  reversible *effect*. Unloading a fiber unwinds services, listeners, child
  plugins and external resources in reverse registration order (LIFO), and
  a failed `apply` still unwinds its partial effects.
- **Spatial composability** — plugins declare service *coeffects*
  (`inject`); fibers stay `Pending` until providers exist, dependents are
  unloaded when a service disappears, and cleanly restart against a new
  implementation when one mounts. Load order is emergent, never manual.

```
┌─────────────────────────────────────────────────────┐
│  应用（agent harness 等）                             │
├─────────────────────────────────────────────────────┤
│  cordis-loader   cordis.yml 按 id diff 协调、          │
│                  disabled/groups/isolate、HMR watch   │
├─────────────────────────────────────────────────────┤
│  cordis-core     Fiber 状态机、effect 回滚栈、          │
│                  (scope,name) 服务注册表、事件分发、     │
│                  依赖级联重载          （纯安全 Rust）   │
├─────────────────────────────────────────────────────┤
│  PluginSource trait  ┌─ StaticSource（编译期注册表）    │
│                      └─ WasmSource（wasmtime 47 /     │
│                         WASI 0.3 原生 async）          │
└─────────────────────────────────────────────────────┘
```

**Full design note: [docs/design.md](docs/design.md)** — research summary of
dsh/cordis, concept mapping, key decisions, risks.

## Crates

| Crate | What it is |
|---|---|
| `cordis-core` | fibers, revertible effects, typed services, 5 event dispatch modes (emit / parallel / serial / waterfall / +dynamic), scopes, cascade reload |
| `cordis-loader` | row config reconciliation by `id`, `disabled`, nested `group` + `isolate` scopes, HMR file watch |
| `cordis-wasm` | `PluginSource` over `.wasm` components: guest init/dispose-effect lifecycle, event streams into guests, `call-service` bridge, fuel + memory limits |
| `wit/` | the host↔guest contract (two packages: `cordis:host` + `cordis:plugin`) |

## Quick start

```rust,ignore
use cordis_core::{App, Context, Plugin, MountSpec};
use std::sync::Arc;

struct Greeter;
struct GreeterSvc(Context);

#[async_trait::async_trait]
impl Plugin for Greeter {
    fn name(&self) -> &str { "greeter" }
    async fn apply(&self, ctx: Context) -> cordis_core::Result<()> {
        ctx.provide::<dyn GreeterService>("greeter", Arc::new(GreeterSvc(ctx.clone()))).await
    }
}

let app = App::new().with_static(
    cordis_core::static_source! { "greeter" => |_cfg| Ok(Arc::new(Greeter)) },
);
let fiber = app.mount(MountSpec::new("greeter")).await?;
fiber.dispose().await?;           // unwinds everything, LIFO
```

Run the end-to-end demo (pending → activation → cascade reload → provider
swap → teardown):

```console
$ cargo run -p cordis-core --example composition
[app] consumer state: pending
[consumer] mounted, service says: hello from v1, world
[app] after provider mounts, consumer state: active
[consumer] external resource released
[app] provider removed, consumer back to: pending
...
```

HMR demo (edit the printed `cordis.yml` live):

```console
$ cargo run -p cordis-loader --example hmr
initial: mounted=1 remounted=0 unmounted=0 unchanged=0 failed=0
edited:  mounted=1 remounted=1 unmounted=0 unchanged=1 failed=0
...
```

## Wasm plugins (runtime code loading)

Guests implement the world in `wit/plugin.wit` (`init` = apply,
`dispose-effect` = disposer callback, `register-effect` / `subscribe` /
`emit` / `call-service` host imports). Host side:

```rust,ignore
let source = cordis_wasm::WasmSource::new("./plugins"); // <name>.wasm
let app = App::new()
    .with_source("wasm", std::sync::Arc::new(source));
let fiber = app.mount(MountSpec::new("hello").with_source("wasm")).await?;
```

Guest build — **this exact path is what the test fixture is built with**
(there is no `wasm32-wasip3` target on any toolchain: WASI 0.3 is a
package-level change. The `plugin` world imports nothing from WASI, so the
plain `wasm32-unknown-unknown` target on **stable** Rust suffices):

```console
./scripts/build-guest.sh        # or, by hand:
rustup target add wasm32-unknown-unknown
(cd examples/wasm-guest && cargo build --release --target wasm32-unknown-unknown)
wasm-tools component new \
  examples/wasm-guest/target/wasm32-unknown-unknown/release/cordis_guest_example.wasm \
  -o crates/cordis-wasm/tests/fixtures/hello.wasm
```

Verified versions: wit-bindgen 0.51 (needs `generate_all` in
`wit_bindgen::generate!`, and maps the contract's unit-error `result` to
`Result<(), ()>` on the guest side), wasm-tools 1.256.0.

Notes: guests are bounded by fuel (default 10M/call, re-armed before each
guest entry) and a 64 MiB memory cap; events are fanned out into guest
`stream<event>` subscriptions with drop-on-slow semantics (the host never
blocks on a slow guest); unmounting runs guest `dispose-effect` callbacks
in LIFO order, then drops the Store (cancelling in-flight guest calls) —
all verified against a real guest by the tests above. Pinned wasmtime
**47** — WASI 0.3 stable support and the future-drop cancellation fix
(GHSA-xjhv-v822-pf94, fixed in 42+).

## Tests

```console
$ cargo test --workspace   # 27 tests, all green
```

Wasm (against a **real compiled guest component**, fixture built by
`scripts/build-guest.sh`):
- `real_guest_full_lifecycle` — instantiate → init (register-effect +
  emit) → async call-service round trip → dispose-effect at unload; event
  stream observed: `["init effect=0", "calc=3", "dispose effect=0"]`
- `remount_gets_a_fresh_guest_instance`, `missing_wasm_file_is_a_mount_failure_not_a_hang`
- unit: pattern matching, registry fan-out/reaping, memory limiter, resolve

Load-bearing ones:
- `unload_rewinds_everything_in_reverse_order` — the temporal baseline
- `failed_apply_unwinds_partial_effects`
- `inject_pending_then_activation_and_cascade_reload` — the spatial baseline
- `disposing_a_never_activated_fiber_removes_it`
- `waterfall_transform_and_veto` / `waterfall_prepend_runs_before_ordinary`
- `isolate_groups_get_private_service_instances`
- `hmr_reload_on_file_change` — real file-watch reconciliation
- review regressions: `panicking_apply_unwinds_effects_and_scheduler_survives`, `failed_apply_removes_services_and_restarts_dependents`, `loader_retries_failed_mounts`

## Status & deferred items

Done: P1 (core + tests), P2 (Wasm host on wasmtime 47), P3 (loader + HMR).
Deferred to v0.2 (documented in [docs/design.md §8](docs/design.md)):
guest-side waterfall middleware (needs a `handle-waterfall` export + handle
resource), epoch interruption alongside fuel, pattern-filtered dispatch at
the tap (currently broadcast-all), bail sugar.
