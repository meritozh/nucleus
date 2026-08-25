//! Integration tests against a REAL guest component.
//!
//! Fixture: `tests/fixtures/hello.wasm`, built from
//! `examples/wasm-guest` (see the build script next to it / README):
//!
//! ```text
//! rustup target add wasm32-unknown-unknown
//! (cd examples/wasm-guest && cargo build --release --target wasm32-unknown-unknown)
//! wasm-tools component new \
//!   examples/wasm-guest/target/wasm32-unknown-unknown/release/nucleus_guest_example.wasm \
//!   -o crates/nucleus-wasm/tests/fixtures/hello.wasm
//! ```
//!
//! The guest exercises every v0.1 import: register-effect, emit, and the
//! async call-service bridge; the host exercises instantiate / init /
//! dispose-effect against the actual component-model-async ABI.

use futures::future::BoxFuture;
use nucleus_core::{App, Event, FiberState, Invokable, MountSpec};
use nucleus_wasm::WasmSource;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone)]
struct GuestLog {
    #[allow(dead_code)]
    level: String,
    msg: String,
}
impl Event for GuestLog {
    const NAME: &'static str = "guest/log";
}

/// Host service exposed to guests through `call-service`.
struct Calc;

impl Invokable for Calc {
    fn invoke(&self, method: &str, arg: Value) -> BoxFuture<'static, Result<Value, String>> {
        let method = method.to_string();
        Box::pin(async move {
            match method.as_str() {
                "add" => {
                    let nums: Vec<u64> = serde_json::from_value(arg).map_err(|e| e.to_string())?;
                    Ok(Value::from(nums.iter().sum::<u64>()))
                }
                other => Err(format!("unknown method `{other}`")),
            }
        })
    }
}

fn fixture_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

async fn app_with_guest() -> (App, Arc<Mutex<Vec<String>>>) {
    let logs = Arc::new(Mutex::new(Vec::new()));

    let app = App::new().with_source("wasm", Arc::new(WasmSource::new(fixture_dir())));
    // collect guest/log events + expose the calc service to the guest
    let ctx = app.context();
    let l = logs.clone();
    ctx.on::<GuestLog, _, _>(move |_ctx, ev| {
        let logs = l.clone();
        async move {
            logs.lock().unwrap().push(ev.msg);
        }
    })
    .unwrap();
    ctx.provide::<dyn Invokable>("calc", Arc::new(Calc))
        .await
        .unwrap();
    (app, logs)
}

async fn drain() {
    // guest `emit` is fire-and-forget: the host spawns listeners, so give
    // the current-thread runtime a turn to run them.
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_guest_full_lifecycle() {
    let (app, logs) = app_with_guest().await;

    // mount: instantiate + init; the guest registers an effect and calls
    // back into the host's calc service.
    let fiber = app
        .mount(MountSpec::new("hello").with_source("wasm"))
        .await
        .expect("guest must instantiate and run init");
    assert_eq!(fiber.state(), FiberState::Active, "{:?}", fiber.info());
    drain().await;

    {
        let logs = logs.lock().unwrap().clone();
        assert!(
            logs.iter().any(|m| m.starts_with("init effect=")),
            "guest init ran and emitted: {logs:?}"
        );
        assert!(
            logs.iter().any(|m| m == "calc=3"),
            "async call-service bridge returned 1+2: {logs:?}"
        );
    }

    // unload: the host must call the guest's dispose-effect export
    fiber.dispose().await.unwrap();
    drain().await;
    {
        let logs = logs.lock().unwrap().clone();
        if std::env::var("NUCLEUS_TEST_VERBOSE").is_ok() {
            println!("guest events: {logs:?}");
        }
        assert!(
            logs.iter().any(|m| m.starts_with("dispose effect=")),
            "guest dispose-effect ran at unload: {logs:?}"
        );
        // LIFO + wiring: exactly one effect was registered (id 0)
        assert!(
            logs.iter().any(|m| m == "dispose effect=0"),
            "dispose-effect received the registered handle: {logs:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remount_gets_a_fresh_guest_instance() {
    let (app, logs) = app_with_guest().await;

    let f1 = app
        .mount(MountSpec::new("hello").with_source("wasm"))
        .await
        .unwrap();
    f1.dispose().await.unwrap();
    drain().await;
    let after_first = logs.lock().unwrap().len();
    assert!(
        after_first >= 3,
        "init x2 events + calc + dispose: {after_first}"
    );

    // a fresh mount must run init again (reload = new instance)
    let f2 = app
        .mount(MountSpec::new("hello").with_source("wasm"))
        .await
        .unwrap();
    drain().await;
    let after_second = logs.lock().unwrap().len();
    assert!(
        after_second > after_first,
        "second mount re-ran guest init: {after_first} -> {after_second}"
    );
    f2.dispose().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_wasm_file_is_a_mount_failure_not_a_hang() {
    let app = App::new().with_source("wasm", Arc::new(WasmSource::new(fixture_dir())));
    let result = app
        .mount(MountSpec::new("does-not-exist").with_source("wasm"))
        .await;
    assert!(result.is_err(), "missing artifact must fail fast");
}
