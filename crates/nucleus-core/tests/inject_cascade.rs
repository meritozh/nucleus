//! Spatial composability: inject-driven activation and cascade reload.
//!
//! The load-bearing scenario (straight from the nucleus tutorial):
//! - a consumer with `inject: ["greeter"]` stays PENDING until a provider
//!   mounts, regardless of mount order;
//! - unmounting the provider unloads the consumer and re-queues it as
//!   PENDING;
//! - mounting a *different* provider implementation cleanly restarts the
//!   consumer against the new implementation.

use nucleus_core::{App, Context, FiberState, MountSpec, Plugin};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

trait Greeter: Send + Sync {
    fn greet(&self, who: &str) -> String;
}

struct V1Greeter;
impl Greeter for V1Greeter {
    fn greet(&self, who: &str) -> String {
        format!("hello from v1, {who}")
    }
}

struct V2Greeter;
impl Greeter for V2Greeter {
    fn greet(&self, who: &str) -> String {
        format!("HELLO FROM V2, {who}")
    }
}

struct GreeterPlugin {
    make: Arc<dyn Fn() -> Arc<dyn Greeter> + Send + Sync>,
}

#[async_trait::async_trait]
impl Plugin for GreeterPlugin {
    fn name(&self) -> &str {
        "greeter"
    }
    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        ctx.provide::<dyn Greeter>("greeter", (self.make)()).await
    }
}

struct ConsumerPlugin {
    /// Last greeting observed by the consumer, appended on each (re)mount.
    seen: Arc<Mutex<Vec<String>>>,
    /// How many times apply ran.
    mounts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Plugin for ConsumerPlugin {
    fn name(&self) -> &str {
        "consumer"
    }
    fn inject(&self) -> &[String] {
        static LIST: OnceLock<Vec<String>> = OnceLock::new();
        LIST.get_or_init(|| vec!["greeter".to_string()])
    }
    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        self.mounts.fetch_add(1, Ordering::SeqCst);
        let greeter = ctx
            .service::<dyn Greeter>("greeter")
            .map_err(|e| nucleus_core::Error::Waterfall(format!("consumer: {e}")))?;
        self.seen.lock().unwrap().push(greeter.greet("world"));
        Ok(())
    }
}

#[tokio::test]
async fn inject_pending_then_activation_and_cascade_reload() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mounts = Arc::new(AtomicUsize::new(0));

    let app = App::new().with_static(nucleus_core::static_source! {
        "greeter" => |cfg| {
            let make: Arc<dyn Fn() -> Arc<dyn Greeter> + Send + Sync> = match cfg.get("variant").and_then(|v| v.as_str()) {
                Some("v2") => Arc::new(|| Arc::new(V2Greeter)),
                _ => Arc::new(|| Arc::new(V1Greeter)),
            };
            Ok(Arc::new(GreeterPlugin { make }))
        },
        "consumer" => {
            let seen = seen.clone();
            let mounts = mounts.clone();
            move |_cfg| Ok(Arc::new(ConsumerPlugin { seen: seen.clone(), mounts: mounts.clone() }))
        },
    });

    // 1. consumer first: stays PENDING, no apply, no crash
    let consumer = app
        .mount(MountSpec::new("consumer").with_row_id("consumer"))
        .await
        .unwrap();
    assert_eq!(consumer.state(), FiberState::Pending);
    assert_eq!(mounts.load(Ordering::SeqCst), 0);
    let info = consumer.info().unwrap();
    assert_eq!(
        info.missing,
        vec!["greeter".to_string()],
        "diagnostics show the missing service"
    );

    // 2. provider mounts (after the consumer — order does not matter)
    let v1 = app
        .mount(
            MountSpec::new("greeter")
                .with_row_id("greeter")
                .with_config(json!({ "variant": "v1" })),
        )
        .await
        .unwrap();
    assert_eq!(v1.state(), FiberState::Active);
    assert_eq!(
        consumer.state(),
        FiberState::Active,
        "consumer activates once the service exists"
    );
    assert_eq!(mounts.load(Ordering::SeqCst), 1);
    assert_eq!(
        seen.lock().unwrap().last().map(String::as_str),
        Some("hello from v1, world")
    );

    // 3. provider disappears: consumer unloads, re-queues as PENDING
    v1.dispose().await.unwrap();
    assert_eq!(
        consumer.state(),
        FiberState::Pending,
        "consumer waits again for the service to return"
    );
    assert_eq!(consumer.info().unwrap().missing, vec!["greeter".to_string()]);

    // 4. a *different* implementation mounts: consumer remounts against it
    let v2 = app
        .mount(
            MountSpec::new("greeter")
                .with_row_id("greeter-2")
                .with_config(json!({ "variant": "v2" })),
        )
        .await
        .unwrap();
    assert_eq!(v2.state(), FiberState::Active);
    assert_eq!(consumer.state(), FiberState::Active);
    assert_eq!(mounts.load(Ordering::SeqCst), 2);
    assert_eq!(
        seen.lock().unwrap().last().map(String::as_str),
        Some("HELLO FROM V2, world"),
        "hot-swapped provider cleanly restarts dependents"
    );
}

#[tokio::test]
async fn disposing_a_never_activated_fiber_removes_it() {
    let app = App::new().with_static(nucleus_core::static_source! {
        "consumer" => |_cfg| Ok(Arc::new(ConsumerPlugin {
            seen: Arc::new(Mutex::new(Vec::new())),
            mounts: Arc::new(AtomicUsize::new(0)),
        })),
    });
    let consumer = app.mount(MountSpec::new("consumer")).await.unwrap();
    assert_eq!(consumer.state(), FiberState::Pending);

    let id = consumer.id();
    consumer.dispose().await.unwrap();
    assert!(
        app.fiber(id).is_none(),
        "unmounting a fiber that never activated must remove its entry"
    );
    assert!(app.fibers().iter().all(|f| f.id != id));
}
