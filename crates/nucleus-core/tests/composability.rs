//! The load-bearing test for temporal composability: unloading a fiber
//! rewinds *everything* it registered — services, listeners, child fibers,
//! external resources — in reverse order.

use nucleus_core::{App, Context, DispatchMode, FiberState, MountSpec, Plugin};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize)]
struct Ping {
    #[allow(dead_code)]
    n: u64,
}
impl nucleus_core::Event for Ping {
    const NAME: &'static str = "test/ping";
    const MODE: DispatchMode = DispatchMode::Emit;
}

#[derive(Default)]
struct Telemetry {
    /// Order of disposal events, for the LIFO assertion.
    log: Mutex<Vec<String>>,
    live_listeners: AtomicUsize,
    /// Shared "external resource" that only a disposer can release.
    timers: AtomicUsize,
}

struct SuitePlugin {
    telemetry: Arc<Telemetry>,
}

struct ChildPlugin {
    telemetry: Arc<Telemetry>,
}

#[async_trait::async_trait]
impl Plugin for SuitePlugin {
    fn name(&self) -> &str {
        "suite"
    }

    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        let telemetry = self.telemetry.clone();

        // 1. an external resource acquired inside ctx.effect
        let t1 = telemetry.clone();
        ctx.effect(async move {
            t1.timers.fetch_add(1, Ordering::SeqCst);
            let t2 = t1.clone();
            move || {
                let t = t2.clone();
                async move {
                    t.log.lock().unwrap().push("timer:root".to_string());
                    t.timers.fetch_sub(1, Ordering::SeqCst);
                }
            }
        })
        .await;

        // 2. an event listener (registration is itself reversible)
        let t3 = telemetry.clone();
        ctx.on::<Ping, _, _>(move |_ctx, _ev| {
            let t = t3.clone();
            async move {
                t.live_listeners.fetch_add(1, Ordering::SeqCst);
            }
        })?;

        // 3. a child plugin with its own effect, to check recursive unwind
        ctx.plugin(MountSpec::new("child").with_row_id("child")).await?;

        // 4. a plain disposer registered last
        let t4 = telemetry.clone();
        ctx.effect_fn(move || {
            let t = t4.clone();
            async move {
                t.log.lock().unwrap().push("plain:root".to_string());
            }
        });
        Ok(())
    }
}

#[async_trait::async_trait]
impl Plugin for ChildPlugin {
    fn name(&self) -> &str {
        "child"
    }
    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        let t = self.telemetry.clone();
        ctx.effect_fn(move || {
            let t = t.clone();
            async move {
                t.log.lock().unwrap().push("child:root-child".to_string());
            }
        });
        Ok(())
    }
}

#[tokio::test]
async fn unload_rewinds_everything_in_reverse_order() {
    let telemetry = Arc::new(Telemetry::default());
    let app = App::new().with_static({
        let t = telemetry.clone();
        nucleus_core::static_source! {
            "suite" => { let t = t.clone(); move |_cfg| Ok(Arc::new(SuitePlugin { telemetry: t.clone() })) },
            "child" => { let t = t.clone(); move |_cfg| Ok(Arc::new(ChildPlugin { telemetry: t.clone() })) },
        }
    });

    let fiber = app.mount(MountSpec::new("suite")).await.unwrap();
    assert_eq!(fiber.state(), FiberState::Active);

    // everything is live
    assert_eq!(telemetry.timers.load(Ordering::SeqCst), 1);
    app.context().emit(&Ping { n: 1 }).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(telemetry.live_listeners.load(Ordering::SeqCst), 1);
    assert!(app.fibers().len() >= 3, "app + suite + child mounted");

    fiber.dispose().await.unwrap();

    // fibers gone (app fiber 0 remains)
    let after: Vec<_> = app.fibers();
    assert_eq!(after.len(), 1, "only the app fiber remains: {after:?}");
    assert_eq!(after[0].id, 0);
    assert!(app.services().is_empty());

    // external resource released
    assert_eq!(telemetry.timers.load(Ordering::SeqCst), 0);

    // listeners removed: a second emit reaches nobody
    app.context().emit(&Ping { n: 2 }).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(telemetry.live_listeners.load(Ordering::SeqCst), 1);

    // disposal order strictly LIFO: plain (4th) -> child (3rd) -> timer (1st).
    let log = telemetry.log.lock().unwrap().clone();
    assert_eq!(
        log,
        vec!["plain:root".to_string(), "child:root-child".to_string(), "timer:root".to_string()],
        "disposers must unwind in reverse registration order"
    );
}

#[tokio::test]
async fn failed_apply_unwinds_partial_effects() {
    use std::sync::atomic::AtomicBool;

    let released = Arc::new(AtomicBool::new(false));

    struct Broken {
        released: Arc<AtomicBool>,
    }
    #[async_trait::async_trait]
    impl Plugin for Broken {
        fn name(&self) -> &str {
            "broken"
        }
        async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
            let released = self.released.clone();
            ctx.effect_fn(move || {
                let released = released.clone();
                async move {
                    released.store(true, Ordering::SeqCst);
                }
            });
            // register an effect, then fail: the effect must still unwind
            Err(nucleus_core::Error::Waterfall("boom".into()))
        }
    }

    let app = App::new().with_static(
        nucleus_core::static_source! {
            "broken" => { let r = released.clone(); move |_cfg| Ok(Arc::new(Broken { released: r.clone() })) }
        },
    );
    let fiber = app.mount(MountSpec::new("broken")).await.unwrap();

    let info = fiber.info().unwrap();
    assert_eq!(info.state, FiberState::Failed, "info: {info:?}");
    assert!(info.error.unwrap().contains("boom"));
    assert!(
        released.load(Ordering::SeqCst),
        "partial effects must be unwound even when apply fails"
    );
}
