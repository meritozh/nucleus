//! Regression tests for issues found in the post-implementation review.

use cordis_core::{App, Context, FiberState, MountSpec, Plugin};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

// ---- 1: a panicking apply must not strand the wake scheduler -------------

struct PanickingPlugin {
    released: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl Plugin for PanickingPlugin {
    fn name(&self) -> &str {
        "panicking"
    }
    async fn apply(&self, ctx: Context) -> cordis_core::Result<()> {
        let released = self.released.clone();
        ctx.effect_fn(move || {
            let released = released.clone();
            async move {
                released.store(true, Ordering::SeqCst);
            }
        });
        panic!("boom: panicking apply");
    }
}

struct HealthyPlugin {
    mounts: Arc<AtomicUsize>,
    provides: bool,
}

#[async_trait::async_trait]
impl Plugin for HealthyPlugin {
    fn name(&self) -> &str {
        "healthy"
    }
    async fn apply(&self, ctx: Context) -> cordis_core::Result<()> {
        self.mounts.fetch_add(1, Ordering::SeqCst);
        if self.provides {
            ctx.provide::<dyn std::any::Any + Send + Sync>("svc", Arc::new(0u32))
                .await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn panicking_apply_unwinds_effects_and_scheduler_survives() {
    // silence the panic backtrace for this test only
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let released = Arc::new(AtomicBool::new(false));
    let mounts = Arc::new(AtomicUsize::new(0));

    let app = App::new().with_static(cordis_core::static_source! {
        "panicking" => { let r = released.clone(); move |_c| Ok(Arc::new(PanickingPlugin { released: r.clone() })) },
        "healthy"  => { let m = mounts.clone(); move |_c| Ok(Arc::new(HealthyPlugin { mounts: m.clone(), provides: false })) },
        "provider" => { let m = mounts.clone(); move |_c| Ok(Arc::new(HealthyPlugin { mounts: m.clone(), provides: true })) },
        "consumer" => |_c| Ok(Arc::new(ConsumerWithInject)),
    });

    let broken = app.mount(MountSpec::new("panicking")).await.unwrap();
    assert_eq!(broken.state(), FiberState::Failed);
    let info = broken.info().unwrap();
    let err = info.error.clone().unwrap();
    assert!(err.contains("panicked"), "{info:?}");
    assert!(
        released.load(Ordering::SeqCst),
        "effects registered before the panic must still unwind"
    );

    // the scheduler must still work: a fresh mount activates...
    let healthy = app.mount(MountSpec::new("healthy")).await.unwrap();
    assert_eq!(healthy.state(), FiberState::Active);

    // ...and a Pending consumer still activates when a provider appears
    let consumer = app.mount(MountSpec::new("consumer")).await.unwrap();
    assert_eq!(consumer.state(), FiberState::Pending);
    let provider = app.mount(MountSpec::new("provider")).await.unwrap();
    assert_eq!(provider.state(), FiberState::Active);
    assert_eq!(
        consumer.state(),
        FiberState::Active,
        "wake_pending survived the panicking apply"
    );

    std::panic::set_hook(prev_hook);
}

// ---- 2: a failed apply removes provided services and restarts dependents -

struct ProvideThenFail;

#[async_trait::async_trait]
impl Plugin for ProvideThenFail {
    fn name(&self) -> &str {
        "provider-that-fails"
    }
    async fn apply(&self, ctx: Context) -> cordis_core::Result<()> {
        // provide triggers wake: a Pending consumer mounts against the
        // service while this apply is still running...
        ctx.provide::<dyn std::any::Any + Send + Sync>("svc", Arc::new(1u32))
            .await?;
        // ...then the apply fails: the service must be removed centrally
        // and the dependent must be unloaded back to Pending.
        Err(cordis_core::Error::Waterfall("planned failure".into()))
    }
}

struct ConsumerWithInject;

static INJECT_LIST: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();

#[async_trait::async_trait]
impl Plugin for ConsumerWithInject {
    fn name(&self) -> &str {
        "consumer"
    }
    fn inject(&self) -> &[String] {
        INJECT_LIST.get_or_init(|| vec!["svc".to_string()])
    }
    async fn apply(&self, _ctx: Context) -> cordis_core::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn failed_apply_removes_services_and_restarts_dependents() {
    let app = App::new().with_static(cordis_core::static_source! {
        "provider-that-fails" => |_c| Ok(Arc::new(ProvideThenFail)),
        "consumer" => |_c| Ok(Arc::new(ConsumerWithInject)),
    });

    let consumer = app.mount(MountSpec::new("consumer")).await.unwrap();
    assert_eq!(consumer.state(), FiberState::Pending);

    let provider = app.mount(MountSpec::new("provider-that-fails")).await.unwrap();
    assert_eq!(provider.state(), FiberState::Failed);
    assert!(
        app.services().is_empty(),
        "a failed apply must not leave its provided services behind: {:?}",
        app.services()
    );
    assert_eq!(
        consumer.state(),
        FiberState::Pending,
        "a dependent that mounted against the doomed provider's service must restart to Pending"
    );
    assert_eq!(consumer.info().unwrap().missing, vec!["svc".to_string()]);
}
