use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use nucleus_core::{App, Context, Error, FiberState, MountSpec, Plugin, ServiceKey};

trait Greeter: Send + Sync {
    fn greet(&self) -> &'static str;
}

struct EnglishGreeter;

impl Greeter for EnglishGreeter {
    fn greet(&self) -> &'static str {
        "hello"
    }
}

const GREETER: ServiceKey<dyn Greeter> = ServiceKey::new("greeter");

#[tokio::test]
async fn typed_service_key_preserves_the_native_contract() {
    let app = App::new();
    let ctx = app.context();
    ctx.provide_key(GREETER, Arc::new(EnglishGreeter))
        .await
        .unwrap();

    assert_eq!(ctx.service_key(GREETER).unwrap().greet(), "hello");
}

struct Provider {
    log: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait::async_trait]
impl Plugin for Provider {
    fn name(&self) -> &str {
        "provider"
    }

    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        ctx.provide_key(GREETER, Arc::new(EnglishGreeter)).await?;
        let log = self.log.clone();
        ctx.effect_fn(move || async move {
            log.lock().unwrap().push("provider");
        });
        Ok(())
    }
}

struct Consumer {
    log: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait::async_trait]
impl Plugin for Consumer {
    fn name(&self) -> &str {
        "consumer"
    }

    fn inject(&self) -> &[String] {
        static INJECT: OnceLock<Vec<String>> = OnceLock::new();
        INJECT.get_or_init(|| vec![GREETER.name().to_string()])
    }

    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        ctx.service_key(GREETER)?;
        let log = self.log.clone();
        ctx.effect_fn(move || async move {
            log.lock().unwrap().push("consumer");
        });
        Ok(())
    }
}

#[tokio::test]
async fn provider_disposal_stops_dependents_before_provider_resources() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let app = App::new().with_static(nucleus_core::static_source! {
        "provider" => {
            let log = log.clone();
            move |_| Ok(Arc::new(Provider { log: log.clone() }))
        },
        "consumer" => {
            let log = log.clone();
            move |_| Ok(Arc::new(Consumer { log: log.clone() }))
        },
    });

    let provider = app.mount(MountSpec::new("provider")).await.unwrap();
    let consumer = app.mount(MountSpec::new("consumer")).await.unwrap();
    assert_eq!(consumer.state(), FiberState::Active);

    provider.dispose().await.unwrap();

    assert_eq!(*log.lock().unwrap(), vec!["consumer", "provider"]);
    assert_eq!(consumer.state(), FiberState::Pending);
}

struct FallibleCleanup {
    log: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait::async_trait]
impl Plugin for FallibleCleanup {
    fn name(&self) -> &str {
        "fallible"
    }

    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        let first = self.log.clone();
        ctx.try_effect_fn(move || async move {
            first.lock().unwrap().push("first");
            Err("first cleanup failed")
        });
        let second = self.log.clone();
        ctx.try_effect_fn(move || async move {
            second.lock().unwrap().push("second");
            Err("second cleanup failed")
        });
        Ok(())
    }
}

#[tokio::test]
async fn disposer_failures_are_aggregated_without_skipping_later_cleanup() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let app = App::new().with_static(nucleus_core::static_source! {
        "fallible" => {
            let log = log.clone();
            move |_| Ok(Arc::new(FallibleCleanup { log: log.clone() }))
        },
    });

    let fiber = app.mount(MountSpec::new("fallible")).await.unwrap();
    let error = fiber.dispose().await.unwrap_err();

    let Error::DisposeFailures(failures) = error else {
        panic!("expected aggregated disposer failures");
    };
    assert_eq!(failures.len(), 2);
    assert_eq!(*log.lock().unwrap(), vec!["second", "first"]);
}

struct Waiting;

#[async_trait::async_trait]
impl Plugin for Waiting {
    fn name(&self) -> &str {
        "waiting"
    }

    fn inject(&self) -> &[String] {
        static INJECT: OnceLock<Vec<String>> = OnceLock::new();
        INJECT.get_or_init(|| vec!["missing".to_string()])
    }

    async fn apply(&self, _ctx: Context) -> nucleus_core::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn batch_mount_rolls_back_when_a_required_plugin_stays_pending() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let app = App::new().with_static(nucleus_core::static_source! {
        "provider" => {
            let log = log.clone();
            move |_| Ok(Arc::new(Provider { log: log.clone() }))
        },
        "waiting" => |_| Ok(Arc::new(Waiting)),
    });

    let error = app
        .mount_batch([MountSpec::new("provider"), MountSpec::new("waiting")])
        .await
        .unwrap_err();

    assert!(matches!(error, Error::BatchMountFailed { .. }));
    assert_eq!(app.fibers().len(), 1);
    assert!(app.services().is_empty());
    assert_eq!(*log.lock().unwrap(), vec!["provider"]);
}

struct NamedGreeter(&'static str);

impl Greeter for NamedGreeter {
    fn greet(&self) -> &'static str {
        self.0
    }
}

struct ScopedProvider {
    name: &'static str,
    greeting: &'static str,
}

#[async_trait::async_trait]
impl Plugin for ScopedProvider {
    fn name(&self) -> &str {
        self.name
    }

    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        ctx.provide_key(GREETER, Arc::new(NamedGreeter(self.greeting)))
            .await
    }
}

struct ObservingConsumer {
    observed: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait::async_trait]
impl Plugin for ObservingConsumer {
    fn name(&self) -> &str {
        "observing-consumer"
    }

    fn inject(&self) -> &[String] {
        static INJECT: OnceLock<Vec<String>> = OnceLock::new();
        INJECT.get_or_init(|| vec![GREETER.name().to_string()])
    }

    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        self.observed
            .lock()
            .unwrap()
            .push(ctx.service_key(GREETER)?.greet());
        Ok(())
    }
}

#[tokio::test]
async fn scoped_service_shadowing_falls_back_to_the_global_provider() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let app = App::new().with_static(nucleus_core::static_source! {
        "global-provider" => |_| Ok(Arc::new(ScopedProvider {
            name: "global-provider",
            greeting: "global",
        })),
        "local-provider" => |_| Ok(Arc::new(ScopedProvider {
            name: "local-provider",
            greeting: "local",
        })),
        "observing-consumer" => {
            let observed = observed.clone();
            move |_| Ok(Arc::new(ObservingConsumer {
                observed: observed.clone(),
            }))
        },
    });
    let scope = app.new_scope();

    app.mount(MountSpec::new("global-provider")).await.unwrap();
    let mut local_spec = MountSpec::new("local-provider");
    local_spec.scope = scope;
    let local = app.mount(local_spec).await.unwrap();
    let mut consumer_spec = MountSpec::new("observing-consumer");
    consumer_spec.scope = scope;
    let consumer = app.mount(consumer_spec).await.unwrap();

    assert_eq!(*observed.lock().unwrap(), vec!["local"]);
    local.dispose().await.unwrap();

    assert_eq!(consumer.state(), FiberState::Active);
    assert_eq!(*observed.lock().unwrap(), vec!["local", "global"]);
    assert_eq!(
        app.context_with_scope(scope)
            .service_key(GREETER)
            .unwrap()
            .greet(),
        "global"
    );
}

struct BlockingProvider {
    name: &'static str,
    greeting: &'static str,
    started: Option<Arc<tokio::sync::Notify>>,
    release: Option<Arc<tokio::sync::Notify>>,
}

#[async_trait::async_trait]
impl Plugin for BlockingProvider {
    fn name(&self) -> &str {
        self.name
    }

    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        ctx.provide_key(GREETER, Arc::new(NamedGreeter(self.greeting)))
            .await?;
        if let (Some(started), Some(release)) = (self.started.clone(), self.release.clone()) {
            ctx.effect_fn(move || async move {
                started.notify_one();
                release.notified().await;
            });
        }
        Ok(())
    }
}

#[tokio::test]
async fn unloading_provider_cannot_delete_a_concurrent_replacement() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let app = App::new().with_static(nucleus_core::static_source! {
        "old-provider" => {
            let started = started.clone();
            let release = release.clone();
            move |_| Ok(Arc::new(BlockingProvider {
                name: "old-provider",
                greeting: "old",
                started: Some(started.clone()),
                release: Some(release.clone()),
            }))
        },
        "new-provider" => |_| Ok(Arc::new(BlockingProvider {
            name: "new-provider",
            greeting: "new",
            started: None,
            release: None,
        })),
    });

    let old = app.mount(MountSpec::new("old-provider")).await.unwrap();
    let dispose = tokio::spawn(async move { old.dispose().await });
    started.notified().await;

    app.mount(MountSpec::new("new-provider")).await.unwrap();
    assert_eq!(app.context().service_key(GREETER).unwrap().greet(), "new");

    release.notify_one();
    dispose.await.unwrap().unwrap();
    assert_eq!(app.context().service_key(GREETER).unwrap().greet(), "new");
}

struct GenerationalConsumer {
    applies: Arc<AtomicUsize>,
    stale_cleanups: Arc<AtomicUsize>,
    live_cleanups: Arc<AtomicUsize>,
    first_started: Arc<tokio::sync::Notify>,
    first_release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl Plugin for GenerationalConsumer {
    fn name(&self) -> &str {
        "generational-consumer"
    }

    fn inject(&self) -> &[String] {
        static INJECT: OnceLock<Vec<String>> = OnceLock::new();
        INJECT.get_or_init(|| vec![GREETER.name().to_string()])
    }

    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        let activation = self.applies.fetch_add(1, Ordering::SeqCst);
        if activation == 0 {
            let started = self.first_started.clone();
            let release = self.first_release.clone();
            let cleanups = self.stale_cleanups.clone();
            ctx.try_effect(async move {
                started.notify_one();
                release.notified().await;
                move || async move {
                    cleanups.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), &'static str>(())
                }
            })
            .await;
            return Err(Error::Waterfall("stale activation failed".into()));
        }

        let cleanups = self.live_cleanups.clone();
        ctx.try_effect_fn(move || async move {
            cleanups.fetch_add(1, Ordering::SeqCst);
            Ok::<(), &'static str>(())
        });
        Ok(())
    }
}

#[tokio::test]
async fn stale_apply_cannot_cross_activation_generation_or_leak_acquired_resource() {
    let applies = Arc::new(AtomicUsize::new(0));
    let stale_cleanups = Arc::new(AtomicUsize::new(0));
    let live_cleanups = Arc::new(AtomicUsize::new(0));
    let first_started = Arc::new(tokio::sync::Notify::new());
    let first_release = Arc::new(tokio::sync::Notify::new());
    let app = App::new().with_static(nucleus_core::static_source! {
        "provider" => |_| Ok(Arc::new(ScopedProvider {
            name: "provider",
            greeting: "provider",
        })),
        "generational-consumer" => {
            let applies = applies.clone();
            let stale_cleanups = stale_cleanups.clone();
            let live_cleanups = live_cleanups.clone();
            let first_started = first_started.clone();
            let first_release = first_release.clone();
            move |_| Ok(Arc::new(GenerationalConsumer {
                applies: applies.clone(),
                stale_cleanups: stale_cleanups.clone(),
                live_cleanups: live_cleanups.clone(),
                first_started: first_started.clone(),
                first_release: first_release.clone(),
            }))
        },
    });

    let provider = app.mount(MountSpec::new("provider")).await.unwrap();
    let app_for_mount = app.clone();
    let mount = tokio::spawn(async move {
        app_for_mount
            .mount(MountSpec::new("generational-consumer"))
            .await
            .unwrap()
    });
    first_started.notified().await;

    provider.dispose().await.unwrap();
    let replacement = app.mount(MountSpec::new("provider")).await.unwrap();
    assert_eq!(applies.load(Ordering::SeqCst), 2);

    first_release.notify_one();
    let consumer = mount.await.unwrap();
    assert_eq!(consumer.state(), FiberState::Active);
    assert_eq!(stale_cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(live_cleanups.load(Ordering::SeqCst), 0);

    replacement.dispose().await.unwrap();
    assert_eq!(live_cleanups.load(Ordering::SeqCst), 1);
}

struct FailingDependent {
    applies: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Plugin for FailingDependent {
    fn name(&self) -> &str {
        "failing-dependent"
    }

    fn inject(&self) -> &[String] {
        static INJECT: OnceLock<Vec<String>> = OnceLock::new();
        INJECT.get_or_init(|| vec![GREETER.name().to_string()])
    }

    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        ctx.try_effect_fn(|| async { Err::<(), _>("dependent cleanup failed") });
        Ok(())
    }
}

#[tokio::test]
async fn failed_dependent_disposer_blocks_automatic_reactivation() {
    let applies = Arc::new(AtomicUsize::new(0));
    let app = App::new().with_static(nucleus_core::static_source! {
        "provider" => |_| Ok(Arc::new(ScopedProvider {
            name: "provider",
            greeting: "provider",
        })),
        "failing-dependent" => {
            let applies = applies.clone();
            move |_| Ok(Arc::new(FailingDependent { applies: applies.clone() }))
        },
    });

    let provider = app.mount(MountSpec::new("provider")).await.unwrap();
    let dependent = app
        .mount(MountSpec::new("failing-dependent"))
        .await
        .unwrap();

    assert!(matches!(
        provider.dispose().await,
        Err(Error::DisposeFailures(_))
    ));
    assert_eq!(dependent.state(), FiberState::Failed);

    app.mount(MountSpec::new("provider")).await.unwrap();
    assert_eq!(dependent.state(), FiberState::Failed);
    assert_eq!(applies.load(Ordering::SeqCst), 1);
}

struct BlockingDependent {
    applies: Arc<AtomicUsize>,
    dispose_started: Arc<tokio::sync::Notify>,
    dispose_release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl Plugin for BlockingDependent {
    fn name(&self) -> &str {
        "blocking-dependent"
    }

    fn inject(&self) -> &[String] {
        static INJECT: OnceLock<Vec<String>> = OnceLock::new();
        INJECT.get_or_init(|| vec![GREETER.name().to_string()])
    }

    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        let started = self.dispose_started.clone();
        let release = self.dispose_release.clone();
        ctx.effect_fn(move || async move {
            started.notify_one();
            release.notified().await;
        });
        Ok(())
    }
}

#[tokio::test]
async fn provider_replacement_does_not_skip_existing_dependents() {
    let applies = Arc::new(AtomicUsize::new(0));
    let dispose_started = Arc::new(tokio::sync::Notify::new());
    let dispose_release = Arc::new(tokio::sync::Notify::new());
    let app = App::new().with_static(nucleus_core::static_source! {
        "old-provider" => |_| Ok(Arc::new(ScopedProvider {
            name: "old-provider",
            greeting: "old",
        })),
        "new-provider" => |_| Ok(Arc::new(ScopedProvider {
            name: "new-provider",
            greeting: "new",
        })),
        "blocking-dependent" => {
            let applies = applies.clone();
            let dispose_started = dispose_started.clone();
            let dispose_release = dispose_release.clone();
            move |_| Ok(Arc::new(BlockingDependent {
                applies: applies.clone(),
                dispose_started: dispose_started.clone(),
                dispose_release: dispose_release.clone(),
            }))
        },
    });

    let old = app.mount(MountSpec::new("old-provider")).await.unwrap();
    let dependent = app
        .mount(MountSpec::new("blocking-dependent"))
        .await
        .unwrap();
    let dispose = tokio::spawn(async move { old.dispose().await });
    dispose_started.notified().await;

    app.mount(MountSpec::new("new-provider")).await.unwrap();
    dispose_release.notify_one();
    dispose.await.unwrap().unwrap();

    assert_eq!(dependent.state(), FiberState::Active);
    assert_eq!(applies.load(Ordering::SeqCst), 2);
}
