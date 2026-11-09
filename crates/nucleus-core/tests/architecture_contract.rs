use nucleus_core::{App, Context, FiberState, MountSpec, Plugin, ServiceKey};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex, OnceLock};
const SVC: ServiceKey<Channel> = ServiceKey::new("channel");
const READER: ServiceKey<Reader> = ServiceKey::new("reader");

struct Channel {
    name: &'static str,
    closed: AtomicBool,
}
struct Reader(Arc<Channel>);
struct Provider(&'static str);
#[async_trait::async_trait]
impl Plugin for Provider {
    fn name(&self) -> &str {
        self.0
    }
    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        let channel = Arc::new(Channel {
            name: self.0,
            closed: false.into(),
        });
        let cleanup = channel.clone();
        ctx.effect_fn(move || async move {
            cleanup.closed.store(true, SeqCst);
        });
        ctx.provide_key(SVC, channel).await
    }
}
struct Consumer {
    starts: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
    observed: Arc<Mutex<Vec<&'static str>>>,
}
#[async_trait::async_trait]
impl Plugin for Consumer {
    fn name(&self) -> &str {
        "C"
    }
    fn inject(&self) -> &[String] {
        static INJECT: OnceLock<Vec<String>> = OnceLock::new();
        INJECT.get_or_init(|| vec![SVC.name().to_owned()])
    }
    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        let bound = ctx.service_key(SVC)?;
        self.starts.fetch_add(1, SeqCst);
        self.observed.lock().unwrap().push(bound.name);
        let stops = self.stops.clone();
        ctx.effect_fn(move || async move {
            stops.fetch_add(1, SeqCst);
        });
        ctx.provide_key(READER, Arc::new(Reader(bound))).await
    }
}
#[tokio::test]
async fn active_consumer_rebinds_after_shadowed_provider_disposal() {
    let starts: Arc<AtomicUsize> = Arc::default();
    let stops: Arc<AtomicUsize> = Arc::default();
    let observed: Arc<Mutex<Vec<&'static str>>> = Arc::default();
    let app = App::new().with_static(nucleus_core::static_source! {
        "G" => |_| Ok(Arc::new(Provider("G"))),
        "L" => |_| Ok(Arc::new(Provider("L"))),
        "C" => { let (starts,stops,observed)=(starts.clone(),stops.clone(),observed.clone());
                 move |_| Ok(Arc::new(Consumer { starts: starts.clone(), stops: stops.clone(), observed: observed.clone() })) },
    });
    let scope = app.new_scope();
    let scoped = |name| {
        let mut s = MountSpec::new(name);
        s.scope = scope;
        s
    };
    let g = app.mount(MountSpec::new("G")).await.unwrap();
    let c = app.mount(scoped("C")).await.unwrap();
    let ctx = app.context_with_scope(scope);
    assert_eq!(ctx.service_key(READER).unwrap().0.name, "G");
    app.mount(scoped("L")).await.unwrap();
    assert_eq!(ctx.service_key(SVC).unwrap().name, "L");
    g.dispose().await.unwrap();
    let now = ctx.service_key(SVC).unwrap();
    let bound = &ctx.service_key(READER).unwrap().0;
    println!("after dispose(G): C={:?}, registry={} closed={}, reader={} closed={}, starts={}, stops={}, history={:?}",
             c.state(),now.name,now.closed.load(SeqCst),bound.name,bound.closed.load(SeqCst),starts.load(SeqCst),stops.load(SeqCst),*observed.lock().unwrap());
    assert_eq!(c.state(), FiberState::Active);
    assert_eq!(now.name, "L");
    assert!(!now.closed.load(SeqCst));
    assert_eq!(bound.name, "L");
    assert!(!bound.closed.load(SeqCst));
    assert_eq!(starts.load(SeqCst), 2);
    assert_eq!(stops.load(SeqCst), 1);
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_shutdown_waiters_share_the_real_terminal_result() {
    use tokio::{
        sync::Notify,
        time::{timeout, Duration},
    };
    let app = App::new();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    app.context().try_effect_fn({
        let (started, release, calls) = (started.clone(), release.clone(), calls.clone());
        move || async move {
            calls.fetch_add(1, SeqCst);
            started.notify_one();
            release.notified().await;
            Err::<(), _>("cleanup result")
        }
    });
    let first = tokio::spawn({
        let app = app.clone();
        async move { app.shutdown().await }
    });
    started.notified().await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(timeout(Duration::from_millis(15), app.shutdown())
        .await
        .is_err());
    release.notify_one();
    let (a, b) = tokio::join!(app.shutdown(), app.shutdown());
    for result in [a, b, app.shutdown().await] {
        assert!(result.unwrap_err().to_string().contains("cleanup result"));
    }
    assert_eq!(calls.load(SeqCst), 1);
    assert!(app.fibers().is_empty());
}
