//! # cordis-wasm
//!
//! Wasm [`PluginSource`](cordis_core::PluginSource) for cordis-core, on
//! wasmtime 47 / WASI 0.3 native async.
//!
//! - guests are WebAssembly components implementing the `plugin` world in
//!   `wit/` (deps/host.wit + plugin.wit);
//! - each mount instantiates a fresh component (reload = new instance; the
//!   fiber model gives us teardown for free);
//! - effects registered by the guest (`register-effect`) are unwound by
//!   calling the guest's `dispose-effect` export in LIFO order;
//! - guest execution is bounded by fuel (default 10M per call) and a
//!   memory cap (default 64 MiB).
//!
//! wasmtime 47 concurrent codegen shape (why the code looks like it does):
//! sync WIT imports land on the `Host` trait (`&mut self`), async imports
//! on `HostWithStore<T>` as associated functions taking an `Access`
//! (which is `AsContextMut`, needed to create streams); exported async
//! functions are called through `Store::run_concurrent` + `Accessor`.

use cordis_core::{Context, Error};
use cordis_core::Plugin as CorePlugin;
use futures::future::BoxFuture;
use serde_json::Value;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context as TaskContext, Poll};
use wasmtime::component::{
    Destination, StreamReader, StreamProducer, StreamResult, VecBuffer,
};
use wasmtime::{Config, Engine, Store};

wasmtime::component::bindgen!({
    path: "../../wit",
    world: "plugin",
});

type Event = cordis::host::host::Event;
type EventTx = tokio::sync::mpsc::Sender<Event>;

/// Resource bounds for one guest instance.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Fuel per guest call (init / dispose-effect / host callbacks).
    pub fuel: u64,
    /// Maximum linear memory of the guest, in bytes.
    pub memory_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            fuel: 10_000_000,
            memory_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Host-side service contract for guest `call-service`: only services
/// registered under `Arc<dyn Invokable>` are guest-visible.
pub trait Invokable: Send + Sync {
    fn invoke(&self, method: &str, arg: Value) -> BoxFuture<'static, Result<Value, String>>;
}

struct MemoryLimiter(usize);

impl wasmtime::ResourceLimiter for MemoryLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool, wasmtime::Error> {
        Ok(desired <= self.0 || desired <= current)
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> Result<bool, wasmtime::Error> {
        Ok(desired <= 1_000_000 || desired <= current)
    }
}

/// Live guest subscriptions, shared between the Store (subscribe host
/// call) and the sync event tap. Lives outside the Store so the tap can
/// reach it without an async lock.
#[derive(Default)]
struct SubscriberRegistry {
    channels: std::sync::Mutex<Vec<(String, EventTx)>>,
}

impl SubscriberRegistry {
    /// Sync fan-out; never blocks the dispatcher (try_send into a bounded
    /// channel — a slow guest drops events rather than stalling the host).
    fn dispatch(&self, name: &str, event: Event) {
        let mut channels = self.channels.lock().unwrap_or_else(|e| e.into_inner());
        channels.retain(|(pattern, tx)| {
            if tx.is_closed() {
                return false;
            }
            if !Self::matches(pattern, name) {
                return true;
            }
            tx.try_send(event.clone()).is_ok()
        });
    }

    fn matches(pattern: &str, name: &str) -> bool {
        pattern == "*"
            || pattern == name
            || (pattern.ends_with("/*") && name.starts_with(&pattern[..pattern.len() - 1]))
    }
}

struct HostState {
    ctx: Context,
    next_effect: u64,
    registry: Arc<SubscriberRegistry>,
    limiter: MemoryLimiter,
    /// Fuel budget, re-armed before each guest entry (init, dispose-effect).
    fuel: u64,
    /// Weak to the owning session so host calls (register-effect) can wire
    /// disposers without an Arc cycle with the Store.
    session: Weak<tokio::sync::Mutex<Session>>,
}

impl HostState {
    fn new(ctx: Context, limits: &Limits, registry: Arc<SubscriberRegistry>) -> Self {
        Self {
            ctx,
            next_effect: 0,
            registry,
            limiter: MemoryLimiter(limits.memory_bytes),
            fuel: limits.fuel,
            session: Weak::new(),
        }
    }
}

struct Session {
    store: Store<HostState>,
    bindings: Plugin,
}

/// A [`cordis_core::PluginSource`] loading `.wasm` components from a
/// directory. Plugin names resolve to `<root>/<name>.wasm`.
pub struct WasmSource {
    engine: Engine,
    root: PathBuf,
    limits: Limits,
}

impl WasmSource {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let mut cfg = Config::new();
        cfg.concurrency_support(true);
        cfg.consume_fuel(true);
        let engine = Engine::new(&cfg).expect("wasmtime engine");
        Self {
            engine,
            root: root.into(),
            limits: Limits::default(),
        }
    }

    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    fn resolve(&self, name: &str) -> PathBuf {
        let candidate = self.root.join(name);
        if candidate.extension().is_some() {
            candidate
        } else {
            self.root.join(format!("{name}.wasm"))
        }
    }
}

#[async_trait::async_trait]
impl cordis_core::PluginSource for WasmSource {
    async fn load(&self, name: &str, config: &Value) -> Result<Arc<dyn CorePlugin>, Error> {
        let path = self.resolve(name);
        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|e| Error::PluginNotFound(format!("{}: {e}", path.display())))?;
        let component = wasmtime::component::Component::new(&self.engine, &bytes)
            .map_err(|e| Error::PluginNotFound(format!("{}: {e}", path.display())))?;
        Ok(Arc::new(WasmPlugin {
            engine: self.engine.clone(),
            component,
            limits: self.limits,
            config: config.clone(),
        }))
    }
}

/// One loaded guest: holds the compiled component; `apply` instantiates it.
struct WasmPlugin {
    engine: Engine,
    component: wasmtime::component::Component,
    limits: Limits,
    /// JSON config from the mount row, handed to guest `init`. A cascade
    /// remount reuses this instance, so the config is stable for the
    /// fiber's lifetime (config changes come through loader Reconfig,
    /// which re-loads the plugin).
    config: Value,
}

#[async_trait::async_trait]
impl CorePlugin for WasmPlugin {
    fn name(&self) -> &str {
        "wasm-plugin"
    }

    async fn apply(&self, ctx: Context) -> Result<(), Error> {
        let registry = Arc::new(SubscriberRegistry::default());
        let mut store = Store::new(
            &self.engine,
            HostState::new(ctx.clone(), &self.limits, registry.clone()),
        );
        store.set_fuel(self.limits.fuel).map_err(werr)?;
        store.limiter(|state| &mut state.limiter);

        // core events -> guest streams (the tap itself is an effect)
        ctx.on_any(move |name, payload| {
            registry.dispatch(
                name,
                Event {
                    name: name.to_string(),
                    payload: payload.to_string(),
                },
            );
        })?;

        let mut linker = wasmtime::component::Linker::new(&self.engine);
        Plugin::add_to_linker::<HostState, wasmtime::component::HasSelf<HostState>>(
            &mut linker,
            |state: &mut HostState| state,
        )
        .map_err(werr)?;
        let bindings = Plugin::instantiate_async(&mut store, &self.component, &linker)
            .await
            .map_err(werr)?;

        let session = Arc::new(tokio::sync::Mutex::new(Session { store, bindings }));

        // Register the session-drop effect BEFORE init runs, so the LIFO
        // stack is [tap, drop_session, guest-disposers...]: at unload the
        // guest's dispose-effect callbacks run first, and only then the
        // last strong Arc<Session> drops (cancelling in-flight guest
        // execution and closing all streams). Registered after init, the
        // Store would die before the guest disposers could upgrade their
        // Weak handles — silently skipping guest cleanup entirely.
        let drop_session = session.clone();
        ctx.effect_fn(move || {
            let session = drop_session.clone();
            Box::pin(async move {
                let _ = session.lock().await;
            })
        });

        {
            let mut guard = session.lock().await;
            guard.store.data_mut().session = Arc::downgrade(&session);
            // guest init: `apply` of the cordis world. Fuel is a per-store
            // pool that only depletes — re-arm before every guest entry so
            // a busy init cannot starve later dispose-effect calls.
            guard.store.set_fuel(self.limits.fuel).map_err(werr)?;
            let config = self.config.to_string();
            let outcome = {
                let Session { store, bindings } = &mut *guard;
                let bindings: &Plugin = &*bindings;
                store
                    .run_concurrent(async |accessor| {
                        bindings.call_init(accessor, config).await
                    })
                    .await
                    .map_err(werr)?
            };
            if let Err(e) = outcome {
                return Err(Error::ApplyFailed(
                    "wasm".into(),
                    format!("guest init failed: {e:?}"),
                ));
            }
        }

        Ok(())
    }
}

fn werr(e: wasmtime::Error) -> Error {
    Error::ApplyFailed("wasm".into(), e.to_string())
}

// ---- generated Host traits -----------------------------------------------
//
// sync WIT imports (register-effect, emit) -> `Host` (&mut self);
// async imports (subscribe, call-service) -> `HostWithStore<HostState>`
// associated fns with `Access` (store access for stream creation).

impl cordis::host::host::Host for HostState {
    fn register_effect(&mut self, label: String) -> u64 {
        let id = self.next_effect;
        self.next_effect += 1;
        tracing::debug!(fiber = self.ctx.fiber_id(), label, id, "guest effect registered");

        // Wire a real disposer: at unload, call the guest's dispose-effect
        // export with this id. LIFO ordering comes from the fiber's
        // disposer stack; the session-drop effect was registered *before*
        // init ran, so guest disposers unwind before the Store drops.
        let session = self.session.clone();
        let ctx = self.ctx.clone();
        let fuel = self.fuel;
        ctx.effect_fn(move || {
            let session = session.clone();
            Box::pin(async move {
                if let Some(session) = session.upgrade() {
                    let mut guard = session.lock().await;
                    if let Err(e) = guard.store.set_fuel(fuel) {
                        tracing::warn!(id, error = %e, "cannot re-arm fuel before dispose");
                        return;
                    }
                    let Session { store, bindings } = &mut *guard;
                    let bindings: &Plugin = &*bindings;
                    if let Err(e) = store
                        .run_concurrent(async |accessor| {
                            bindings.call_dispose_effect(accessor, id).await
                        })
                        .await
                    {
                        tracing::warn!(id, error = %e, "guest dispose-effect trapped");
                    }
                }
            })
        });
        id
    }

    fn emit(&mut self, name: String, payload: String) {
        let value: Value = serde_json::from_str(&payload).unwrap_or(Value::String(payload));
        self.ctx.emit_dynamic(&name, value).ok();
    }
}

impl cordis::host::host::HostWithStore<HostState> for wasmtime::component::HasSelf<HostState> {
    fn subscribe(
        accessor: &wasmtime::component::Accessor<HostState, Self>,
        pattern: String,
    ) -> impl ::core::future::Future<Output = StreamReader<Event>> + Send {
        async move {
            let (tx, rx) = tokio::sync::mpsc::channel::<Event>(64);
            accessor.with(|mut a| {
                a.data_mut()
                    .registry
                    .channels
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push((pattern, tx));
            });
            accessor.with(|mut a| {
                StreamReader::new(&mut a, ChannelProducer::new(rx))
                    .expect("create guest event stream (table full / no concurrency?)")
            })
        }
    }

    fn call_service(
        accessor: &wasmtime::component::Accessor<HostState, Self>,
        service: String,
        method: String,
        arg: String,
    ) -> impl ::core::future::Future<Output = Result<String, String>> + Send {
        async move {
            let svc = accessor.with(|mut a| a.data_mut().ctx.try_service::<dyn Invokable>(&service));
            let arg: Value = serde_json::from_str(&arg).unwrap_or(Value::Null);
            match svc {
                Some(s) => match s.invoke(&method, arg).await {
                    Ok(v) => Ok(serde_json::to_string(&v).unwrap_or_default()),
                    Err(e) => Err(e),
                },
                None => Err(format!("service `{service}` not invokable")),
            }
        }
    }
}

/// Live channel producer: bridges a tokio mpsc receiver into a component
/// model stream read by the guest.
struct ChannelProducer<T> {
    rx: tokio::sync::mpsc::Receiver<T>,
}

impl<T> ChannelProducer<T> {
    fn new(rx: tokio::sync::mpsc::Receiver<T>) -> Self {
        Self { rx }
    }
}

impl<T, D> StreamProducer<D> for ChannelProducer<T>
where
    T: Unpin + Send + Sync + 'static,
{
    type Item = T;
    type Buffer = VecBuffer<T>;

    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        store: wasmtime::StoreContextMut<'a, D>,
        mut dst: Destination<'a, Self::Item, Self::Buffer>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        // Zero-length reads are readiness probes. Returning Pending here
        // without a registered waker would suspend the guest forever (the
        // mpsc receiver never sees the task waker on this path). The
        // component-model spec sanctions answering "ready" without
        // buffering; the guest then issues a non-zero read, which goes
        // through poll_recv below and registers the waker properly.
        if let Some(0) = dst.remaining(store) {
            return Poll::Ready(Ok(StreamResult::Completed));
        }
        let this = self.get_mut();
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(item)) => {
                dst.set_buffer(vec![item].into());
                Poll::Ready(Ok(StreamResult::Completed))
            }
            Poll::Ready(None) => Poll::Ready(Ok(StreamResult::Dropped)),
            Poll::Pending => {
                if finish {
                    Poll::Ready(Ok(StreamResult::Cancelled))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscriber_pattern_matching() {
        let m = SubscriberRegistry::matches;
        assert!(m("*", "anything/at-all"));
        assert!(m("stats/report", "stats/report"));
        assert!(m("stats/*", "stats/report"));
        assert!(!m("stats/*", "stats"));
        assert!(!m("stats/*", "other/report"));
        assert!(!m("stats/*", "statisreport"));
        // exact-boundary: pattern "/*" degenerates to prefix "/"
        assert!(m("/*", "/x"));
        assert!(!m("/*", "x"));
    }

    #[test]
    fn registry_dispatch_drops_closed_and_non_matching_kept() {
        let reg = SubscriberRegistry::default();
        let (tx1, mut rx1) = tokio::sync::mpsc::channel(1);
        let (tx2, _rx2) = tokio::sync::mpsc::channel(1);
        let (tx3, _rx3) = tokio::sync::mpsc::channel(1);
        {
            let mut chans = reg.channels.lock().unwrap();
            chans.push(("a/*".to_string(), tx1));
            chans.push(("b/*".to_string(), tx2));
            chans.push(("a/x".to_string(), tx3));
        }
        drop(_rx2); // closed subscriber must be reaped
        reg.dispatch(
            "a/x",
            Event {
                name: "a/x".to_string(),
                payload: "1".to_string(),
            },
        );
        let got = rx1.try_recv().expect("matching subscriber received");
        assert_eq!(got.payload, "1");
        let chans = reg.channels.lock().unwrap();
        assert_eq!(chans.len(), 2, "closed channel reaped: {chans:?}");
    }

    #[test]
    fn memory_limiter_caps_growth() {
        fn grow(
            lim: &mut MemoryLimiter,
            f: fn(
                &mut MemoryLimiter,
                usize,
                usize,
                Option<usize>,
            ) -> Result<bool, wasmtime::Error>,
            current: usize,
            desired: usize,
        ) -> bool {
            f(lim, current, desired, None).unwrap()
        }
        let mut lim = MemoryLimiter(1000);
        assert!(grow(&mut lim, wasmtime::ResourceLimiter::memory_growing, 0, 1000));
        assert!(
            grow(&mut lim, wasmtime::ResourceLimiter::memory_growing, 900, 800),
            "shrink ok"
        );
        assert!(!grow(&mut lim, wasmtime::ResourceLimiter::memory_growing, 900, 1001));
        assert!(grow(&mut lim, wasmtime::ResourceLimiter::table_growing, 0, 999_999));
        assert!(!grow(&mut lim, wasmtime::ResourceLimiter::table_growing, 0, 1_000_001));
    }

    #[test]
    fn limits_defaults_and_resolve() {
        let l = Limits::default();
        assert_eq!(l.fuel, 10_000_000);
        assert_eq!(l.memory_bytes, 64 * 1024 * 1024);

        let src = WasmSource::new("/tmp/plugins");
        assert_eq!(src.resolve("hello"), std::path::Path::new("/tmp/plugins/hello.wasm"));
        assert_eq!(
            src.resolve("nested/thing.wasm"),
            std::path::Path::new("/tmp/plugins/nested/thing.wasm"),
            "explicit extension is respected"
        );
    }
}
