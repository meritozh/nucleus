//! Internal shared state: fibers, services, event routes, sources.

use crate::error::{Error, Result};
use crate::events::{PlainBox, WaterfallBox};
use crate::fiber::{FiberId, FiberState, ScopeId, GLOBAL};
use crate::plugin::Plugin;
use futures::future::BoxFuture;
use serde_json::Value;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

pub type Disposer = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

/// Object-safe trait for cloneable boxed values. Service entries store
/// `Arc<T>` boxed as `dyn CloneAny` so `get_service` can return a clone
/// without knowing `T`.
pub trait CloneAny: Any + Send + Sync {
    fn clone_box(&self) -> Box<dyn CloneAny>;
}

impl<T: Any + Clone + Send + Sync> CloneAny for T {
    fn clone_box(&self) -> Box<dyn CloneAny> {
        Box::new(self.clone())
    }
}

impl dyn CloneAny {
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        (self as &dyn Any).downcast_ref::<T>()
    }
}

/// Synchronous event tap: sees every dispatched event (name + payload),
/// used by bridging plugin sources (e.g. the Wasm host forwarding events
/// into guest streams).
pub type TapBox = Arc<dyn Fn(&str, &Value) + Send + Sync>;

pub(crate) struct ServiceEntry {
    /// Stores an `Arc<T>` (T possibly `dyn Trait`) boxed as `CloneAny`, so
    /// that `service::<T>()` can recover the exact `Arc<T>` via downcast.
    pub value: Box<dyn CloneAny>,
    pub type_id: TypeId,
    pub type_name: &'static str,
    pub provider: FiberId,
}

pub(crate) struct ListenerEntry<H> {
    pub id: u64,
    pub owner: FiberId,
    pub handler: H,
}

pub(crate) struct Route {
    pub plain: Vec<ListenerEntry<PlainBox>>,
    pub waterfall: Vec<ListenerEntry<WaterfallBox>>,
}

impl Default for Route {
    fn default() -> Self {
        Self {
            plain: Vec::new(),
            waterfall: Vec::new(),
        }
    }
}

impl Route {
    pub fn is_empty(&self) -> bool {
        self.plain.is_empty() && self.waterfall.is_empty()
    }
}

pub(crate) struct FiberData {
    pub id: FiberId,
    pub row_id: Option<String>,
    pub name: String,
    pub source: String,
    pub config: Value,
    pub plugin: Arc<dyn Plugin>,
    pub state: FiberState,
    pub parent: Option<FiberId>,
    pub children: Vec<FiberId>,
    pub inject: Vec<String>,
    pub scope: ScopeId,
    /// LIFO disposer stack; registration order, unwound in reverse.
    pub disposers: Vec<Disposer>,
    /// Service names this fiber provides (unwound with the fiber).
    pub provides: Vec<String>,
    pub error: Option<String>,
}

pub(crate) struct AppState {
    pub fibers: HashMap<FiberId, FiberData>,
    pub services: HashMap<(ScopeId, String), ServiceEntry>,
    pub routes: HashMap<String, Route>,
    pub taps: Vec<ListenerEntry<TapBox>>,
    pub sources: HashMap<String, Arc<dyn crate::plugin::PluginSource>>,
    pub next_fiber: FiberId,
    pub next_listener: u64,
    pub next_scope: ScopeId,
    /// Root-level mount order, for reverse-order shutdown.
    pub roots: Vec<FiberId>,
    pub shut_down: bool,
    /// Bound on one fiber's whole disposal phase.
    pub unload_timeout: Duration,
}

impl AppState {
    pub fn service_exists(&self, scope: ScopeId, name: &str) -> bool {
        self.services.contains_key(&(scope, name.to_string()))
            || (scope != GLOBAL && self.services.contains_key(&(GLOBAL, name.to_string())))
    }

    pub fn alloc_scope(&mut self) -> ScopeId {
        self.next_scope += 1;
        self.next_scope
    }
}

pub(crate) struct AppInner {
    pub state: Mutex<AppState>,
    /// Tokio handle used to spawn `emit` fire-and-forget dispatches and to
    /// arm unload timeouts. Only present on the host (native); Wasm guests
    /// forward async work to the host via WIT imports.
    #[cfg(not(target_arch = "wasm32"))]
    pub runtime: tokio::runtime::Handle,
    /// Reentrancy guard for `wake_pending` (apply → provide → wake).
    pub waking: AtomicBool,
    /// Weak back-reference so `&self` methods can recover an `Arc<Self>`
    /// (needed for child-fiber disposers and `Context` construction).
    pub self_weak: Weak<AppInner>,
}

/// Abstraction over the host capabilities that [`Context`] needs. The host
/// implementation is [`AppInner`]; the Wasm guest implementation lives in
/// `nucleus-guest` and bridges these calls to the WIT host imports. This is
/// what lets one plugin source compile both as a static (native) plugin and
/// as a dynamically-loaded Wasm component.
#[async_trait::async_trait]
pub trait Host: Any + Send + Sync {
    // ---- effects --------------------------------------------------------

    /// Push a disposer onto the owning fiber's LIFO stack.
    fn push_disposer(&self, fiber: FiberId, disposer: Disposer);

    // ---- services -------------------------------------------------------

    /// Register a service under `(scope, name)`. `value` is a boxed `Arc<T>`.
    async fn provide_service(
        &self,
        fiber: FiberId,
        scope: ScopeId,
        name: &str,
        value: Box<dyn CloneAny>,
        type_id: TypeId,
        type_name: &'static str,
    ) -> Result<()>;

    /// Look up a service by `(scope, name)` and `type_id`.
    fn get_service(
        &self,
        scope: ScopeId,
        name: &str,
        type_id: TypeId,
    ) -> Result<Box<dyn CloneAny>>;

    /// Whether a service exists at `(scope, name)` (falling back to GLOBAL).
    fn service_exists(&self, scope: ScopeId, name: &str) -> bool;

    // ---- events ---------------------------------------------------------

    /// Register a plain (emit/parallel/serial) listener. Returns the
    /// listener id used for removal.
    fn add_plain_listener(
        &self,
        fiber: FiberId,
        name: &str,
        handler: PlainBox,
    ) -> Result<u64>;

    /// Register a waterfall listener.
    fn add_waterfall_listener(
        &self,
        fiber: FiberId,
        name: &str,
        handler: WaterfallBox,
        prepend: bool,
    ) -> Result<u64>;

    fn remove_plain_listener(&self, name: &str, id: u64);
    fn remove_waterfall_listener(&self, name: &str, id: u64);

    /// Snapshot of plain listeners for an event (owner, scope, handler).
    fn snapshot_plain(&self, name: &str) -> Vec<(FiberId, ScopeId, PlainBox)>;

    /// Snapshot of waterfall listeners for an event.
    fn snapshot_waterfall(&self, name: &str) -> Vec<WaterfallBox>;

    /// Notify every event tap of a dispatch.
    fn notify_taps(&self, name: &str, payload: &Value);

    /// Dispatch an event. On the host this runs registered listeners
    /// (spawned on the runtime); in a Wasm guest it forwards to the host's
    /// `emit` import. This lets `Context::emit_dynamic` work in both
    /// backends without the guest needing a tokio runtime.
    fn emit(&self, name: &str, payload: &Value);

    // ---- taps -----------------------------------------------------------

    fn add_tap(&self, fiber: FiberId, handler: TapBox) -> Result<u64>;
    fn remove_tap(&self, id: u64);

    // ---- child plugins --------------------------------------------------

    /// Mount a child plugin owned by `parent`.
    async fn mount_child(
        &self,
        parent: FiberId,
        spec: crate::plugin::MountSpec,
    ) -> Result<FiberId>;

    // ---- fiber / app state ---------------------------------------------

    /// Whether the fiber is in `Loading` or `Active` state.
    fn fiber_is_live(&self, fiber: FiberId) -> bool;

    /// Whether the app has been shut down.
    fn is_shut_down(&self) -> bool;

    /// The tokio runtime handle. Only available on the host (native);
    /// Wasm guests have no tokio runtime and forward async work to the
    /// host via WIT imports.
    #[cfg(not(target_arch = "wasm32"))]
    fn runtime(&self) -> &tokio::runtime::Handle;

    /// Wake fibers whose injects are now satisfiable.
    async fn wake_pending(&self);
}

impl AppInner {
    pub fn lock_state(&self) -> MutexGuard<'_, AppState> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Missing inject names for one fiber (diagnostics + mount gating).
    #[allow(dead_code)]
    pub fn missing_injects(&self, id: FiberId) -> Vec<String> {
        let st = self.lock_state();
        let Some(f) = st.fibers.get(&id) else {
            return Vec::new();
        };
        f.inject
            .iter()
            .filter(|n| !st.service_exists(f.scope, n))
            .cloned()
            .collect()
    }

    pub fn fiber_info(&self, id: FiberId) -> Option<crate::fiber::FiberInfo> {
        let st = self.lock_state();
        let f = st.fibers.get(&id)?;
        let missing = if f.state == FiberState::Pending {
            f.inject
                .iter()
                .filter(|n| !st.service_exists(f.scope, n))
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        Some(crate::fiber::FiberInfo {
            id: f.id,
            row_id: f.row_id.clone(),
            name: f.name.clone(),
            source: f.source.clone(),
            config: f.config.clone(),
            state: f.state,
            parent: f.parent,
            inject: f.inject.clone(),
            missing,
            error: f.error.clone(),
        })
    }

    /// Remove every service this fiber provides and return the Active
    /// dependents that must be repended. Single critical section with
    /// `mem::take` so a racing `provide` cannot hide a new entry from the
    /// drain (provide rejects non-live fibers, closing the other half).
    pub(crate) fn remove_fiber_services(&self, id: FiberId) -> Vec<FiberId> {
        let mut st = self.lock_state();
        let Some(f) = st.fibers.get_mut(&id) else {
            return Vec::new();
        };
        let scope = f.scope;
        let provides = std::mem::take(&mut f.provides);
        let mut affected: Vec<FiberId> = Vec::new();
        for name in &provides {
            if st.services.remove(&(scope, name.clone())).is_some() {
                affected.extend(
                    st.fibers
                        .values()
                        .filter(|d| {
                            d.state == FiberState::Active
                                && (d.scope == scope || scope == GLOBAL)
                                && d.inject.iter().any(|i| i == name)
                        })
                        .map(|d| d.id),
                );
            }
        }
        affected.sort_unstable();
        affected.dedup();
        affected
    }

    /// Central, idempotent removal of event routes and taps owned by a
    /// fiber. Normally the disposers do this; this runs regardless, so a
    /// disposer dropped by the unload timeout cannot leak registrations.
    pub(crate) fn purge_fiber_registrations(&self, id: FiberId) {
        let mut st = self.lock_state();
        st.routes.retain(|_, r| {
            r.plain.retain(|e| e.owner != id);
            r.waterfall.retain(|e| e.owner != id);
            !r.is_empty()
        });
        st.taps.retain(|t| t.owner != id);
    }

    /// Invoke every event tap for a dispatch. Taps must not dispatch
    /// recursively (the Wasm bridge only queues into guest streams).
    pub(crate) fn notify_taps(&self, name: &str, payload: &Value) {
        let taps: Vec<TapBox> = {
            let st = self.lock_state();
            st.taps.iter().map(|t| t.handler.clone()).collect()
        };
        for tap in taps {
            tap(name, payload);
        }
    }

    // ---- mount / unload orchestration ------------------------------------

    pub(crate) async fn mount_fiber(
        &self,
        parent: Option<FiberId>,
        spec: crate::plugin::MountSpec,
    ) -> Result<FiberId, Error> {
        let source = {
            let st = self.lock_state();
            if st.shut_down {
                return Err(Error::Shutdown);
            }
            st.sources
                .get(&spec.source)
                .cloned()
                .ok_or_else(|| Error::SourceUnknown(spec.source.clone()))?
        };
        let plugin = source.load(&spec.name, &spec.config).await?;
        let inject = plugin.inject().to_vec();

        let id = {
            let mut st = self.lock_state();
            if st.shut_down {
                return Err(Error::Shutdown);
            }
            let id = st.next_fiber;
            st.next_fiber += 1;
            // A child mounted by a fiber that died mid-apply would be
            // orphaned (not a root, parent gone): refuse instead.
            if let Some(p) = parent {
                match st.fibers.get(&p).map(|f| f.state) {
                    Some(FiberState::Loading) | Some(FiberState::Active) => {}
                    _ => {
                        return Err(Error::ApplyFailed(
                            spec.name.clone(),
                            "parent fiber is no longer live".to_string(),
                        ))
                    }
                }
            }
            let scope = if spec.scope == GLOBAL {
                parent
                    .and_then(|p| st.fibers.get(&p))
                    .map(|p| p.scope)
                    .unwrap_or(GLOBAL)
            } else {
                spec.scope
            };
            st.fibers.insert(
                id,
                FiberData {
                    id,
                    row_id: spec.row_id.clone(),
                    name: spec.name.clone(),
                    source: spec.source.clone(),
                    config: spec.config.clone(),
                    plugin: plugin.clone(),
                    state: FiberState::Pending,
                    parent,
                    children: Vec::new(),
                    inject,
                    scope,
                    disposers: Vec::new(),
                    provides: Vec::new(),
                    error: None,
                },
            );
            match parent {
                Some(p) => {
                    if let Some(pf) = st.fibers.get_mut(&p) {
                        pf.children.push(id);
                        // The child's unload is itself an effect of the
                        // parent, at its LIFO position: a child mounted
                        // between two parent effects unwinds between them.
                        let app = self.self_weak.upgrade().expect("app alive");
                        pf.disposers.push(Box::new(move || {
                            let app = app.clone();
                            Box::pin(async move {
                                let _ = app.unload_fiber(id, false).await;
                            })
                        }));
                    }
                }
                None => st.roots.push(id),
            }
            id
        };

        self.try_progress(id).await;
        Ok(id)
    }

    /// `Pending → Loading → apply`. Stays `Pending` when injects are not
    /// satisfiable yet. `Failed` still unwinds partial effects.
    pub(crate) async fn try_progress(&self, id: FiberId) {
        let (plugin, ctx) = {
            let mut st = self.lock_state();
            let Some(f) = st.fibers.get(&id) else {
                return;
            };
            if f.state != FiberState::Pending {
                return;
            }
            let scope = f.scope;
            let unsatisfied = f
                .inject
                .iter()
                .any(|n| !st.service_exists(scope, n));
            if unsatisfied {
                return; // stay Pending: a provider may mount later
            }
            let plugin = f.plugin.clone();
            if let Some(f) = st.fibers.get_mut(&id) {
                f.state = FiberState::Loading;
            }
            (
                plugin,
                crate::context::Context {
                    host: self.self_weak.upgrade().expect("app alive") as Arc<dyn Host>,
                    fiber: id,
                    scope,
                },
            )
        };

        // A panicking apply must not strand the wake guard or leak its
        // partial effects: catch it and route it through the normal error
        // path (unwind + Failed).
        let outcome = {
            let name = plugin.name().to_string();
            match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                plugin.apply(ctx),
            ))
            .await
            {
                Ok(r) => r,
                Err(panic) => {
                    let msg = panic
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic payload".to_string());
                    Err(Error::ApplyFailed(name, format!("panicked: {msg}")))
                }
            }
        };

        match outcome {
            Ok(()) => {
                // Re-check injects under the lock: a provider may have been
                // removed while apply was running (the cascade filter only
                // catches Active dependents). If unsatisfied now, rewind to
                // Pending instead of activating a fiber with a dead inject.
                let unsatisfied = {
                    let st = self.lock_state();
                    match st.fibers.get(&id) {
                        Some(f) if f.state == FiberState::Loading => f
                            .inject
                            .iter()
                            .any(|n| !st.service_exists(f.scope, n)),
                        _ => false,
                    }
                };
                if unsatisfied {
                    if let Some(f) = self.lock_state().fibers.get_mut(&id) {
                        if f.state == FiberState::Loading {
                            f.state = FiberState::Unloading;
                        }
                    }
                    self.dispose_phase(id).await;
                    let affected = self.remove_fiber_services(id);
                    self.purge_fiber_registrations(id);
                    if let Some(f) = self.lock_state().fibers.get_mut(&id) {
                        if f.state == FiberState::Unloading {
                            f.state = FiberState::Pending;
                        }
                    }
                    for dep in affected {
                        let _ = Box::pin(self.unload_fiber(dep, true)).await;
                    }
                } else if let Some(f) = self.lock_state().fibers.get_mut(&id) {
                    if f.state == FiberState::Loading {
                        f.state = FiberState::Active;
                    }
                }
            }
            Err(err) => {
                tracing::error!(fiber = id, plugin = %plugin.name(), error = %err, "apply failed");
                // Unwind effects registered before the error, remove any
                // services that were provided (they are effects too), and
                // cascade to dependents that mounted against them.
                self.dispose_phase(id).await;
                let affected = self.remove_fiber_services(id);
                self.purge_fiber_registrations(id);
                {
                    let mut st = self.lock_state();
                    if let Some(f) = st.fibers.get_mut(&id) {
                        if matches!(f.state, FiberState::Loading | FiberState::Unloading) {
                            f.state = FiberState::Failed;
                        }
                        f.error = Some(err.to_string());
                    }
                }
                for dep in affected {
                    let _ = Box::pin(self.unload_fiber(dep, true)).await;
                }
            }
        }

        self.wake_pending().await;
    }

    /// Mount everything whose injects became satisfiable. Guarded against
    /// reentrant storms: nested calls (from `apply` → `provide`) return
    /// immediately and let the outer loop re-collect.
    pub(crate) async fn wake_pending(&self) {
        if self
            .waking
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        // Drop guard: a panic anywhere below (e.g. inside a listener tap)
        // must not strand the flag and brick every future wake.
        struct WakeGuard<'a>(&'a AtomicBool);
        impl Drop for WakeGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        let guard = WakeGuard(&self.waking);
        loop {
            let ids: Vec<FiberId> = {
                let st = self.lock_state();
                st.fibers
                    .values()
                    .filter(|f| f.state == FiberState::Pending)
                    .map(|f| f.id)
                    .collect()
            };
            if ids.is_empty() {
                break;
            }
            let mut progressed = false;
            for id in ids {
                let before = FiberState::Pending;
                // Boxed: try_progress ↔ wake_pending are mutually recursive.
                Box::pin(self.try_progress(id)).await;
                let after = {
                    let st = self.lock_state();
                    st.fibers.get(&id).map(|f| f.state).unwrap_or(FiberState::Disposed)
                };
                if after != before {
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
        drop(guard);
        // Missed-window recheck: a `provide` can land between our final
        // unsatisfied check and the flag clear, with its own nested wake
        // having been suppressed by the guard. If something is now
        // satisfiable, run one more round.
        let satisfiable = {
            let st = self.lock_state();
            st.fibers.values().any(|f| {
                f.state == FiberState::Pending
                    && f.inject.iter().all(|n| st.service_exists(f.scope, n))
            })
        };
        if satisfiable {
            Box::pin(self.wake_pending()).await;
        }
    }

    /// Unload a fiber: children first (reverse order), then own disposers
    /// (reverse registration order) under a timeout, then remove provided
    /// services and cascade to dependents. With `repend`, the fiber itself
    /// returns to `Pending` (its entry is retained) instead of `Disposed` —
    /// this is the "unload and reload when the service returns" semantics.
    pub(crate) async fn unload_fiber(&self, id: FiberId, repend: bool) -> Result<(), Error> {
        {
            let mut st = self.lock_state();
            let Some(f) = st.fibers.get_mut(&id) else {
                return Ok(());
            };
            match f.state {
                // Pending is explicitly disposable too: unmounting a fiber
                // that never activated just removes its entry (nucleus
                // unmounts rows regardless of whether they ever ran).
                FiberState::Pending | FiberState::Active | FiberState::Failed | FiberState::Loading => {
                    f.state = FiberState::Unloading;
                }
                _ => return Ok(()),
            }
        }

        // Children are unwound by the child-unload disposers on this
        // fiber's LIFO stack (pushed at mount time), so teardown order is
        // exactly reverse registration order.
        self.dispose_phase(id).await;

        // Remove provided services in one critical section (mem::take, so
        // a provide racing the drain cannot slip past it) and collect the
        // dependents that must restart. Central purge afterwards keeps
        // routes/taps from outliving the fiber even if a disposer hung and
        // was dropped by the unload timeout.
        self.purge_fiber_registrations(id);
        let parent = self.lock_state().fibers.get(&id).and_then(|f| f.parent);
        let affected = self.remove_fiber_services(id);
        {
            let mut st = self.lock_state();
            if let Some(p) = parent {
                if let Some(pf) = st.fibers.get_mut(&p) {
                    pf.children.retain(|c| *c != id);
                }
            }
            st.roots.retain(|r| *r != id);
            if let Some(f) = st.fibers.get_mut(&id) {
                f.children.clear();
                f.provides.clear();
            }
            if repend {
                // Retain the entry: the fiber waits for its services to
                // return, then remounts from the same plugin.
                if let Some(f) = st.fibers.get_mut(&id) {
                    f.state = FiberState::Pending;
                    f.error = None;
                }
            } else {
                st.fibers.remove(&id);
            }
        }

        for dep in affected {
            Box::pin(self.unload_fiber(dep, true)).await?;
        }

        self.wake_pending().await;
        Ok(())
    }

    /// Run the fiber's disposers in reverse registration order, bounded by
    /// the configured unload timeout. On timeout the fiber is still marked
    /// torn down (best-effort semantics, recorded in `error`).
    pub(crate) async fn dispose_phase(&self, id: FiberId) {
        let (disposers, timeout, name) = {
            let mut st = self.lock_state();
            let disposers = st
                .fibers
                .get_mut(&id)
                .map(|f| std::mem::take(&mut f.disposers))
                .unwrap_or_default();
            let timeout = st.unload_timeout;
            let name = st
                .fibers
                .get(&id)
                .map(|f| f.name.clone())
                .unwrap_or_default();
            (disposers, timeout, name)
        };
        if disposers.is_empty() {
            return;
        }
        let work = async {
            for d in disposers.into_iter().rev() {
                d().await;
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Err(_elapsed) = tokio::time::timeout(timeout, work).await {
                let msg = format!("unload timed out after {:?}", timeout);
                tracing::error!(fiber = id, plugin = %name, %msg);
                let mut st = self.lock_state();
                if let Some(f) = st.fibers.get_mut(&id) {
                    f.error = Some(msg.clone());
                }
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (timeout, name);
            work.await;
        }
    }
}

// ---------------------------------------------------------------------------
// Host trait implementation for AppInner
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl Host for AppInner {
    fn push_disposer(&self, fiber: FiberId, disposer: Disposer) {
        let mut st = self.lock_state();
        match st.fibers.get_mut(&fiber) {
            Some(f) if matches!(f.state, FiberState::Loading | FiberState::Active) => {
                f.disposers.push(disposer);
            }
            _ => {
                tracing::warn!(
                    fiber,
                    "effect registered on a non-live fiber; dropping"
                );
            }
        }
    }

    async fn provide_service(
        &self,
        fiber: FiberId,
        scope: ScopeId,
        name: &str,
        value: Box<dyn CloneAny>,
        type_id: TypeId,
        type_name: &'static str,
    ) -> Result<()> {
        let mut st = self.lock_state();
        if st.shut_down {
            return Err(Error::Shutdown);
        }
        if !matches!(
            st.fibers.get(&fiber).map(|f| f.state),
            Some(FiberState::Loading) | Some(FiberState::Active)
        ) {
            return Err(Error::FiberNotLive(fiber));
        }
        let key = (scope, name.to_string());
        if st.services.contains_key(&key) {
            return Err(Error::ServiceExists(name.to_string()));
        }
        st.services.insert(
            key,
            ServiceEntry {
                value,
                type_id,
                type_name,
                provider: fiber,
            },
        );
        if let Some(f) = st.fibers.get_mut(&fiber) {
            f.provides.push(name.to_string());
        }
        Ok(())
    }

    fn get_service(
        &self,
        scope: ScopeId,
        name: &str,
        type_id: TypeId,
    ) -> Result<Box<dyn CloneAny>> {
        let st = self.lock_state();
        let entry = st
            .services
            .get(&(scope, name.to_string()))
            .or_else(|| {
                (scope != GLOBAL)
                    .then(|| st.services.get(&(GLOBAL, name.to_string())))
                    .flatten()
            });
        let Some(entry) = entry else {
            return Err(Error::ServiceMissing(name.to_string()));
        };
        if entry.type_id != type_id {
            return Err(Error::ServiceTypeMismatch {
                name: name.to_string(),
                provided: entry.type_id,
                requested: type_id,
            });
        }
        Ok(entry.value.clone_box())
    }

    fn service_exists(&self, scope: ScopeId, name: &str) -> bool {
        self.lock_state().service_exists(scope, name)
    }

    fn add_plain_listener(
        &self,
        fiber: FiberId,
        name: &str,
        handler: PlainBox,
    ) -> Result<u64> {
        let mut st = self.lock_state();
        if st.shut_down {
            return Err(Error::Shutdown);
        }
        if !matches!(
            st.fibers.get(&fiber).map(|f| f.state),
            Some(FiberState::Loading) | Some(FiberState::Active)
        ) {
            return Err(Error::FiberNotLive(fiber));
        }
        let id = st.next_listener;
        st.next_listener += 1;
        st.routes
            .entry(name.to_string())
            .or_default()
            .plain
            .push(ListenerEntry {
                id,
                owner: fiber,
                handler,
            });
        Ok(id)
    }

    fn add_waterfall_listener(
        &self,
        fiber: FiberId,
        name: &str,
        handler: WaterfallBox,
        prepend: bool,
    ) -> Result<u64> {
        let mut st = self.lock_state();
        if st.shut_down {
            return Err(Error::Shutdown);
        }
        if !matches!(
            st.fibers.get(&fiber).map(|f| f.state),
            Some(FiberState::Loading) | Some(FiberState::Active)
        ) {
            return Err(Error::FiberNotLive(fiber));
        }
        let id = st.next_listener;
        st.next_listener += 1;
        let entry = ListenerEntry {
            id,
            owner: fiber,
            handler,
        };
        let route = st.routes.entry(name.to_string()).or_default();
        if prepend {
            route.waterfall.insert(0, entry);
        } else {
            route.waterfall.push(entry);
        }
        Ok(id)
    }

    fn remove_plain_listener(&self, name: &str, id: u64) {
        let mut st = self.lock_state();
        if let Some(route) = st.routes.get_mut(name) {
            route.plain.retain(|e| e.id != id);
            if route.is_empty() {
                st.routes.remove(name);
            }
        }
    }

    fn remove_waterfall_listener(&self, name: &str, id: u64) {
        let mut st = self.lock_state();
        if let Some(route) = st.routes.get_mut(name) {
            route.waterfall.retain(|e| e.id != id);
            if route.is_empty() {
                st.routes.remove(name);
            }
        }
    }

    fn snapshot_plain(&self, name: &str) -> Vec<(FiberId, ScopeId, PlainBox)> {
        let st = self.lock_state();
        st.routes
            .get(name)
            .map(|r| {
                r.plain
                    .iter()
                    .map(|e| {
                        let scope = st
                            .fibers
                            .get(&e.owner)
                            .map(|f| f.scope)
                            .unwrap_or(GLOBAL);
                        (e.owner, scope, e.handler.clone())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn snapshot_waterfall(&self, name: &str) -> Vec<WaterfallBox> {
        let st = self.lock_state();
        st.routes
            .get(name)
            .map(|r| r.waterfall.iter().map(|e| e.handler.clone()).collect())
            .unwrap_or_default()
    }

    fn notify_taps(&self, name: &str, payload: &Value) {
        AppInner::notify_taps(self, name, payload)
    }

    fn emit(&self, name: &str, payload: &Value) {
        self.notify_taps(name, payload);
        let entries = self.snapshot_plain(name);
        let host = self
            .self_weak
            .upgrade()
            .expect("AppInner dropped while still in use");
        for (owner, scope, handler) in entries {
            let ctx = crate::context::Context {
                host: host.clone() as Arc<dyn Host>,
                fiber: owner,
                scope,
            };
            let payload = payload.clone();
            #[cfg(not(target_arch = "wasm32"))]
            self.runtime.spawn(async move {
                handler(ctx, payload).await;
            });
            #[cfg(target_arch = "wasm32")]
            let _ = (ctx, handler, payload);
        }
    }

    fn add_tap(&self, fiber: FiberId, handler: TapBox) -> Result<u64> {
        let mut st = self.lock_state();
        if st.shut_down {
            return Err(Error::Shutdown);
        }
        if !matches!(
            st.fibers.get(&fiber).map(|f| f.state),
            Some(FiberState::Loading) | Some(FiberState::Active)
        ) {
            return Err(Error::FiberNotLive(fiber));
        }
        let id = st.next_listener;
        st.next_listener += 1;
        st.taps.push(ListenerEntry {
            id,
            owner: fiber,
            handler,
        });
        Ok(id)
    }

    fn remove_tap(&self, id: u64) {
        let mut st = self.lock_state();
        st.taps.retain(|t| t.id != id);
    }

    async fn mount_child(
        &self,
        parent: FiberId,
        spec: crate::plugin::MountSpec,
    ) -> Result<FiberId> {
        self.mount_fiber(Some(parent), spec).await
    }

    fn fiber_is_live(&self, fiber: FiberId) -> bool {
        matches!(
            self.lock_state().fibers.get(&fiber).map(|f| f.state),
            Some(FiberState::Loading) | Some(FiberState::Active)
        )
    }

    fn is_shut_down(&self) -> bool {
        self.lock_state().shut_down
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn runtime(&self) -> &tokio::runtime::Handle {
        &self.runtime
    }

    async fn wake_pending(&self) {
        AppInner::wake_pending(self).await
    }
}
