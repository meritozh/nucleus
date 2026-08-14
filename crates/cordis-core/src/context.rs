//! Context: the per-fiber facade every plugin receives.

use crate::error::{Error, Result};
use crate::events::{DefaultBox, Event, Next, PlainBox, TypedNext, WaterfallBox, WaterfallEvent};
use crate::fiber::{FiberHandle, ScopeId, GLOBAL};
use crate::plugin::MountSpec;
use crate::state::AppInner;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::any::TypeId;
use std::future::Future;
use std::sync::Arc;

/// Scope handle bound to one fiber. All registrations through it are
/// reversible effects owned by that fiber.
#[derive(Clone)]
pub struct Context {
    pub(crate) app: Arc<AppInner>,
    pub(crate) fiber: crate::fiber::FiberId,
    pub(crate) scope: ScopeId,
}

impl Context {
    /// The application this context belongs to.
    pub fn app(&self) -> crate::app::App {
        crate::app::App {
            inner: self.app.clone(),
        }
    }

    pub fn fiber_id(&self) -> crate::fiber::FiberId {
        self.fiber
    }

    pub fn scope(&self) -> ScopeId {
        self.scope
    }

    // ---- effects ----------------------------------------------------------

    /// Register an external resource: run `acquire` now; the returned
    /// disposer closure runs at unload. This is cordis'
    /// `ctx.effect(() => { ...acquire...; return () => cleanup })`.
    pub async fn effect<F, D, DFut>(&self, acquire: F)
    where
        F: Future<Output = D>,
        D: FnOnce() -> DFut + Send + 'static,
        DFut: Future<Output = ()> + Send + 'static,
    {
        let disposer = acquire.await;
        self.push_disposer(Box::new(move || Box::pin(disposer())));
    }

    /// `effect` without an acquisition step: just hand over the disposer.
    pub fn effect_fn<D, DFut>(&self, disposer: D)
    where
        D: FnOnce() -> DFut + Send + 'static,
        DFut: Future<Output = ()> + Send + 'static,
    {
        self.push_disposer(Box::new(move || Box::pin(disposer())));
    }

    pub(crate) fn push_disposer(&self, disposer: crate::state::Disposer) {
        let mut st = self.app.lock_state();
        match st.fibers.get_mut(&self.fiber) {
            Some(f) if matches!(f.state, crate::fiber::FiberState::Loading | crate::fiber::FiberState::Active) => {
                f.disposers.push(disposer);
            }
            _ => {
                tracing::warn!(
                    fiber = self.fiber,
                    "effect registered on a non-live fiber; dropping"
                );
            }
        }
    }

    // ---- services ---------------------------------------------------------

    /// Provide a named service in this context's scope. Providing is an
    /// effect: unloading this fiber removes the service and cascades to
    /// dependents. `T` may be a trait object (`Arc<dyn Greeter>`).
    pub async fn provide<T>(&self, name: &str, svc: Arc<T>) -> Result<()>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        {
            let mut st = self.app.lock_state();
            if st.shut_down {
                return Err(Error::Shutdown);
            }
            // Providing from a fiber that was unloaded mid-apply would
            // insert a service nothing can ever remove (ghost provider).
            if !matches!(
                st.fibers.get(&self.fiber).map(|f| f.state),
                Some(crate::fiber::FiberState::Loading) | Some(crate::fiber::FiberState::Active)
            ) {
                return Err(Error::FiberNotLive(self.fiber));
            }
            let key = (self.scope, name.to_string());
            if st.services.contains_key(&key) {
                return Err(Error::ServiceExists(name.to_string()));
            }
            st.services.insert(
                key,
                crate::state::ServiceEntry {
                    value: Box::new(svc),
                    type_id: TypeId::of::<T>(),
                    type_name: std::any::type_name::<T>(),
                    provider: self.fiber,
                },
            );
            if let Some(f) = st.fibers.get_mut(&self.fiber) {
                f.provides.push(name.to_string());
            }
        }
        self.app.wake_pending().await;
        Ok(())
    }

    /// Look up a required service. Inside `apply` with a matching `inject`
    /// declaration this cannot fail. Distinguishes "no provider" from
    /// "registered under a different type".
    pub fn service<T>(&self, name: &str) -> Result<Arc<T>>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let st = self.app.lock_state();
        let entry = st
            .services
            .get(&(self.scope, name.to_string()))
            .or_else(|| {
                (self.scope != GLOBAL)
                    .then(|| st.services.get(&(GLOBAL, name.to_string())))
                    .flatten()
            });
        let Some(entry) = entry else {
            return Err(Error::ServiceMissing(name.to_string()));
        };
        if entry.type_id != TypeId::of::<T>() {
            return Err(Error::ServiceTypeMismatch {
                name: name.to_string(),
                provided: entry.type_id,
                requested: TypeId::of::<T>(),
            });
        }
        entry
            .value
            .downcast_ref::<Arc<T>>()
            .cloned()
            .ok_or_else(|| Error::ServiceMissing(name.to_string()))
    }

    /// Optional lookup (cordis `ctx.get`): `None` when absent (including
    /// type mismatch).
    pub fn try_service<T>(&self, name: &str) -> Option<Arc<T>>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.service::<T>(name).ok()
    }

    // ---- events -----------------------------------------------------------

    /// Register an `emit`/`parallel`/`serial` listener. Registration is an
    /// effect: it is removed when this fiber unloads.
    pub fn on<E, F, Fut>(&self, handler: F) -> Result<()>
    where
        E: Event,
        F: Fn(Context, E) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let plain: PlainBox = Arc::new(move |ctx, payload| {
            let handler = handler.clone();
            Box::pin(async move {
                match serde_json::from_value::<E>(payload) {
                    Ok(event) => handler(ctx, event).await,
                    Err(e) => tracing::warn!(event = E::NAME, error = %e, "listener payload decode failed"),
                }
                Value::Null
            })
        });
        self.add_plain_listener(E::NAME, plain)
    }

    /// Register a `serial` listener: `Some(value)` is the first truthy
    /// answer and short-circuits the chain; `None` passes.
    pub fn on_serial<E, O, F, Fut>(&self, handler: F) -> Result<()>
    where
        E: Event,
        O: Serialize + Send + Sync + 'static,
        F: Fn(Context, E) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<O>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let plain: PlainBox = Arc::new(move |ctx, payload| {
            let handler = handler.clone();
            Box::pin(async move {
                let answer = match serde_json::from_value::<E>(payload) {
                    Ok(event) => handler(ctx, event).await,
                    Err(e) => {
                        tracing::warn!(event = E::NAME, error = %e, "listener payload decode failed");
                        None
                    }
                };
                match answer {
                    Some(o) => serde_json::to_value(o).unwrap_or(Value::Null),
                    None => Value::Null,
                }
            })
        });
        self.add_plain_listener(E::NAME, plain)
    }

    /// Register a waterfall (around-middleware) listener. Observing
    /// listeners must call `next.run()`; returning without calling it is
    /// a veto.
    pub fn on_waterfall<E, F, Fut>(&self, handler: F) -> Result<()>
    where
        E: WaterfallEvent,
        F: Fn(Context, E::Args, TypedNext<E::Args, E::Output>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<E::Output>> + Send + 'static,
    {
        self.on_waterfall_opt::<E, F, Fut>(E::NAME, handler, false)
    }

    /// Waterfall listener that runs before ordinary registrations
    /// (cordis `prepend: true`).
    pub fn on_waterfall_prepend<E, F, Fut>(&self, handler: F) -> Result<()>
    where
        E: WaterfallEvent,
        F: Fn(Context, E::Args, TypedNext<E::Args, E::Output>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<E::Output>> + Send + 'static,
    {
        self.on_waterfall_opt::<E, F, Fut>(E::NAME, handler, true)
    }

    fn on_waterfall_opt<E, F, Fut>(&self, name: &'static str, handler: F, prepend: bool) -> Result<()>
    where
        E: WaterfallEvent,
        F: Fn(Context, E::Args, TypedNext<E::Args, E::Output>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<E::Output>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let boxed: WaterfallBox = Arc::new(move |ctx, payload, next| {
            let handler = handler.clone();
            Box::pin(async move {
                let args: E::Args = serde_json::from_value(payload)?;
                let out = handler(ctx, args, TypedNext::new(next)).await?;
                Ok(serde_json::to_value(out)?)
            })
        });
        self.add_waterfall_listener(name, boxed, prepend)
    }

    /// Fire-and-forget dispatch: listeners are spawned, return values
    /// ignored.
    pub fn emit<E: Event>(&self, event: &E) -> Result<()> {
        if E::MODE != crate::events::DispatchMode::Emit {
            tracing::debug!(event = E::NAME, declared = ?E::MODE, "dispatch mode differs from declaration");
        }
        self.emit_dynamic(E::NAME, serde_json::to_value(event)?)
    }

    /// Dynamic (untyped) fire-and-forget dispatch, for plugin sources whose
    /// guests only produce JSON (Wasm). Listeners decode the payload into
    /// their declared event type; listeners whose schema does not match
    /// the payload log and skip.
    pub fn emit_dynamic(&self, name: &str, payload: Value) -> Result<()> {
        self.app.notify_taps(name, &payload);
        let entries = self.snapshot_plain(name);
        for (owner, scope, handler) in entries {
            let ctx = Context {
                app: self.app.clone(),
                fiber: owner,
                scope,
            };
            let payload = payload.clone();
            self.app.runtime.spawn(async move {
                handler(ctx, payload).await;
            });
        }
        Ok(())
    }

    /// Await every listener concurrently.
    pub async fn parallel<E: Event>(&self, event: &E) -> Result<()> {
        if E::MODE != crate::events::DispatchMode::Parallel {
            // The declared mode is a documented contract, not an enforcement:
            // cordis allows mixed dispatch on one event.
            tracing::debug!(
                event = E::NAME,
                declared = ?E::MODE,
                "dispatch mode differs from declaration"
            );
        }
        let payload = serde_json::to_value(event)?;
        self.app.notify_taps(E::NAME, &payload);
        let entries = self.snapshot_plain(E::NAME);
        let mut futs = Vec::with_capacity(entries.len());
        for (owner, scope, handler) in entries {
            let ctx = Context {
                app: self.app.clone(),
                fiber: owner,
                scope,
            };
            let payload = payload.clone();
            futs.push(handler(ctx, payload));
        }
        futures::future::join_all(futs).await;
        Ok(())
    }

    /// Run listeners in registration order; the first non-null answer wins
    /// and short-circuits.
    pub async fn serial<E: Event, O: DeserializeOwned + Send + 'static>(
        &self,
        event: &E,
    ) -> Result<Option<O>> {
        if E::MODE != crate::events::DispatchMode::Serial {
            // The declared mode is a documented contract, not an enforcement:
            // cordis allows mixed dispatch on one event.
            tracing::debug!(
                event = E::NAME,
                declared = ?E::MODE,
                "dispatch mode differs from declaration"
            );
        }
        let payload = serde_json::to_value(event)?;
        self.app.notify_taps(E::NAME, &payload);
        let entries = self.snapshot_plain(E::NAME);
        for (owner, scope, handler) in entries {
            let ctx = Context {
                app: self.app.clone(),
                fiber: owner,
                scope,
            };
            let answer = handler(ctx, payload.clone()).await;
            if !answer.is_null() {
                return Ok(Some(serde_json::from_value(answer)?));
            }
        }
        Ok(None)
    }

    /// Waterfall dispatch with the dispatcher's default behavior. Runs the
    /// registered middleware chain around `default`.
    pub async fn waterfall<E, F, Fut>(&self, args: E::Args, default: F) -> Result<E::Output>
    where
        E: WaterfallEvent,
        F: Fn(E::Args) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<E::Output>> + Send + 'static,
    {
        let input = serde_json::to_value(args)?;
        self.app.notify_taps(E::NAME, &input);
        let chain = self.snapshot_waterfall(E::NAME);
        let default = Arc::new(default);
        let default_box: DefaultBox = Arc::new(move |input| {
            let default = default.clone();
            Box::pin(async move {
                let args: E::Args = serde_json::from_value(input)?;
                let out = default(args).await?;
                Ok(serde_json::to_value(out)?)
            })
        });
        let next = Next {
            ctx: self.clone(),
            rest: chain,
            default: default_box,
            input,
        };
        let out = next.run().await?;
        Ok(serde_json::from_value(out)?)
    }

    /// Observe every dispatched event (name + payload), across all event
    /// types. Registration is an effect owned by this fiber. Intended for
    /// bridges (Wasm host forwarding into guest subscriptions), not for
    /// ordinary plugins — prefer typed `on`.
    pub fn on_any<F>(&self, tap: F) -> Result<()>
    where
        F: Fn(&str, &Value) + Send + Sync + 'static,
    {
        let app = self.app.clone();
        let lid = {
            let mut st = self.app.lock_state();
            if st.shut_down {
                return Err(Error::Shutdown);
            }
            if !matches!(
                st.fibers.get(&self.fiber).map(|f| f.state),
                Some(crate::fiber::FiberState::Loading) | Some(crate::fiber::FiberState::Active)
            ) {
                return Err(Error::FiberNotLive(self.fiber));
            }
            let id = st.next_listener;
            st.next_listener += 1;
            st.taps.push(crate::state::ListenerEntry {
                id,
                owner: self.fiber,
                handler: Arc::new(tap),
            });
            id
        };
        self.effect_fn(move || {
            Box::pin(async move {
                let mut st = app.lock_state();
                st.taps.retain(|t| t.id != lid);
            })
        });
        Ok(())
    }

    // ---- child plugins ----------------------------------------------------

    /// Mount a child plugin owned by this fiber: it unloads with the parent.
    pub async fn plugin(&self, spec: MountSpec) -> Result<FiberHandle> {
        let mut spec = spec;
        if self.scope != GLOBAL {
            spec.scope = self.scope;
        }
        let id = self.app.mount_fiber(Some(self.fiber), spec).await?;
        Ok(FiberHandle {
            app: self.app.clone(),
            id,
        })
    }

    // ---- internals ----------------------------------------------------------

    fn snapshot_plain(
        &self,
        name: &str,
    ) -> Vec<(crate::fiber::FiberId, crate::fiber::ScopeId, PlainBox)> {
        let st = self.app.lock_state();
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
        let st = self.app.lock_state();
        st.routes
            .get(name)
            .map(|r| r.waterfall.iter().map(|e| e.handler.clone()).collect())
            .unwrap_or_default()
    }

    fn add_plain_listener(&self, name: &'static str, handler: PlainBox) -> Result<()> {
        let app = self.app.clone();
        let lid = {
            let mut st = self.app.lock_state();
            if st.shut_down {
                return Err(Error::Shutdown);
            }
            if !matches!(
                st.fibers.get(&self.fiber).map(|f| f.state),
                Some(crate::fiber::FiberState::Loading) | Some(crate::fiber::FiberState::Active)
            ) {
                return Err(Error::FiberNotLive(self.fiber));
            }
            let id = st.next_listener;
            st.next_listener += 1;
            st.routes.entry(name.to_string()).or_default().plain.push(
                crate::state::ListenerEntry {
                    id,
                    owner: self.fiber,
                    handler,
                },
            );
            id
        };
        self.effect_fn(move || {
            let name = name.to_string();
            Box::pin(async move {
                let mut st = app.lock_state();
                if let Some(route) = st.routes.get_mut(&name) {
                    route.plain.retain(|e| e.id != lid);
                    if route.is_empty() {
                        st.routes.remove(&name);
                    }
                }
            })
        });
        Ok(())
    }

    fn add_waterfall_listener(
        &self,
        name: &'static str,
        handler: WaterfallBox,
        prepend: bool,
    ) -> Result<()> {
        let app = self.app.clone();
        let lid = {
            let mut st = self.app.lock_state();
            if st.shut_down {
                return Err(Error::Shutdown);
            }
            if !matches!(
                st.fibers.get(&self.fiber).map(|f| f.state),
                Some(crate::fiber::FiberState::Loading) | Some(crate::fiber::FiberState::Active)
            ) {
                return Err(Error::FiberNotLive(self.fiber));
            }
            let id = st.next_listener;
            st.next_listener += 1;
            let entry = crate::state::ListenerEntry {
                id,
                owner: self.fiber,
                handler,
            };
            let route = st.routes.entry(name.to_string()).or_default();
            if prepend {
                route.waterfall.insert(0, entry);
            } else {
                route.waterfall.push(entry);
            }
            id
        };
        self.effect_fn(move || {
            let name = name.to_string();
            Box::pin(async move {
                let mut st = app.lock_state();
                if let Some(route) = st.routes.get_mut(&name) {
                    route.waterfall.retain(|e| e.id != lid);
                    if route.is_empty() {
                        st.routes.remove(&name);
                    }
                }
            })
        });
        Ok(())
    }
}
