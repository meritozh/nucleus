//! App: composition root. Owns the fiber registry and orchestrates
//! mount / unload / cascade-reload.

use crate::context::Context;
use crate::error::Result;
use crate::fiber::{FiberHandle, FiberId, FiberInfo, FiberState, ScopeId, GLOBAL};
use crate::plugin::{MountSpec, Plugin, PluginSource, StaticSource};
use crate::state::{AppInner, AppState, FiberData};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Diagnostic view of one registered service.
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub name: String,
    pub scope: ScopeId,
    pub provider: FiberId,
    pub type_name: &'static str,
}

#[derive(Clone)]
pub struct App {
    pub(crate) inner: Arc<AppInner>,
}

/// Placeholder plugin backing the app's root fiber (fiber 0), so host-side
/// registrations through `App::context()` are also reversible effects that
/// unwind on shutdown.
struct RootPlugin;

#[async_trait::async_trait]
impl Plugin for RootPlugin {
    fn name(&self) -> &str {
        "app"
    }
    async fn apply(&self, _ctx: Context) -> Result<()> {
        Ok(())
    }
}

impl App {
    /// Must be called inside a tokio runtime (fire-and-forget event dispatch
    /// and unload timeouts are spawned on it).
    pub fn new() -> Self {
        Self::with_runtime(tokio::runtime::Handle::current())
    }

    pub fn with_runtime(runtime: tokio::runtime::Handle) -> Self {
        let mut state = AppState {
            fibers: HashMap::new(),
            services: HashMap::new(),
            routes: HashMap::new(),
            taps: Vec::new(),
            sources: HashMap::new(),
            next_fiber: 1,
            next_listener: 0,
            next_scope: 1,
            roots: Vec::new(),
            shut_down: false,
            unload_timeout: Duration::from_secs(5),
        };
        state.fibers.insert(
            0,
            FiberData {
                id: 0,
                row_id: None,
                name: "app".to_string(),
                source: "-".to_string(),
                config: Value::Null,
                plugin: Arc::new(RootPlugin),
                state: FiberState::Active,
                parent: None,
                children: Vec::new(),
                inject: Vec::new(),
                scope: GLOBAL,
                disposers: Vec::new(),
                provides: Vec::new(),
                error: None,
            },
        );
        state.roots.push(0);
        Self {
            inner: Arc::new(AppInner {
                state: Mutex::new(state),
                runtime,
                waking: AtomicBool::new(false),
            }),
        }
    }

    /// Register a plugin source under a name (`"static"`, `"wasm"`, ...).
    pub fn with_source(self, name: impl AsRef<str>, source: Arc<dyn PluginSource>) -> Self {
        self.inner
            .lock_state()
            .sources
            .insert(name.as_ref().to_string(), source);
        self
    }

    /// Convenience: register a [`StaticSource`] under the default name.
    pub fn with_static(self, source: StaticSource) -> Self {
        self.with_source("static", Arc::new(source))
    }

    /// Bound on the whole disposal phase of one fiber (default 5s).
    pub fn unload_timeout(self, timeout: Duration) -> Self {
        self.inner.lock_state().unload_timeout = timeout;
        self
    }

    /// Host-level context (fiber 0). Its effects unwind on `shutdown`.
    pub fn context(&self) -> Context {
        Context {
            app: self.inner.clone(),
            fiber: 0,
            scope: GLOBAL,
        }
    }

    /// Host-level context bound to a service scope (for isolated groups
    /// driven programmatically rather than through the loader).
    pub fn context_with_scope(&self, scope: ScopeId) -> Context {
        Context {
            app: self.inner.clone(),
            fiber: 0,
            scope,
        }
    }

    /// Mount a root-level plugin.
    pub async fn mount(&self, spec: MountSpec) -> Result<FiberHandle> {
        let id = self.inner.mount_fiber(None, spec).await?;
        Ok(FiberHandle {
            app: self.inner.clone(),
            id,
        })
    }

    pub fn fiber(&self, id: FiberId) -> Option<FiberHandle> {
        self.inner.lock_state().fibers.contains_key(&id).then(|| FiberHandle {
            app: self.inner.clone(),
            id,
        })
    }

    /// All fibers with diagnostics; PENDING fibers include their missing
    /// injects (the cordis "diagnose" plugin as an API).
    pub fn fibers(&self) -> Vec<FiberInfo> {
        let mut infos: Vec<FiberInfo> = {
            let st = self.inner.lock_state();
            st.fibers.keys().copied().collect::<Vec<_>>()
        }
        .into_iter()
        .filter_map(|id| self.inner.fiber_info(id))
        .collect();
        infos.sort_by_key(|i| i.id);
        infos
    }

    pub fn services(&self) -> Vec<ServiceInfo> {
        let st = self.inner.lock_state();
        let mut out: Vec<ServiceInfo> = st
            .services
            .iter()
            .map(|((scope, name), e)| ServiceInfo {
                name: name.clone(),
                scope: *scope,
                provider: e.provider,
                type_name: e.type_name,
            })
            .collect();
        out.sort_by(|a, b| (a.scope, &a.name).cmp(&(b.scope, &b.name)));
        out
    }

    /// Allocate a fresh service scope (used by isolated loader groups).
    pub fn new_scope(&self) -> ScopeId {
        self.inner.lock_state().alloc_scope()
    }

    /// Unload every root fiber in reverse mount order (including the app
    /// fiber 0), unwinding all effects.
    pub async fn shutdown(&self) -> Result<()> {
        let roots: Vec<FiberId> = {
            let mut st = self.inner.lock_state();
            st.shut_down = true;
            st.roots.clone()
        };
        for id in roots.into_iter().rev() {
            self.inner.unload_fiber(id, false).await?;
        }
        Ok(())
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}
