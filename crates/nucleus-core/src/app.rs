//! App: composition root. Owns the fiber registry and orchestrates
//! mount / unload / cascade-reload.

use crate::context::Context;
use crate::error::{DisposeFailure, Error, Result};
use crate::fiber::{FiberHandle, FiberId, FiberInfo, FiberState, ScopeId, GLOBAL};
#[cfg(not(target_arch = "wasm32"))]
use crate::plugin::Plugin;
use crate::plugin::{MountSpec, PluginSource, StaticSource};
use crate::state::AppInner;
#[cfg(not(target_arch = "wasm32"))]
use crate::state::{AppState, FiberData};
#[cfg(not(target_arch = "wasm32"))]
use serde_json::Value;
#[cfg(not(target_arch = "wasm32"))]
use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Mutex;
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
#[cfg(not(target_arch = "wasm32"))]
struct RootPlugin;

#[cfg(not(target_arch = "wasm32"))]
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
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new() -> Self {
        Self::with_runtime(tokio::runtime::Handle::current())
    }

    #[cfg(not(target_arch = "wasm32"))]
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
                activation: 0,
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
        let inner = Arc::new_cyclic(|weak| AppInner {
            state: Mutex::new(state),
            runtime,
            waking: AtomicBool::new(false),
            self_weak: weak.clone(),
        });
        Self { inner }
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
            host: self.inner.clone() as Arc<dyn crate::state::Host>,
            fiber: 0,
            scope: GLOBAL,
            activation: 0,
        }
    }

    /// Host-level context bound to a service scope (for isolated groups
    /// driven programmatically rather than through the loader).
    pub fn context_with_scope(&self, scope: ScopeId) -> Context {
        Context {
            host: self.inner.clone() as Arc<dyn crate::state::Host>,
            fiber: 0,
            scope,
            activation: 0,
        }
    }

    /// Mount a root-level plugin.
    pub async fn mount(&self, spec: MountSpec) -> Result<FiberHandle> {
        let id = self.inner.mount_fiber(None, None, spec).await?;
        Ok(FiberHandle {
            app: self.inner.clone(),
            id,
        })
    }

    /// Mount a complete required plugin set.
    ///
    /// Pending or failed fibers make the batch fail. Every fiber created by
    /// this call is then disposed in reverse order before the error is
    /// returned, so callers never publish a partially active application.
    pub async fn mount_batch<I>(&self, specs: I) -> Result<Vec<FiberHandle>>
    where
        I: IntoIterator<Item = MountSpec>,
    {
        let mut mounted = Vec::new();
        for spec in specs {
            match self.mount(spec).await {
                Ok(handle) => mounted.push(handle),
                Err(error) => {
                    let rollback = rollback_handles(&mut mounted).await;
                    return Err(Error::BatchMountFailed {
                        reason: error.to_string(),
                        rollback,
                    });
                }
            }
        }

        let inactive = mounted
            .iter()
            .filter_map(|handle| {
                let info = handle.info()?;
                (info.state != FiberState::Active).then_some(info)
            })
            .map(|info| {
                if let Some(error) = info.error {
                    format!("{} is {}: {error}", info.name, info.state)
                } else if info.missing.is_empty() {
                    format!("{} is {}", info.name, info.state)
                } else {
                    format!(
                        "{} is {} (missing: {})",
                        info.name,
                        info.state,
                        info.missing.join(", ")
                    )
                }
            })
            .collect::<Vec<_>>();
        if !inactive.is_empty() {
            let rollback = rollback_handles(&mut mounted).await;
            return Err(Error::BatchMountFailed {
                reason: format!("required plugins did not activate: {}", inactive.join("; ")),
                rollback,
            });
        }

        Ok(mounted)
    }

    pub fn fiber(&self, id: FiberId) -> Option<FiberHandle> {
        self.inner
            .lock_state()
            .fibers
            .contains_key(&id)
            .then(|| FiberHandle {
                app: self.inner.clone(),
                id,
            })
    }

    /// All fibers with diagnostics; PENDING fibers include their missing
    /// injects (the nucleus "diagnose" plugin as an API).
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
        let mut failures = Vec::new();
        for id in roots.into_iter().rev() {
            if let Err(error) = self.inner.unload_fiber(id, false).await {
                append_dispose_error(&mut failures, id, "app", error);
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(Error::DisposeFailures(failures))
        }
    }
}

async fn rollback_handles(handles: &mut Vec<FiberHandle>) -> Vec<DisposeFailure> {
    let mut failures = Vec::new();
    while let Some(handle) = handles.pop() {
        let id = handle.id();
        let name = handle.name().unwrap_or_else(|| "unknown".to_string());
        if let Err(error) = handle.dispose().await {
            append_dispose_error(&mut failures, id, &name, error);
        }
    }
    failures
}

fn append_dispose_error(
    failures: &mut Vec<DisposeFailure>,
    fiber_id: FiberId,
    plugin: &str,
    error: Error,
) {
    match error {
        Error::DisposeFailures(mut nested) => failures.append(&mut nested),
        other => failures.push(DisposeFailure {
            fiber_id,
            plugin: plugin.to_string(),
            message: other.to_string(),
        }),
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}
