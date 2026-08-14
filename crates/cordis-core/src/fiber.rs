//! Fiber: the runtime handle for one loaded plugin instance.

use crate::error::Result;
use crate::state::AppInner;
use std::fmt;
use std::sync::Arc;

pub type FiberId = u64;

/// Service scope identifier. `GLOBAL` is the default shared namespace;
/// isolated loader groups get their own scope so that equally named
/// services can coexist (cordis `isolate`).
pub type ScopeId = u64;
pub const GLOBAL: ScopeId = 0;

/// Mirrors the cordis fiber state machine:
/// `PENDING → LOADING → ACTIVE → UNLOADING → DISPOSED`, with `FAILED`
/// reachable from `LOADING` when `apply` (or config validation) errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FiberState {
    /// Declared, waiting on a required service (`inject`).
    Pending,
    /// `apply` is running.
    Loading,
    /// `apply` completed; effects are live.
    Active,
    /// `apply` errored; partial effects have been unwound.
    Failed,
    /// Disposers are running.
    Unloading,
    /// Fully torn down.
    Disposed,
}

impl FiberState {
    pub fn is_terminal(self) -> bool {
        matches!(self, FiberState::Disposed)
    }
}

impl fmt::Display for FiberState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            FiberState::Pending => "pending",
            FiberState::Loading => "loading",
            FiberState::Active => "active",
            FiberState::Failed => "failed",
            FiberState::Unloading => "unloading",
            FiberState::Disposed => "disposed",
        };
        f.write_str(s)
    }
}

/// Diagnostics snapshot of one fiber (cordis "diagnose PENDING" plugin,
/// promoted to a first-class API).
#[derive(Debug, Clone)]
pub struct FiberInfo {
    pub id: FiberId,
    pub row_id: Option<String>,
    pub name: String,
    pub source: String,
    pub config: serde_json::Value,
    pub state: FiberState,
    pub parent: Option<FiberId>,
    pub inject: Vec<String>,
    /// For `Pending` fibers: the inject names with no provider.
    pub missing: Vec<String>,
    pub error: Option<String>,
}

/// Handle to a mounted plugin instance. Cheap to clone.
#[derive(Clone)]
pub struct FiberHandle {
    pub(crate) app: Arc<AppInner>,
    pub(crate) id: FiberId,
}

impl FiberHandle {
    pub fn id(&self) -> FiberId {
        self.id
    }

    pub fn state(&self) -> FiberState {
        self.app
            .lock_state()
            .fibers
            .get(&self.id)
            .map(|f| f.state)
            .unwrap_or(FiberState::Disposed)
    }

    pub fn name(&self) -> Option<String> {
        self.app
            .lock_state()
            .fibers
            .get(&self.id)
            .map(|f| f.name.clone())
    }

    pub fn info(&self) -> Option<FiberInfo> {
        self.app.fiber_info(self.id)
    }

    /// Recursively unload this fiber and its children, unwinding every
    /// registered effect in reverse order.
    pub async fn dispose(self) -> Result<()> {
        self.app.unload_fiber(self.id, false).await
    }
}

impl fmt::Debug for FiberHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FiberHandle")
            .field("id", &self.id)
            .field("state", &self.state())
            .finish()
    }
}
