use std::any::TypeId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisposeFailure {
    pub fiber_id: u64,
    pub plugin: String,
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("service `{0}` already provided in this scope")]
    ServiceExists(String),

    #[error("service `{0}` not available (no provider mounted)")]
    ServiceMissing(String),

    #[error("service `{name}` type mismatch (provided {provided:?}, requested {requested:?})")]
    ServiceTypeMismatch {
        name: String,
        provided: TypeId,
        requested: TypeId,
    },

    #[error("plugin source `{0}` is not registered")]
    SourceUnknown(String),

    #[error("plugin `{0}` not found in source")]
    PluginNotFound(String),

    #[error("event `{0}` has no listeners registered (waterfall requires a default)")]
    NoListeners(String),

    #[error("plugin `{0}` failed to apply: {1}")]
    ApplyFailed(String, String),

    #[error("unload of fiber `{0}` timed out after {1:?}; state may be partially torn down")]
    UnloadTimeout(String, std::time::Duration),

    #[error("one or more fiber disposers failed: {0:?}")]
    DisposeFailures(Vec<DisposeFailure>),

    #[error("batch mount failed: {reason}; rollback failures: {rollback:?}")]
    BatchMountFailed {
        reason: String,
        rollback: Vec<DisposeFailure>,
    },

    #[error("waterfall handler failed: {0}")]
    Waterfall(String),

    #[error("payload serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("app has been shut down")]
    Shutdown,

    #[error("owning fiber {0} is no longer live; registration rejected")]
    FiberNotLive(u64),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
