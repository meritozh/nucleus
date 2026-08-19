//! Plugin definition and plugin sources.

use crate::context::Context;
use crate::error::{Error, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// A plugin: the unit of spatiotemporal composition. Mirrors nucleus'
/// `{ name, inject?, apply }` plugin objects.
#[async_trait::async_trait]
pub trait Plugin: Send + Sync {
    fn name(&self) -> &str;

    /// Hard service requirements. The fiber stays `Pending` until every
    /// named service exists; when one disappears, this fiber is unloaded
    /// and re-queued as `Pending` until it returns.
    fn inject(&self) -> &[String] {
        &[]
    }

    /// Acquire resources and register effects/listeners/services.
    /// Everything registered through `ctx` is reversible on unload.
    async fn apply(&self, ctx: Context) -> Result<()>;
}

/// Where plugin code comes from. Static (compile-time registry), Wasm
/// (runtime-loaded components), or future dylib/process sources.
#[async_trait::async_trait]
pub trait PluginSource: Send + Sync {
    async fn load(&self, name: &str, config: &Value) -> Result<Arc<dyn Plugin>>;
}

/// Host-side service contract for guest `call-service`: only services
/// registered under `Arc<dyn Invokable>` are guest-visible. JSON in/out.
/// This trait lives in nucleus-core so both the host (nucleus-wasm) and
/// guest (nucleus-guest) can implement it.
pub trait Invokable: Send + Sync {
    fn invoke(&self, method: &str, arg: Value) -> futures::future::BoxFuture<'static, Result<Value, String>>;
}

/// One config row to mount (nucleus `nucleus.yml` entry, minus `id`/`disabled`
/// bookkeeping which belongs to the loader).
#[derive(Debug, Clone)]
pub struct MountSpec {
    /// Stable identity used by the loader's reconciliation.
    pub row_id: Option<String>,
    /// Registered source name; `"static"` by default.
    pub source: String,
    /// Plugin name inside that source.
    pub name: String,
    pub config: Value,
    /// Service scope; `GLOBAL` unless an isolated group assigns one.
    pub scope: crate::fiber::ScopeId,
}

impl MountSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            row_id: None,
            source: "static".to_string(),
            name: name.into(),
            config: Value::Null,
            scope: crate::fiber::GLOBAL,
        }
    }

    pub fn with_config(mut self, config: Value) -> Self {
        self.config = config;
        self
    }

    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = source.into();
        self
    }

    pub fn with_row_id(mut self, row_id: impl Into<String>) -> Self {
        self.row_id = Some(row_id.into());
        self
    }
}

type Factory = Arc<dyn Fn(&Value) -> Result<Arc<dyn Plugin>> + Send + Sync>;

/// Compile-time plugin registry: the zero-overhead trusted path.
#[derive(Default)]
pub struct StaticSource {
    factories: HashMap<String, Factory>,
}

impl StaticSource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(mut self, name: impl Into<String>, factory: F) -> Self
    where
        F: Fn(&Value) -> Result<Arc<dyn Plugin>> + Send + Sync + 'static,
    {
        self.factories.insert(name.into(), Arc::new(factory));
        self
    }
}

#[async_trait::async_trait]
impl PluginSource for StaticSource {
    async fn load(&self, name: &str, config: &Value) -> Result<Arc<dyn Plugin>> {
        let factory = self
            .factories
            .get(name)
            .ok_or_else(|| Error::PluginNotFound(name.to_string()))?;
        factory(config)
    }
}

/// ```
/// # async fn demo() -> nucleus_core::Result<()> {
/// use std::sync::Arc;
/// use nucleus_core::{Context, Plugin, static_source};
/// use async_trait::async_trait;
///
/// struct Hello;
/// #[async_trait]
/// impl Plugin for Hello {
///     fn name(&self) -> &str { "hello" }
///     async fn apply(&self, _ctx: Context) -> nucleus_core::Result<()> { Ok(()) }
/// }
///
/// let source = static_source! {
///     "hello" => |_cfg| Ok(Arc::new(Hello)),
/// };
/// # let _ = source;
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! static_source {
    ( $( $name:literal => $factory:expr ),* $(,)? ) => {{
        let mut src = $crate::plugin::StaticSource::new();
        $( src = src.register($name, $factory); )*
        src
    }};
}
