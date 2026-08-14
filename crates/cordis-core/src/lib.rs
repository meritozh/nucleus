//! # cordis-core
//!
//! Spatiotemporal composability runtime — a Rust port of the cordis
//! paradigm (as vendored in deepseek-harness). See `docs/design.md` in the
//! repository root for the full design note.
//!
//! Three ideas, one per dimension:
//!
//! - **Temporal composability** — every registration through [`Context`] is
//!   a reversible *effect*: listeners, services, child plugins and external
//!   resources are unwound in reverse order when the owning fiber unloads.
//! - **Spatial composability** — plugins declare service *coeffects*
//!   (`inject`); fibers stay `Pending` until providers exist, and dependents
//!   are unloaded and re-mounted when a service disappears and returns.
//! - **Unified context** — one [`Context`] type carries both: effects are
//!   registered through it, and services are located through it.
//!
//! ```
//! # async fn demo() -> cordis_core::Result<()> {
//! use std::sync::Arc;
//! use cordis_core::{App, Context, Plugin, MountSpec};
//! use async_trait::async_trait;
//!
//! struct Greeter;
//! #[async_trait]
//! impl Plugin for Greeter {
//!     fn name(&self) -> &str { "greeter" }
//!     async fn apply(&self, ctx: Context) -> cordis_core::Result<()> {
//!         ctx.effect_fn(|| async { println!("greeter cleaned up") });
//!         Ok(())
//!     }
//! }
//!
//! let app = App::new().with_static(
//!     cordis_core::static_source! { "greeter" => |_cfg| Ok(Arc::new(Greeter)) },
//! );
//! let fiber = app.mount(MountSpec::new("greeter")).await?;
//! fiber.dispose().await?; // runs the disposer above
//! # Ok(())
//! # }
//! ```

pub mod app;
pub mod context;
pub mod error;
pub mod events;
pub mod fiber;
pub mod plugin;
pub(crate) mod state;

pub use app::{App, ServiceInfo};
pub use context::Context;
pub use error::{Error, Result};
pub use events::{DispatchMode, Event, Next, TypedNext, WaterfallEvent};
pub use fiber::{FiberHandle, FiberId, FiberInfo, FiberState, ScopeId, GLOBAL};
pub use plugin::{MountSpec, Plugin, PluginSource, StaticSource};

pub mod prelude {
    pub use crate::app::App;
    pub use crate::context::Context;
    pub use crate::error::{Error, Result};
    pub use crate::events::{Event, WaterfallEvent};
    pub use crate::fiber::{FiberHandle, FiberState};
    pub use crate::plugin::{MountSpec, Plugin, PluginSource, StaticSource};
}
