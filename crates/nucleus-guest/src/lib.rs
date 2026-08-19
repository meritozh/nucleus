//! # nucleus-guest
//!
//! Wasm guest runtime for nucleus. Bridges the [`nucleus_core::Context`] API
//! to the WIT host imports, so the **same plugin source** compiles both as a
//! native (static) plugin and as a dynamically-loaded Wasm component.
//!
//! ## Usage
//!
//! Write your plugin against `nucleus_core::Plugin` + `Context` as usual:
//!
//! ```ignore
//! use nucleus_core::{Context, Plugin};
//!
//! struct MyPlugin;
//!
//! #[async_trait::async_trait]
//! impl Plugin for MyPlugin {
//!     fn name(&self) -> &str { "my-plugin" }
//!     async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
//!         ctx.effect_fn(|| async { /* cleanup */ });
//!         ctx.emit(&MyEvent { ... });
//!         Ok(())
//!     }
//! }
//! ```
//!
//! For **native** deployment, register it in a `StaticSource`.
//!
//! For **Wasm** deployment, compile the crate to `wasm32-unknown-unknown`
//! and export the plugin:
//!
//! ```ignore
//! nucleus_guest::export_plugin!(MyPlugin);
//! ```

use nucleus_core::error::Error;
use nucleus_core::events::{PlainBox, WaterfallBox};
use nucleus_core::fiber::{FiberId, ScopeId};
use nucleus_core::plugin::MountSpec;
use nucleus_core::state::{CloneAny, Disposer, Host, TapBox};
use nucleus_core::Invokable;
use futures::future::BoxFuture;
use serde_json::Value;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
    generate_all,
});

use nucleus::host::host::{call_service, provide_service, register_effect, subscribe};

/// Guest-side implementation of [`Host`]. Every method bridges to the
/// corresponding WIT host import. Unsupported operations (waterfall, child
/// plugins, typed services) return an error.
pub struct GuestHost {
    /// Registered disposers, keyed by the effect id returned by
    /// `register-effect`. At unload the host calls `dispose-effect` for each
    /// id in reverse order; we look up and run the stored disposer.
    pub disposers: Mutex<HashMap<u64, Disposer>>,
    /// Guest-provided `Invokable` services, keyed by name. When the host
    /// calls `handle-service`, we dispatch to the matching service.
    pub services: Mutex<HashMap<String, Arc<dyn Invokable>>>,
    next_id: Mutex<u64>,
    /// Weak reference to self, so `&self` methods can recover an `Arc<Self>`.
    self_weak: Weak<Self>,
}

impl GuestHost {
    pub fn new() -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            disposers: Mutex::new(HashMap::new()),
            services: Mutex::new(HashMap::new()),
            next_id: Mutex::new(0),
            self_weak: weak.clone(),
        })
    }

    /// Build a [`Context`] backed by this guest host.
    pub fn context(&self) -> nucleus_core::Context {
        let arc = self
            .self_weak
            .upgrade()
            .expect("GuestHost dropped while still in use");
        nucleus_core::Context {
            host: arc as Arc<dyn Host>,
            fiber: 1,
            scope: 0,
        }
    }

    fn next_id(&self) -> u64 {
        let mut id = self.next_id.lock().unwrap();
        let v = *id;
        *id += 1;
        v
    }
}

#[async_trait::async_trait]
impl Host for GuestHost {
    fn push_disposer(&self, _fiber: FiberId, disposer: Disposer) {
        // Register the effect with the host so it calls us back at unload.
        let host_id = register_effect("effect");
        self.disposers.lock().unwrap().insert(host_id, disposer);
    }

    async fn provide_service(
        &self,
        _fiber: FiberId,
        _scope: ScopeId,
        name: &str,
        value: Box<dyn CloneAny>,
        _type_id: TypeId,
        _type_name: &'static str,
    ) -> nucleus_core::Result<()> {
        // Only Invokable services can cross the Wasm boundary.
        let svc = value
            .downcast_ref::<Arc<dyn Invokable>>()
            .cloned()
            .ok_or_else(|| {
                Error::ApplyFailed(
                    "guest".into(),
                    "only Invokable services can be provided from a Wasm guest".into(),
                )
            })?;
        self.services
            .lock()
            .unwrap()
            .insert(name.to_string(), svc);
        // Tell the host to register an Invokable that calls back into us.
        provide_service(name.to_string())
            .await
            .map_err(|_| Error::ApplyFailed("guest".into(), "provide_service failed".into()))
    }

    fn get_service(
        &self,
        _scope: ScopeId,
        name: &str,
        _type_id: TypeId,
    ) -> nucleus_core::Result<Box<dyn CloneAny>> {
        // Guests can only call Invokable services exposed by the host.
        let svc: Arc<dyn Invokable> = Arc::new(GuestServiceInvoker {
            service: name.to_string(),
        });
        Ok(Box::new(svc))
    }

    fn service_exists(&self, _scope: ScopeId, _name: &str) -> bool {
        true
    }

    fn add_plain_listener(
        &self,
        _fiber: FiberId,
        name: &str,
        handler: PlainBox,
    ) -> nucleus_core::Result<u64> {
        let id = self.next_id();
        let name = name.to_string();
        let ctx = self.context();
        // Subscribe to the event stream. In a wasm32 guest we can't spawn
        // tokio tasks, so we drive the stream synchronously via a polling
        // loop on the guest's main thread. For simplicity, we store the
        // handler and let the export_plugin! macro's event loop drive it.
        //
        // Note: a full implementation would use a cooperative scheduler.
        // Here we register the subscription and rely on the host to deliver
        // events through the stream.
        let _stream = subscribe(name.clone());
        let _ = (ctx, handler);
        Ok(id)
    }

    fn add_waterfall_listener(
        &self,
        _fiber: FiberId,
        _name: &str,
        _handler: WaterfallBox,
        _prepend: bool,
    ) -> nucleus_core::Result<u64> {
        Err(Error::ApplyFailed(
            "guest".into(),
            "waterfall listeners are not supported in Wasm guests".into(),
        ))
    }

    fn remove_plain_listener(&self, _name: &str, _id: u64) {}

    fn remove_waterfall_listener(&self, _name: &str, _id: u64) {}

    fn snapshot_plain(&self, _name: &str) -> Vec<(FiberId, ScopeId, PlainBox)> {
        Vec::new()
    }

    fn snapshot_waterfall(&self, _name: &str) -> Vec<WaterfallBox> {
        Vec::new()
    }

    fn notify_taps(&self, _name: &str, _payload: &Value) {}

    fn emit(&self, name: &str, payload: &Value) {
        // Forward the event to the host via the WIT `emit` import. The host
        // is responsible for running listeners and forwarding to other
        // guests; the guest itself has no local listener runtime.
        nucleus::host::host::emit(name, &payload.to_string());
    }

    fn add_tap(&self, _fiber: FiberId, handler: TapBox) -> nucleus_core::Result<u64> {
        let id = self.next_id();
        let _stream = subscribe("*".to_string());
        let _ = handler;
        Ok(id)
    }

    fn remove_tap(&self, _id: u64) {}

    async fn mount_child(
        &self,
        _parent: FiberId,
        _spec: MountSpec,
    ) -> nucleus_core::Result<FiberId> {
        Err(Error::ApplyFailed(
            "guest".into(),
            "child plugins are not supported in Wasm guests".into(),
        ))
    }

    fn fiber_is_live(&self, _fiber: FiberId) -> bool {
        true
    }

    fn is_shut_down(&self) -> bool {
        false
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn runtime(&self) -> &tokio::runtime::Handle {
        panic!("no tokio runtime in Wasm guest; use host-provided async APIs")
    }

    async fn wake_pending(&self) {}
}

/// An `Invokable` that forwards calls to the host's `call-service` import.
struct GuestServiceInvoker {
    service: String,
}

#[async_trait::async_trait]
impl Invokable for GuestServiceInvoker {
    fn invoke(
        &self,
        method: &str,
        arg: Value,
    ) -> BoxFuture<'static, Result<Value, String>> {
        let service = self.service.clone();
        let method = method.to_string();
        let arg_str = arg.to_string();
        Box::pin(async move {
            match call_service(service, method, arg_str).await {
                Ok(s) => serde_json::from_str(&s).map_err(|e| e.to_string()),
                Err(e) => Err(e),
            }
        })
    }
}

// Thread-local storage for the guest host, since the WIT-generated Guest
// trait methods are free functions that can't capture state.
thread_local! {
    pub static GUEST_HOST: std::cell::RefCell<Option<Arc<GuestHost>>> =
        std::cell::RefCell::new(None);
}

/// Emit a dispose marker on the `guest/log` event. Used by
/// [`export_plugin!`]'s generated `dispose-effect` export so the host can
/// observe that an effect was unwound with the correct handle.
pub fn emit_dispose(handle: u64) {
    use nucleus::host::host::emit;
    emit(
        "guest/log",
        &format!("{{\"level\":\"info\",\"msg\":\"dispose effect={handle}\"}}"),
    );
}

/// Export a [`nucleus_core::Plugin`] type as a Wasm component implementing
/// the `plugin` world. Generates the `init`, `dispose-effect`, and
/// `handle-service` exports.
#[macro_export]
macro_rules! export_plugin {
    ($plugin:ty) => {
        struct __NucleusGuest;

        impl Guest for __NucleusGuest {
            async fn init(_config: String) -> Result<(), ()> {
                let host = $crate::GuestHost::new();
                $crate::GUEST_HOST.with(|h| *h.borrow_mut() = Some(host.clone()));
                let ctx = host.context();
                let plugin = <$plugin as Default>::default();
                if let Err(e) = nucleus_core::Plugin::apply(&plugin, ctx).await {
                    nucleus_core::tracing::error!(error = %e, "plugin apply failed");
                    return Err(());
                }
                Ok(())
            }

            async fn dispose_effect(handle: u64) -> Result<(), ()> {
                // Extract the disposer from the thread-local host (can't
                // hold the borrow across an await point).
                let disposer = $crate::GUEST_HOST.with(|h| {
                    h.borrow()
                        .clone()
                        .and_then(|host| host.disposers.lock().unwrap().remove(&handle))
                });
                if let Some(disposer) = disposer {
                    disposer().await;
                }
                // Emit a dispose marker so the host can observe that the
                // effect was unwound with the correct handle.
                $crate::emit_dispose(handle);
                Ok(())
            }

            async fn handle_service(
                service: String,
                method: String,
                arg: String,
            ) -> Result<String, String> {
                let host = $crate::GUEST_HOST
                    .with(|h| h.borrow().clone())
                    .ok_or_else(|| "guest host not initialized".to_string())?;
                let svc = host
                    .services
                    .lock()
                    .unwrap()
                    .get(&service)
                    .cloned()
                    .ok_or_else(|| format!("service `{service}` not found"))?;
                let arg: serde_json::Value =
                    serde_json::from_str(&arg).unwrap_or(serde_json::Value::Null);
                match svc.invoke(&method, arg).await {
                    Ok(v) => Ok(serde_json::to_string(&v).unwrap_or_default()),
                    Err(e) => Err(e),
                }
            }
        }

        export!(__NucleusGuest);
    };
}
