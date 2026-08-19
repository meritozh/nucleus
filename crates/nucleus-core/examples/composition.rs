//! End-to-end demo of the two composability dimensions:
//!
//! 1. temporal — dispose a fiber, watch everything unwind;
//! 2. spatial — swap a service provider, watch dependents restart against
//!    the new implementation.
//!
//! Run: `cargo run -p nucleus-core --example composition`

use nucleus_core::{App, Context, MountSpec, Plugin};
use serde_json::json;
use std::sync::Arc;

trait Greeter: Send + Sync {
    fn greet(&self, who: &str) -> String;
}

struct GreeterV1;
impl Greeter for GreeterV1 {
    fn greet(&self, who: &str) -> String {
        format!("hello from v1, {who}")
    }
}

struct GreeterV2;
impl Greeter for GreeterV2 {
    fn greet(&self, who: &str) -> String {
        format!("HELLO FROM V2, {who}")
    }
}

struct GreeterPlugin {
    v2: bool,
}

#[async_trait::async_trait]
impl Plugin for GreeterPlugin {
    fn name(&self) -> &str {
        "greeter"
    }
    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        let svc: Arc<dyn Greeter> = if self.v2 { Arc::new(GreeterV2) } else { Arc::new(GreeterV1) };
        ctx.provide::<dyn Greeter>("greeter", svc).await
    }
}

struct ConsumerPlugin;

#[async_trait::async_trait]
impl Plugin for ConsumerPlugin {
    fn name(&self) -> &str {
        "consumer"
    }
    fn inject(&self) -> &[String] {
        static LIST: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
        LIST.get_or_init(|| vec!["greeter".to_string()])
    }
    async fn apply(&self, ctx: Context) -> nucleus_core::Result<()> {
        let greeter = ctx.service::<dyn Greeter>("greeter")?;
        println!("[consumer] mounted, service says: {}", greeter.greet("world"));
        // an external resource, released on unload
        ctx.effect_fn(|| async { println!("[consumer] external resource released"); });
        Ok(())
    }
}

#[tokio::main]
async fn main() -> nucleus_core::Result<()> {
    let app = App::new().with_static(nucleus_core::static_source! {
        "greeter" => |cfg| {
            let v2 = cfg.get("variant").and_then(|v| v.as_str()) == Some("v2");
            Ok(Arc::new(GreeterPlugin { v2 }))
        },
        "consumer" => |_cfg| Ok(Arc::new(ConsumerPlugin)),
    });

    // mount the consumer BEFORE its dependency: it waits as PENDING
    let consumer = app.mount(MountSpec::new("consumer")).await?;
    println!("[app] consumer state: {}", consumer.state());

    // provider appears — dependency-driven activation
    let v1 = app
        .mount(MountSpec::new("greeter").with_config(json!({"variant": "v1"})))
        .await?;
    println!("[app] after provider mounts, consumer state: {}", consumer.state());

    // temporal composability: remove the provider
    v1.dispose().await?;
    println!("[app] provider removed, consumer back to: {}", consumer.state());
    println!("[app] diagnostics: {:?}", consumer.info().unwrap().missing);

    // spatial composability: a different implementation mounts, dependent restarts
    let v2 = app
        .mount(MountSpec::new("greeter").with_config(json!({"variant": "v2"})))
        .await?;
    println!("[app] new provider mounted, consumer state: {}", consumer.state());

    // teardown everything (reverse mount order, all effects unwound)
    v2.dispose().await?;
    let consumer_id = consumer.id();
    consumer.dispose().await?;
    println!(
        "[app] remaining fibers: {:?}",
        app.fibers().iter().map(|f| f.name.clone()).collect::<Vec<_>>()
    );
    assert!(app.fiber(consumer_id).is_none(), "consumer fully disposed");
    Ok(())
}
