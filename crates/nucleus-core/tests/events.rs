//! Event dispatch semantics: emit / parallel / serial short-circuit /
//! waterfall transform & veto / prepend, plus scoped services.

use nucleus_core::{App, Context, DispatchMode, Event, TypedNext, WaterfallEvent};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Clone)]
struct StatsReport {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    count: u64,
}
impl Event for StatsReport {
    const NAME: &'static str = "stats/report";
    const MODE: DispatchMode = DispatchMode::Emit;
}

#[derive(Serialize, Deserialize, Clone)]
struct Decide {
    question: String,
}
impl Event for Decide {
    const NAME: &'static str = "policy/decide";
    const MODE: DispatchMode = DispatchMode::Serial;
}

struct Transform;
impl WaterfallEvent for Transform {
    type Args = String;
    type Output = String;
    const NAME: &'static str = "demo/transform";
}

#[tokio::test]
async fn emit_parallel_and_listener_cleanup() {
    let app = App::new();
    let ctx = app.context();

    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    ctx.on::<StatsReport, _, _>(move |_ctx, _ev| {
        let hits = h.clone();
        async move {
            hits.fetch_add(1, Ordering::SeqCst);
        }
    })
    .unwrap();

    ctx.emit(&StatsReport { name: "a".into(), count: 1 }).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1, "emit reaches the listener");

    ctx.parallel(&StatsReport { name: "b".into(), count: 2 })
        .await
        .unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 2, "parallel awaits listeners");

    // effects unwind with the app (fiber 0): after shutdown, no listeners.
    app.shutdown().await.unwrap();
    ctx.emit(&StatsReport { name: "c".into(), count: 3 }).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 2, "listener removed on shutdown");
}

#[tokio::test]
async fn serial_first_truthy_short_circuits() {
    let app = App::new();
    let ctx = app.context();

    ctx.on_serial::<Decide, String, _, _>(|_ctx, ev| async move {
        if ev.question.contains("blocked") {
            Some("** blocked **".to_string())
        } else {
            None
        }
    })
    .unwrap();

    let later = Arc::new(AtomicUsize::new(0));
    let l = later.clone();
    ctx.on_serial::<Decide, String, _, _>(move |_ctx, _ev| {
        let later = l.clone();
        async move {
            later.fetch_add(1, Ordering::SeqCst);
            Some("fallback".to_string())
        }
    })
    .unwrap();

    let v: Option<String> = ctx
        .serial(&Decide { question: "blocked words".into() })
        .await
        .unwrap();
    assert_eq!(v.as_deref(), Some("** blocked **"));
    assert_eq!(later.load(Ordering::SeqCst), 0, "vetoed chain stops early");

    let v: Option<String> = ctx
        .serial(&Decide { question: "fine".into() })
        .await
        .unwrap();
    assert_eq!(v.as_deref(), Some("fallback"));
    assert_eq!(later.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn waterfall_transform_and_veto() {
    let app = App::new();
    let ctx = app.context();

    // transform listener: uppercases whatever the rest of the chain returns
    ctx.on_waterfall::<Transform, _, _>(
        |_ctx, _input, next: TypedNext<String, String>| async move {
            let downstream = next.run().await?;
            Ok(downstream.to_uppercase())
        },
    )
    .unwrap();

    // owner listener: vetoes on "blocked", otherwise delegates
    ctx.on_waterfall::<Transform, _, _>(
        |_ctx, input: String, next: TypedNext<String, String>| async move {
            if input.contains("blocked") {
                return Ok("** blocked **".to_string());
            }
            next.run().await
        },
    )
    .unwrap();

    // pass-through: transform wraps the default
    let out: String = ctx
        .waterfall::<Transform, _, _>("hello".to_string(), |args| async move { Ok(args) })
        .await
        .unwrap();
    assert_eq!(out, "HELLO");

    // veto: default never runs, transform still wraps the replacement
    let out: String = ctx
        .waterfall::<Transform, _, _>("blocked words".to_string(), |args| async move { Ok(args) })
        .await
        .unwrap();
    assert_eq!(out, "** BLOCKED **");
}

#[tokio::test]
async fn waterfall_prepend_runs_before_ordinary() {
    struct Echo;
    impl WaterfallEvent for Echo {
        type Args = String;
        type Output = String;
        const NAME: &'static str = "echo/chain";
    }

    let app = App::new();
    let ctx = app.context();

    // ordinary listener appends "-suffix"
    ctx.on_waterfall::<Echo, _, _>(|_ctx, _i, next: TypedNext<String, String>| async move {
        let out = next.run().await?;
        Ok(format!("{out}-suffix"))
    })
    .unwrap();

    // prepended listener runs before the ordinary one
    ctx.on_waterfall_prepend::<Echo, _, _>(
        |_ctx, _i, next: TypedNext<String, String>| async move {
            let out = next.run().await?;
            Ok(format!("{out}-prefix"))
        },
    )
    .unwrap();

    let out: String = ctx
        .waterfall::<Echo, _, _>("x".to_string(), |args| async move { Ok(args) })
        .await
        .unwrap();
    assert_eq!(out, "x-suffix-prefix");
}

trait Greeter: Send + Sync {
    fn greet(&self, who: &str) -> String;
}
struct GlobalGreeter;
impl Greeter for GlobalGreeter {
    fn greet(&self, who: &str) -> String {
        format!("global:{who}")
    }
}
struct ScopedGreeter;
impl Greeter for ScopedGreeter {
    fn greet(&self, who: &str) -> String {
        format!("scoped:{who}")
    }
}

#[tokio::test]
async fn scoped_services_shadow_and_fall_back() {
    let app = App::new();

    let ctx_global = app.context();
    ctx_global
        .provide::<dyn Greeter>("greeter", Arc::new(GlobalGreeter))
        .await
        .unwrap();

    let scope = app.new_scope();
    let scoped = app.context_with_scope(scope);
    assert_eq!(
        scoped.service::<dyn Greeter>("greeter").unwrap().greet("x"),
        "global:x",
        "scope falls back to global when it has no own provider"
    );

    scoped
        .provide::<dyn Greeter>("greeter", Arc::new(ScopedGreeter))
        .await
        .unwrap();
    assert_eq!(
        scoped.service::<dyn Greeter>("greeter").unwrap().greet("x"),
        "scoped:x",
        "own scope shadows global"
    );
    assert_eq!(
        ctx_global.service::<dyn Greeter>("greeter").unwrap().greet("x"),
        "global:x",
        "global namespace untouched"
    );

    let infos = app.services();
    assert_eq!(infos.len(), 2);
}

#[tokio::test]
async fn type_mismatch_is_reported() {
    let app = App::new();
    let ctx = app.context();
    ctx.provide::<dyn Greeter>("greeter", Arc::new(GlobalGreeter))
        .await
        .unwrap();

    let err = match Context::service::<dyn StatsReportSender>(&ctx, "greeter") {
        Err(e) => e,
        Ok(_) => panic!("expected type mismatch"),
    };
    assert!(matches!(
        err,
        nucleus_core::Error::ServiceTypeMismatch { .. }
    ));
    let missing = match Context::service::<dyn Greeter>(&ctx, "absent") {
        Err(e) => e,
        Ok(_) => panic!("expected missing service"),
    };
    assert!(matches!(missing, nucleus_core::Error::ServiceMissing(_)));
}

trait StatsReportSender: Send + Sync {}
