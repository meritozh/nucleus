//! Loader reconciliation: diff by id, disabled rows, isolate groups, HMR.

use nucleus_core::{App, FiberState, Plugin};
use nucleus_loader::{diff, resolve, Loader, Op, Row};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct CountingPlugin {
    tag: &'static str,
    mounts: Arc<AtomicUsize>,
    provides: Option<&'static str>,
}

pub trait ShellService: Send + Sync {}

struct SharedTag;
impl ShellService for SharedTag {}

#[async_trait::async_trait]
impl Plugin for CountingPlugin {
    fn name(&self) -> &str {
        self.tag
    }
    async fn apply(&self, ctx: nucleus_core::Context) -> nucleus_core::Result<()> {
        self.mounts.fetch_add(1, Ordering::SeqCst);
        if let Some(svc) = self.provides {
            ctx.provide::<dyn ShellService>(svc, Arc::new(SharedTag))
                .await?;
        }
        Ok(())
    }
}

fn counting_app() -> (App, Arc<AtomicUsize>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));
    let s = Arc::new(AtomicUsize::new(0));
    let app = App::new().with_static(nucleus_core::static_source! {
        "alpha" => { let a = a.clone(); move |_cfg| Ok(Arc::new(CountingPlugin { tag: "alpha", mounts: a.clone(), provides: None })) },
        "beta"  => { let b = b.clone(); move |_cfg| Ok(Arc::new(CountingPlugin { tag: "beta",  mounts: b.clone(), provides: None })) },
        "shell" => { let s = s.clone(); move |_cfg| Ok(Arc::new(CountingPlugin { tag: "shell", mounts: s.clone(), provides: Some("shell") })) },
    });
    (app, a, b, s)
}

fn row(id: &str, name: &str) -> Row {
    Row {
        id: Some(id.to_string()),
        name: name.to_string(),
        source: "static".to_string(),
        config: serde_json::Value::Null,
        disabled: false,
        group: None,
    }
}

#[tokio::test]
async fn diff_by_id_only_touches_changed_rows() {
    let app = App::new();
    let prev = resolve(&[row("a", "alpha"), row("b", "beta")], &app, 0);
    let mut changed_b = row("b", "beta");
    changed_b.config = json!({ "x": 1 });
    let next = resolve(&[row("a", "alpha"), changed_b], &app, 0);
    let ops = diff(&prev, &next);
    assert_eq!(ops[0], Op::Noop("a".to_string()));
    assert_eq!(ops[1], Op::Reconfig("b".to_string()));

    let next2 = resolve(&[row("b", "beta")], &app, 0);
    let ops = diff(&prev, &next2);
    assert_eq!(ops[0], Op::Unmount("a".to_string()));
    assert_eq!(ops[1], Op::Noop("b".to_string()));
}

#[tokio::test]
async fn unchanged_rows_do_not_remount() {
    let (app, a, b, _) = counting_app();
    let mut loader = Loader::new(app);

    let report = loader
        .apply(vec![row("a", "alpha"), row("b", "beta")])
        .await
        .unwrap();
    assert_eq!(report.mounted, vec!["a", "b"]);
    assert_eq!(a.load(Ordering::SeqCst), 1);
    assert_eq!(b.load(Ordering::SeqCst), 1);

    // identical pass: nothing remounts
    let report = loader
        .apply(vec![row("a", "alpha"), row("b", "beta")])
        .await
        .unwrap();
    assert_eq!(
        report.summary(),
        "mounted=0 remounted=0 unmounted=0 unchanged=2 failed=0"
    );
    assert_eq!(a.load(Ordering::SeqCst), 1);
    assert_eq!(b.load(Ordering::SeqCst), 1);

    // one row's config changes: only that row remounts
    let mut changed = row("a", "alpha");
    changed.config = json!({"k": 1});
    let report = loader.apply(vec![changed, row("b", "beta")]).await.unwrap();
    assert_eq!(report.remounted, vec!["a"]);
    assert_eq!(a.load(Ordering::SeqCst), 2);
    assert_eq!(b.load(Ordering::SeqCst), 1);
}

struct FailingDisposePlugin {
    mounts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Plugin for FailingDisposePlugin {
    fn name(&self) -> &str {
        "failing-dispose"
    }

    async fn apply(&self, ctx: nucleus_core::Context) -> nucleus_core::Result<()> {
        self.mounts.fetch_add(1, Ordering::SeqCst);
        ctx.try_effect_fn(|| async { Err::<(), _>("planned dispose failure") });
        Ok(())
    }
}

#[tokio::test]
async fn reconfig_does_not_mount_after_dispose_failure() {
    let mounts = Arc::new(AtomicUsize::new(0));
    let app = App::new().with_static(nucleus_core::static_source! {
        "failing-dispose" => {
            let mounts = mounts.clone();
            move |_| Ok(Arc::new(FailingDisposePlugin { mounts: mounts.clone() }))
        },
    });
    let mut loader = Loader::new(app);

    loader
        .apply(vec![row("failing", "failing-dispose")])
        .await
        .unwrap();
    let mut changed = row("failing", "failing-dispose");
    changed.config = json!({"revision": 2});
    let report = loader.apply(vec![changed.clone()]).await.unwrap();

    assert_eq!(mounts.load(Ordering::SeqCst), 1);
    assert!(loader.fiber("failing").is_none());
    assert_eq!(report.remounted, Vec::<String>::new());
    assert_eq!(report.failed.len(), 1);

    let retry = loader.apply(vec![changed]).await.unwrap();
    assert_eq!(mounts.load(Ordering::SeqCst), 2);
    assert_eq!(retry.mounted, vec!["failing"]);
}

#[tokio::test]
async fn disabled_keeps_entry_and_skips_mount() {
    let (app, a, _, _) = counting_app();
    let mut loader = Loader::new(app);

    loader.apply(vec![row("a", "alpha")]).await.unwrap();
    assert_eq!(a.load(Ordering::SeqCst), 1);

    let mut dis = row("a", "alpha");
    dis.disabled = true;
    let report = loader.apply(vec![dis]).await.unwrap();
    assert_eq!(report.unmounted, vec!["a"]);

    // re-enable restores the fiber (Reconfig path: disabled flip)
    let report = loader.apply(vec![row("a", "alpha")]).await.unwrap();
    assert_eq!(report.remounted, vec!["a"]);
    assert_eq!(a.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn isolate_groups_get_private_service_instances() {
    let (app, _, _, s) = counting_app();
    let mut loader = Loader::new(app);

    let yaml = r#"
- id: g1
  name: alpha
  group:
    isolate: true
    rows:
      - id: shell-1
        name: shell
- id: g2
  name: alpha
  group:
    isolate: true
    rows:
      - id: shell-2
        name: shell
"#;
    let report = loader.load_str(yaml).await.unwrap();
    assert_eq!(report.mounted.len(), 4);
    assert!(report.failed.is_empty(), "{report:?}");

    for id in ["g1", "shell-1", "g2", "shell-2"] {
        let fiber = loader.fiber(id).unwrap();
        assert_eq!(fiber.state(), FiberState::Active, "row {id}");
    }
    assert_eq!(s.load(Ordering::SeqCst), 2, "shell mounted once per group");

    // two distinct scopes, one private `shell` each — no global collision
    let services = loader.app().services();
    assert_eq!(services.len(), 2, "{services:?}");
    let scopes: Vec<_> = services.iter().map(|s| s.scope).collect();
    assert_ne!(scopes[0], scopes[1]);

    // adding a third non-isolated shell at global scope would now be
    // visible to nobody's private lookup first — global fallback still works
}

#[tokio::test]
async fn hmr_reload_on_file_change() {
    let dir = std::env::temp_dir().join(format!("nucleus-hmr-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = dir.join("nucleus.yml");

    std::fs::write(&cfg, "- id: a\n  name: alpha\n").unwrap();

    let (app, a, b, _) = counting_app();
    let handle = nucleus_loader::hmr::spawn_hmr(app, cfg.clone());
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(a.load(Ordering::SeqCst), 1, "initial load applied");
    assert_eq!(b.load(Ordering::SeqCst), 0);

    // edit: add a row; existing row untouched
    std::fs::write(&cfg, "- id: a\n  name: alpha\n- id: b\n  name: beta\n").unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    assert_eq!(b.load(Ordering::SeqCst), 1, "new row mounted after edit");
    assert_eq!(
        a.load(Ordering::SeqCst),
        1,
        "unchanged row NOT remounted by HMR"
    );

    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
