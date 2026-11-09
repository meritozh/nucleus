//! Regression: a failed mount must be retried on the next identical pass
//! (a transient failure — e.g. a wasm file mid-write — must not stick
//! until someone edits the yaml).

use nucleus_core::{App, FiberState, Plugin};
use nucleus_loader::Loader;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct Flaky;

#[async_trait::async_trait]
impl Plugin for Flaky {
    fn name(&self) -> &str {
        "flaky"
    }
    async fn apply(&self, _ctx: nucleus_core::Context) -> nucleus_core::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn loader_retries_failed_mounts() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let app = App::new().with_static(nucleus_core::static_source! {
        "flaky" => {
            let attempts = attempts.clone();
            move |_c| {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(nucleus_core::Error::PluginNotFound("flaky: transient".into()))
                } else {
                    Ok(Arc::new(Flaky))
                }
            }
        },
    });

    let mut loader = Loader::new(app);
    let row = nucleus_loader::Row {
        id: Some("flaky".into()),
        name: "flaky".into(),
        source: "static".into(),
        config: serde_json::Value::Null,
        disabled: false,
        group: None,
    };

    let report = loader.apply(vec![row.clone()]).await.unwrap();
    assert_eq!(report.failed.len(), 1);
    assert!(loader.fiber("flaky").is_none());

    // identical pass: the failed row must be retried (and now succeed)
    let report = loader.apply(vec![row]).await.unwrap();
    assert!(report.failed.is_empty(), "{report:?}");
    assert_eq!(report.mounted, vec!["flaky".to_string()]);
    assert_eq!(loader.fiber("flaky").unwrap().state(), FiberState::Active);
}

#[tokio::test]
async fn duplicate_nested_ids_preserve_previous_configuration() {
    let app =
        App::new().with_static(nucleus_core::static_source! {"flaky"=>|_|Ok(Arc::new(Flaky))});
    let mut loader = Loader::new(app.clone());
    loader.load_str("- {id: old, name: flaky}").await.unwrap();
    let old = loader.fiber("old").unwrap().id();
    let err=loader.load_str("- {id: x, name: flaky}\n- id: group\n  name: flaky\n  group:\n    rows: [{id: x, name: flaky}]").await.unwrap_err();
    assert!(matches!(
        err,
        nucleus_loader::LoaderError::DuplicateId {
            first: 1,
            second: 3,
            ..
        }
    ));
    assert_eq!(loader.fiber("old").unwrap().id(), old);
    loader.load_str("[]").await.unwrap();
    assert_eq!(app.fibers().len(), 1);
}
struct FailOnce(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl Plugin for FailOnce {
    fn name(&self) -> &str {
        "plugin"
    }
    async fn apply(&self, _: nucleus_core::Context) -> nucleus_core::Result<()> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(nucleus_core::Error::ApplyFailed(
                "plugin".into(),
                "first".into(),
            ));
        }
        Ok(())
    }
}
#[tokio::test]
async fn failed_activation_is_not_reported_mounted_and_retries() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let app = App::new().with_static(
        nucleus_core::static_source! {"plugin"=> move |_|Ok(Arc::new(FailOnce(attempts.clone())))},
    );
    let mut loader = Loader::new(app.clone());
    let yaml = "- {id: x, name: plugin}";
    let first = loader.load_str(yaml).await.unwrap();
    assert!(first.active.is_empty() && first.mounted.is_empty());
    assert_eq!(first.failed.len(), 1);
    assert_eq!(app.fibers().len(), 1);
    let second = loader.load_str(yaml).await.unwrap();
    assert_eq!(second.active, ["x"]);
    assert!(second.failed.is_empty());
}
