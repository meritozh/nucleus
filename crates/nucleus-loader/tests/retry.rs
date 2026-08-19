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
