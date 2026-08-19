//! HMR: watch a `nucleus.yml` file and re-reconcile on change.
//!
//! Hot reload here is nothing special: unloading rewinds every effect and
//! loading follows dependencies, so reconciliation *is* the reload
//! mechanism (same insight as nucleus-plugin-hmr).

use crate::Loader;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Debounce window for coalescing editor save events.
const DEBOUNCE: Duration = Duration::from_millis(200);

/// Run one reconcile now, then watch `path` until the watcher is dropped
/// or the task is aborted. Bad reads keep the last good state and log.
pub async fn run_hmr(app: nucleus_core::App, path: impl Into<PathBuf>) -> crate::Result<()> {
    let path: PathBuf = path.into();
    let mut loader = Loader::new(app);

    let report = loader.load_file(&path).await?;
    tracing::info!(path = %path.display(), report = %report.summary(), "initial load");

    let (tx, rx) = mpsc::channel::<Event>();
    let rx = Arc::new(Mutex::new(rx));
    let mut watcher: RecommendedWatcher = Watcher::new(
        move |res: std::result::Result<Event, notify::Error>| {
            if let Ok(ev) = res {
                let _ = tx.send(ev);
            }
        },
        notify::Config::default(),
    )
    .map_err(|e| crate::LoaderError::Watch(e.to_string()))?;
    watcher
        .watch(path.as_path(), RecursiveMode::NonRecursive)
        .map_err(|e| crate::LoaderError::Watch(e.to_string()))?;
    let _watcher = watcher; // kept alive for the loop duration

    loop {
        // notify hands us events on a sync channel; bridge without
        // block_in_place so this works on current-thread runtimes too.
        let ev = {
            let rx = rx.clone();
            tokio::task::spawn_blocking(move || rx.lock().unwrap().recv())
                .await
                .map_err(|e| crate::LoaderError::Watch(e.to_string()))?
        };
        let ev = match ev {
            Ok(ev) => ev,
            Err(_) => return Ok(()), // watcher dropped: stop
        };
        if !matches!(
            ev.kind,
            EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
        ) {
            continue;
        }
        // Give editors that write via rename/replace a moment to settle,
        // then drain whatever coalesced behind the event we saw (one save
        // typically emits several events; each must not trigger its own
        // reconcile).
        tokio::time::sleep(DEBOUNCE).await;
        {
            let rx = rx.clone();
            let _ = tokio::task::spawn_blocking(move || {
                while rx.lock().unwrap().try_recv().is_ok() {}
            })
            .await;
        }
        match loader.load_file(&path).await {
            Ok(report) => {
                tracing::info!(report = %report.summary(), "reconciled after config change");
            }
            Err(e) => {
                tracing::warn!(error = %e, "config reload failed; keeping last good state");
            }
        }
    }
}

/// Spawn [`run_hmr`] on the current tokio runtime.
pub fn spawn_hmr(
    app: nucleus_core::App,
    path: impl Into<PathBuf>,
) -> tokio::task::JoinHandle<crate::Result<()>> {
    let path = path.into();
    tokio::spawn(async move { run_hmr(app, path).await })
}
