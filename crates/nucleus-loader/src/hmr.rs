//! HMR: watch a `nucleus.yml` file and re-reconcile on change.
//!
//! Hot reload here is nothing special: unloading rewinds every effect and
//! loading follows dependencies, so reconciliation *is* the reload
//! mechanism (same insight as nucleus-plugin-hmr).

use crate::Loader;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::AsyncReadExt;

// Re-open the configured pathname so an editor's atomic rename is observed
// even on filesystems that coalesce or omit change notifications.
const CHECK_INTERVAL: Duration = Duration::from_millis(250);
const MAX_CONFIG_BYTES: u64 = 1024 * 1024;
async fn read_config(path: &Path) -> crate::Result<String> {
    let mut content = String::new();
    tokio::fs::File::open(path)
        .await?
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_string(&mut content)
        .await?;
    if content.len() as u64 > MAX_CONFIG_BYTES {
        return Err(crate::LoaderError::Watch(
            "configuration exceeds 1 MiB".into(),
        ));
    }
    Ok(content)
}

/// Monitor one configuration file. Invalid edits retain the last applied
/// configuration. A cancelled monitor owns no blocked receiver thread.
pub async fn run_hmr(app: nucleus_core::App, path: impl Into<PathBuf>) -> crate::Result<()> {
    let path = path.into();
    let mut loader = Loader::new(app);
    let mut previous = read_config(&path).await?;
    let report = loader.load_str(&previous).await?;
    tracing::info!(path=%path.display(),report=%report.summary(),"initial load");
    loop {
        tokio::time::sleep(CHECK_INTERVAL).await;
        let yaml = match read_config(&path).await {
            Ok(value) if value == previous => continue,
            Ok(value) => value,
            Err(e) => {
                tracing::warn!(error=%e,"configuration unavailable");
                continue;
            }
        };
        match loader.load_str(&yaml).await {
            Ok(report) => {
                tracing::info!(report=%report.summary(),"reconciled changed configuration")
            }
            Err(e) => tracing::warn!(error=%e,"configuration rejected; keeping last good state"),
        }
        previous = yaml;
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
