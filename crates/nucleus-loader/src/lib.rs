//! # nucleus-loader
//!
//! nucleus.yml row reconciliation for [`nucleus_core::App`], mirroring the
//! nucleus loader semantics researched from deepseek-harness:
//!
//! - rows are diffed **by `id`**: mount / unmount / reconfigure only what
//!   changed;
//! - `disabled: true` keeps the entry but skips mounting (re-enabling
//!   restores the fiber and everything pending on its services);
//! - nested `group` rows load/unload as one unit; `isolate: true` gives the
//!   group a private service scope;
//! - rows without an explicit `id` get a generated one on each read, so any
//!   edit remounts them (nucleus behaves the same way — always write ids).
//!
//! HMR (`spawn_hmr`) watches the config file and re-applies reconciliation
//! on change; unloading rewinds effects and loading follows dependencies,
//! so reconcile *is* hot reload.

use nucleus_core::{App, FiberHandle, MountSpec, ScopeId, GLOBAL};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

pub mod hmr;

#[derive(Debug, thiserror::Error)]
pub enum LoaderError {
    #[error("yaml parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("duplicate row id `{id}` at flattened rows {first} and {second}")]
    DuplicateId {
        id: String,
        first: usize,
        second: usize,
    },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("mount error: {0}")]
    Mount(#[from] nucleus_core::Error),
    #[error("watch error: {0}")]
    Watch(String),
}

pub type Result<T, E = LoaderError> = std::result::Result<T, E>;

/// One row of `nucleus.yml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// Stable identity for reconciliation. **Always set this.**
    pub id: Option<String>,
    /// Plugin name inside the source.
    pub name: String,
    /// Source name; `"static"` when omitted.
    #[serde(default = "default_source")]
    pub source: String,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub group: Option<GroupSpec>,
}

fn default_source() -> String {
    "static".to_string()
}

/// Nested sub-list that loads/unloads as one unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSpec {
    /// Give the group its own instance of every service name.
    #[serde(default)]
    pub isolate: bool,
    pub rows: Vec<Row>,
}

/// A row resolved to a stable identity + service scope.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedRow {
    pub id: String,
    pub name: String,
    pub source: String,
    pub config: Value,
    pub disabled: bool,
    pub scope: ScopeId,
}

/// One reconciliation operation (returned by [`diff`]).
#[derive(Debug, PartialEq, Eq)]
pub enum Op {
    Mount(String),
    Unmount(String),
    Reconfig(String),
    Noop(String),
}

/// Result of one reconciliation pass.
#[derive(Debug, Default, Clone)]
pub struct ReconcileReport {
    pub mounted: Vec<String>,
    pub remounted: Vec<String>,
    pub unmounted: Vec<String>,
    pub unchanged: usize,
    /// Actual end-of-pass state; mounted/remounted describe operations.
    pub active: Vec<String>,
    pub pending: Vec<String>,
    pub failed: Vec<(String, String)>,
}

impl ReconcileReport {
    pub fn summary(&self) -> String {
        format!(
            "mounted={} remounted={} unmounted={} unchanged={} failed={}",
            self.mounted.len(),
            self.remounted.len(),
            self.unmounted.len(),
            self.unchanged,
            self.failed.len()
        )
    }
}

/// Flatten a row tree. Rows without an explicit id get a generated one
/// that is **new on every call**. Fresh scope cache: isolated groups get
/// new scopes every call (test/diff use only — the Loader uses
/// `resolve_with_scopes` so group scopes stay stable across passes).
pub fn resolve(rows: &[Row], app: &App, base: ScopeId) -> Vec<ResolvedRow> {
    let mut scopes = HashMap::new();
    resolve_with_scopes(rows, app, base, &mut scopes)
}

/// Like [`resolve`], but isolated-group scopes are memoized in `scopes`
/// (keyed by group row id + isolate flag). A group that survives an edit
/// keeps its scope, so a Reconfig of one member cannot split the group
/// across two scopes.
pub fn resolve_with_scopes(
    rows: &[Row],
    app: &App,
    base: ScopeId,
    scopes: &mut HashMap<String, ScopeId>,
) -> Vec<ResolvedRow> {
    let mut seq: u64 = 0;
    let mut out = Vec::new();
    resolve_into(rows, app, base, &mut seq, scopes, &mut out);
    out
}

fn resolve_into(
    rows: &[Row],
    app: &App,
    base: ScopeId,
    seq: &mut u64,
    scopes: &mut HashMap<String, ScopeId>,
    out: &mut Vec<ResolvedRow>,
) {
    for row in rows {
        let id = match &row.id {
            Some(id) => id.clone(),
            None => {
                *seq += 1;
                format!("~gen{}", seq)
            }
        };
        let scope = base;
        out.push(ResolvedRow {
            id,
            name: row.name.clone(),
            source: row.source.clone(),
            config: row.config.clone(),
            disabled: row.disabled,
            scope,
        });
        if let Some(group) = &row.group {
            let child_base = if group.isolate {
                let key = format!(
                    "{}:{}",
                    out.last().map(|r| r.id.clone()).unwrap_or_default(),
                    group.isolate
                );
                *scopes.entry(key).or_insert_with(|| app.new_scope())
            } else {
                base
            };
            resolve_into(&group.rows, app, child_base, seq, scopes, out);
        }
    }
}

/// Diff two resolved states by row id. Ops carry ids; rows are looked up
/// by the caller in the `next` state.
pub fn diff(prev: &[ResolvedRow], next: &[ResolvedRow]) -> Vec<Op> {
    let prev_map: HashMap<&str, &ResolvedRow> = prev.iter().map(|r| (r.id.as_str(), r)).collect();
    let next_map: HashMap<&str, &ResolvedRow> = next.iter().map(|r| (r.id.as_str(), r)).collect();

    let mut ops = Vec::new();
    // Unmounts first (reverse order of appearance), then mounts/reconfigs in
    // order, so dependency-driven activation can settle in between.
    for row in prev.iter().rev() {
        if !next_map.contains_key(row.id.as_str()) {
            ops.push(Op::Unmount(row.id.clone()));
        }
    }
    for row in next {
        match prev_map.get(row.id.as_str()) {
            None => ops.push(Op::Mount(row.id.clone())),
            Some(old) => {
                let changed = old.name != row.name
                    || old.source != row.source
                    || old.config != row.config
                    || old.disabled != row.disabled
                    || old.scope != row.scope;
                if changed {
                    ops.push(Op::Reconfig(row.id.clone()));
                } else {
                    ops.push(Op::Noop(row.id.clone()));
                }
            }
        }
    }
    ops
}

/// Config-driven mount/unmount engine over one [`App`].
pub struct Loader {
    app: App,
    current: Vec<ResolvedRow>,
    mounted: HashMap<String, FiberHandle>,
    /// Group row id -> allocated scope, stable across reconcile passes.
    scopes: HashMap<String, ScopeId>,
}

impl Loader {
    pub fn new(app: App) -> Self {
        Self {
            app,
            current: Vec::new(),
            mounted: HashMap::new(),
            scopes: HashMap::new(),
        }
    }

    pub fn app(&self) -> &App {
        &self.app
    }

    /// Parse `nucleus.yml` content and reconcile.
    pub async fn load_str(&mut self, yaml: &str) -> Result<ReconcileReport> {
        let rows: Vec<Row> = serde_yaml::from_str(yaml)?;
        self.apply(rows).await
    }

    /// Load from a file (used by HMR).
    pub async fn load_file(&mut self, path: impl AsRef<Path>) -> Result<ReconcileReport> {
        let yaml = tokio::fs::read_to_string(path).await?;
        self.load_str(&yaml).await
    }

    /// Resolve + diff + apply. The loader is transactional in the dsh
    /// sense: a row that fails to mount is reported and skipped without
    /// unwinding the rest of the pass.
    pub async fn apply(&mut self, rows: Vec<Row>) -> Result<ReconcileReport> {
        let mut scopes = self.scopes.clone();
        let next = resolve_with_scopes(&rows, &self.app, GLOBAL, &mut scopes);
        let mut seen = HashMap::new();
        for (i, row) in next.iter().enumerate() {
            if let Some(first) = seen.insert(&row.id, i + 1) {
                return Err(LoaderError::DuplicateId {
                    id: row.id.clone(),
                    first,
                    second: i + 1,
                });
            }
        }
        self.scopes = scopes;
        let ops = diff(&self.current, &next);
        let next_map: HashMap<String, ResolvedRow> =
            next.iter().cloned().map(|r| (r.id.clone(), r)).collect();
        let mut report = ReconcileReport::default();
        let mut failed_ids: Vec<String> = Vec::new();

        for op in ops {
            match op {
                Op::Unmount(id) => {
                    if let Some(handle) = self.mounted.remove(&id) {
                        // A failed unload is recorded, not fatal: the rest
                        // of the pass still runs and `current` still moves
                        // forward (the next pass re-derives from it).
                        match handle.dispose().await {
                            Ok(()) => report.unmounted.push(id.clone()),
                            Err(e) => {
                                failed_ids.push(id.clone());
                                report.failed.push((id, e.to_string()));
                            }
                        }
                    }
                }
                Op::Noop(_id) => report.unchanged += 1,
                Op::Mount(id) => {
                    let Some(row) = next_map.get(&id) else {
                        continue;
                    };
                    if row.disabled {
                        report.unchanged += 1;
                        continue;
                    }
                    match self.mount_row(row).await {
                        Ok(()) => report.mounted.push(id.clone()),
                        Err(e) => {
                            failed_ids.push(id.clone());
                            report.failed.push((id.clone(), e.to_string()));
                        }
                    }
                }
                Op::Reconfig(id) => {
                    let Some(row) = next_map.get(&id) else {
                        continue;
                    };
                    if let Some(handle) = self.mounted.remove(&id) {
                        if let Err(e) = handle.dispose().await {
                            failed_ids.push(id.clone());
                            report.failed.push((id.clone(), e.to_string()));
                            continue;
                        }
                    }
                    if row.disabled {
                        report.unmounted.push(id.clone());
                        continue;
                    }
                    match self.mount_row(row).await {
                        Ok(()) => report.remounted.push(id.clone()),
                        Err(e) => {
                            failed_ids.push(id.clone());
                            report.failed.push((id.clone(), e.to_string()));
                        }
                    }
                }
            }
        }

        // A row can become Failed after its mount (a later dependency may
        // have activated it). Inspect all handles after the complete pass.
        for row in &next {
            let Some(handle) = self.mounted.get(&row.id) else {
                continue;
            };
            use nucleus_core::FiberState;
            match handle.state() {
                FiberState::Active => report.active.push(row.id.clone()),
                FiberState::Pending | FiberState::Loading => report.pending.push(row.id.clone()),
                state => {
                    let message = handle
                        .info()
                        .and_then(|i| i.error)
                        .unwrap_or_else(|| state.to_string());
                    let handle = self.mounted.remove(&row.id).expect("observed handle");
                    let message = match handle.dispose().await {
                        Ok(()) => message,
                        Err(e) => format!("{message}; cleanup: {e}"),
                    };
                    report.mounted.retain(|id| id != &row.id);
                    report.remounted.retain(|id| id != &row.id);
                    failed_ids.push(row.id.clone());
                    report.failed.push((row.id.clone(), message));
                }
            }
        }
        // Failed rows stay out of `current`: an identical next pass must
        // re-derive a Mount and retry (e.g. a wasm file that was mid-write
        // during a compile). Without this a transient failure would stick
        // until someone edits the yaml.
        self.current = next
            .into_iter()
            .filter(|r| !failed_ids.contains(&r.id))
            .collect();
        Ok(report)
    }

    async fn mount_row(&mut self, row: &ResolvedRow) -> nucleus_core::Result<()> {
        let spec = MountSpec {
            row_id: Some(row.id.clone()),
            source: row.source.clone(),
            name: row.name.clone(),
            config: row.config.clone(),
            scope: row.scope,
        };
        let handle = self.app.mount(spec).await?;
        self.mounted.insert(row.id.clone(), handle);
        Ok(())
    }

    /// Mounted fiber for a row id (present even when the fiber is `Pending`).
    pub fn fiber(&self, row_id: &str) -> Option<FiberHandle> {
        self.mounted.get(row_id).cloned()
    }

    pub fn rows(&self) -> &[ResolvedRow] {
        &self.current
    }
}
