//! cordis.yml reconciliation + HMR demo.
//!
//! Run: `cargo run -p cordis-loader --example hmr`
//!
//! The example writes a cordis.yml into a temp dir, starts the HMR
//! watcher, then rewrites the file (add a row / change a config / disable
//! a row) and prints the reconciliation reports. Edit the file yourself
//! while it runs to play with live reloading; Ctrl-C to stop.

use cordis_core::{App, Plugin};
use cordis_loader::Loader;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct Counting(&'static str, Arc<AtomicUsize>);

#[async_trait::async_trait]
impl Plugin for Counting {
    fn name(&self) -> &str {
        self.0
    }
    async fn apply(&self, _ctx: cordis_core::Context) -> cordis_core::Result<()> {
        let n = self.1.fetch_add(1, Ordering::SeqCst) + 1;
        println!("  [{}] apply #{}", self.0, n);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let alpha = Arc::new(AtomicUsize::new(0));
    let beta = Arc::new(AtomicUsize::new(0));

    let app = App::new().with_static(cordis_core::static_source! {
        "alpha" => { let c = alpha.clone(); move |_c| Ok(Arc::new(Counting("alpha", c.clone()))) },
        "beta"  => { let c = beta.clone(); move |_c| Ok(Arc::new(Counting("beta", c.clone()))) },
    });

    let dir = std::env::temp_dir().join(format!("cordis-hmr-demo-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let cfg = dir.join("cordis.yml");
    std::fs::write(&cfg, "- id: a\n  name: alpha\n")?;

    // manual pass first, to show reconcile reports
    let mut loader = Loader::new(app.clone());
    let report = loader.load_file(&cfg).await?;
    println!("initial: {}", report.summary());

    std::fs::write(&cfg, "- id: a\n  name: alpha\n  config: {k: 1}\n- id: b\n  name: beta\n")?;
    let report = loader.load_file(&cfg).await?;
    println!("edited:  {}", report.summary());

    std::fs::write(&cfg, "- id: a\n  name: alpha\n  disabled: true\n")?;
    let report = loader.load_file(&cfg).await?;
    println!("disabled a: {}", report.summary());

    // then live HMR: edit the file while this is running
    let handle = cordis_loader::hmr::spawn_hmr(app, cfg.clone());
    println!("\nwatching {} — edit it live, Ctrl-C to stop", cfg.display());
    tokio::signal::ctrl_c().await.ok();
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
