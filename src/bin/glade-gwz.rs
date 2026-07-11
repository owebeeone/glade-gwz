//! `glade-gwz` — attach a gwz command supplier to a glade node and serve until
//! SIGTERM/SIGINT (GLP-0006 P1.S2).
//!
//! ```text
//! glade-gwz --node ws://127.0.0.1:PORT --root DIR
//!           [--share ws-razel] [--glade-id gwz.ops] [--output-id gwz.output]
//!           [--principal P] [--gwz-bin gwz] [--timeout-secs 30]
//! ```
//!
//! It connects, attaches as THE provider for `(share, glade_id)`, reattaches on
//! link drop (the kit helper), and tears the session down cleanly on a signal.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use glade_gwz::{
    serve, GwzConfig, DEFAULT_GLADE_ID, DEFAULT_OUTPUT_ID, DEFAULT_SHARE, DEFAULT_TIMEOUT_SECS,
};

const USAGE: &str = "usage: glade-gwz --node ws://HOST:PORT --root DIR \
[--share ws-razel] [--glade-id gwz.ops] [--output-id gwz.output] \
[--principal P] [--gwz-bin gwz] [--timeout-secs 30]";

#[tokio::main]
async fn main() -> ExitCode {
    let config = match parse_args(std::env::args().skip(1).collect()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("glade-gwz: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("glade-gwz: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: GwzConfig) -> std::io::Result<()> {
    eprintln!(
        "glade-gwz: attaching to {} as {}/{} (root {}, principal {})",
        config.node_url,
        config.share,
        config.glade_id,
        config.root.display(),
        config.principal.as_deref().unwrap_or("<none>"),
    );
    let supplier = serve(config).await?;
    eprintln!("glade-gwz: serving; SIGTERM/SIGINT to stop");

    wait_for_shutdown_signal().await;
    eprintln!("glade-gwz: signal received, detaching");
    supplier.shutdown().await;
    Ok(())
}

/// Resolve on SIGTERM or SIGINT (Ctrl-C) — the clean-shutdown trigger.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// A tiny hand-rolled flag parser (the crate stays dep-light — no clap). `--node`
/// and `--root` are required; everything else defaults.
fn parse_args(args: Vec<String>) -> Result<GwzConfig, String> {
    let mut node: Option<String> = None;
    let mut root: Option<PathBuf> = None;
    let mut share = DEFAULT_SHARE.to_string();
    let mut glade_id = DEFAULT_GLADE_ID.to_string();
    let mut output_id = DEFAULT_OUTPUT_ID.to_string();
    let mut principal: Option<String> = None;
    let mut gwz_bin = PathBuf::from("gwz");
    let mut timeout_secs = DEFAULT_TIMEOUT_SECS;

    let mut it = args.into_iter();
    while let Some(flag) = it.next() {
        let mut take = |name: &str| it.next().ok_or_else(|| format!("{name} needs a value"));
        match flag.as_str() {
            "--node" => node = Some(take("--node")?),
            "--root" => root = Some(PathBuf::from(take("--root")?)),
            "--share" => share = take("--share")?,
            "--glade-id" => glade_id = take("--glade-id")?,
            "--output-id" => output_id = take("--output-id")?,
            "--principal" => principal = Some(take("--principal")?),
            "--gwz-bin" => gwz_bin = PathBuf::from(take("--gwz-bin")?),
            "--timeout-secs" => {
                timeout_secs = take("--timeout-secs")?
                    .parse()
                    .map_err(|_| "--timeout-secs must be an integer".to_string())?
            }
            "-h" | "--help" => return Err("help".into()),
            other => return Err(format!("unknown flag `{other}`")),
        }
    }

    Ok(GwzConfig {
        node_url: node.ok_or("--node is required")?,
        share,
        glade_id,
        output_id,
        root: root.ok_or("--root is required")?,
        gwz_bin,
        principal,
        timeout: Duration::from_secs(timeout_secs),
    })
}
