//! Integration: the glade-gwz supplier against a SPAWNED glade-node booted with
//! an app file that declares the gwz exchange + output surfaces (never the real
//! `~/.glade` — temp GLADE_HOME/HOME + temp store). The crate holds no node
//! internals; the tests talk to the shipped binaries exactly as a deployment
//! would. Coverage the plan names (§P1.S2, Tests):
//!
//!   1. allow-listed verbs (status/ls/diff) round-trip against a REAL `gwz`
//!      invocation on a scratch workspace the test creates (`gwz init`).
//!   2. a disallowed verb + a scope-redirecting arg are failure-as-DATA.
//!   3. a timeout is failure-as-DATA (a slow shim binary — the timeout machinery
//!      deterministically, without a long real command; clearly marked).
//!   4. a streaming run's output appends are visible to a log subscriber, closed
//!      by a `done:true` marker (REAL gwz).
//!   5. the `glade-gwz` BINARY attaches, answers, and shuts down cleanly on
//!      SIGTERM.
//!   6. a RESTARTED supplier streams its first run: a subscriber on the new
//!      run's id folds that run's output, not an earlier process's (REAL gwz).
//!   7. a record the node REFUSES is said on the binary's stderr, with its
//!      chain, its seq and its code (a gated shim, clearly marked).
//!   8. gwz gets the environment its config carries and nothing else, from both
//!      runners (an env-printing shim, clearly marked).
//!   9. a variable set in the process after the capture never reaches gwz.
//!  10. the BINARY gives gwz its whole start-up environment, and finds `gwz` on
//!      that environment's PATH, as grazel starts it.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

use glade_client::{GladeClient, OpOutcome};
use glade_gwz::{serve, Environment, GwzConfig, GwzOutputRecord, GwzResponse};

// ---- harness --------------------------------------------------------------

fn manifest() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
fn node_bin() -> PathBuf {
    manifest().join("../glade/node/target/debug/glade-node")
}
fn app_file() -> PathBuf {
    manifest().join("tests/fixtures/gwz-test-app.glade")
}
/// The real `gwz` binary — prefer the cargo-bin install so the test does not
/// depend on PATH; fall back to `gwz` on PATH.
fn gwz_bin() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        let p = PathBuf::from(home).join(".cargo/bin/gwz");
        if p.exists() {
            return p;
        }
    }
    PathBuf::from("gwz")
}

/// The gate pre-builds the node; build once if absent so the suite is
/// self-sufficient (the node has its own target dir — no lock clash).
fn ensure_node_built() {
    let bin = node_bin();
    if bin.exists() {
        return;
    }
    let status = std::process::Command::new(env!("CARGO"))
        .args(["build", "--bin", "glade-node"])
        .current_dir(manifest().join("../glade/node"))
        .status()
        .expect("build glade-node");
    assert!(
        status.success() && bin.exists(),
        "glade-node missing after build"
    );
}

/// A temp dir that removes itself on drop (never the real `~/.glade`).
struct Tmp(PathBuf);
impl Tmp {
    fn new(tag: &str) -> Tmp {
        static N: AtomicU64 = AtomicU64::new(0);
        let uniq = format!(
            "{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        );
        let p = std::env::temp_dir().join(format!("glade-gwz-{tag}-{uniq}"));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Tmp(p)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Read the node's `listening <port>` line (bounded), then drain stdout.
async fn wait_listening(child: &mut Child) -> u16 {
    let stdout = child.stdout.take().expect("piped stdout");
    let mut lines = BufReader::new(stdout).lines();
    let port = tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(line) = lines.next_line().await.ok().flatten() {
            if let Some(rest) = line.strip_prefix("listening ") {
                if let Ok(p) = rest.trim().parse::<u16>() {
                    return Some(p);
                }
            }
        }
        None
    })
    .await
    .ok()
    .flatten()
    .expect("node printed a listening port");
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    port
}

/// Boot the node with the gwz-test app (declares gwz.ops exchange + gwz.output
/// log + the ws-razel workspace share) under a temp GLADE_HOME/HOME.
async fn boot(tmp: &Tmp) -> (Child, u16) {
    ensure_node_built();
    let mut child = Command::new(node_bin())
        .args(["--profile", "local", "--name", "gwzit", "--app"])
        .arg(app_file())
        .arg("0")
        .arg(tmp.path().join("store"))
        .env("GLADE_HOME", tmp.path().join("gh"))
        .env("HOME", tmp.path().join("h"))
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn booted glade-node");
    let port = wait_listening(&mut child).await;
    (child, port)
}

/// A fresh gwz workspace (`gwz init`) in a temp dir — the app-owned root the
/// supplier serves against.
fn make_gwz_workspace(tmp: &Tmp) -> PathBuf {
    let ws = tmp.path().join("ws");
    std::fs::create_dir_all(&ws).unwrap();
    let status = std::process::Command::new(gwz_bin())
        .arg("--root")
        .arg(&ws)
        .arg("init")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run gwz init");
    assert!(status.success(), "gwz init failed (is `gwz` installed?)");
    ws
}

/// A config whose gwz gets this test process's own environment, as it did when
/// gwz inherited it.
fn config_for(url: &str, root: PathBuf) -> GwzConfig {
    let mut c = GwzConfig::new(url, root, Environment::from_vars(std::env::vars_os()));
    c.gwz_bin = gwz_bin();
    c.principal = Some("gianni".into());
    c
}

async fn poll<F, Fut>(mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..200 {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

/// Issue an exchange envelope and decode the `GwzResponse` payload. Asserts the
/// WIRE ok is true (the exchange always produces a structured answer).
async fn ask(requester: &GladeClient, envelope: &str) -> GwzResponse {
    let out = requester
        .exchange("ws-razel", "gwz.ops", envelope.as_bytes().to_vec())
        .await
        .expect("exchange");
    assert!(
        out.ok,
        "wire ExchangeRes.ok is always true (failure is in the payload); error={:?}",
        out.error
    );
    serde_json::from_slice(&out.payload.expect("payload")).expect("GwzResponse json")
}

// ---- 1. allow-listed verbs round-trip against real gwz ---------------------

#[tokio::test(flavor = "multi_thread")]
async fn allowlisted_verbs_round_trip_real_gwz() {
    let tmp = Tmp::new("rt");
    let (mut node, port) = boot(&tmp).await;
    let url = format!("ws://127.0.0.1:{port}");
    let ws = make_gwz_workspace(&tmp);

    let _sup = serve(config_for(&url, ws)).await.unwrap();

    let requester = GladeClient::new("requester");
    requester.connect(&url).await.unwrap();

    // wait for the provider to attach + answer (serve resolves on the ack, but
    // poll defensively against residual ordering).
    let r = requester.clone();
    assert!(
        poll(|| {
            let r = r.clone();
            async move {
                r.exchange("ws-razel", "gwz.ops", br#"{"verb":"status"}"#.to_vec())
                    .await
                    .map(|o| o.ok)
                    .unwrap_or(false)
            }
        })
        .await,
        "the gwz supplier attached and answered"
    );

    // status: ran clean, stdout non-empty (the freshly-init'd workspace has
    // staged files), attribution stamped.
    let status = ask(&requester, r#"{"verb":"status"}"#).await;
    assert!(status.ok, "status ran clean: {status:?}");
    assert_eq!(status.exit, Some(0));
    assert!(
        !status.stdout.is_empty(),
        "status produced output: {status:?}"
    );
    assert_eq!(status.attributed_to.as_deref(), Some("gianni"));

    // ls + diff also answer ok:true, exit 0.
    let ls = ask(&requester, r#"{"verb":"ls"}"#).await;
    assert!(ls.ok && ls.exit == Some(0), "ls: {ls:?}");
    let diff = ask(&requester, r#"{"verb":"diff"}"#).await;
    assert!(diff.ok && diff.exit == Some(0), "diff: {diff:?}");

    // a request-supplied principal overrides the configured one (attribution as
    // data).
    let who = ask(&requester, r#"{"verb":"status","principal":"alice"}"#).await;
    assert_eq!(who.attributed_to.as_deref(), Some("alice"));

    requester.close().await;
    node.kill().await.ok();
}

// ---- 2. disallowed verb + denied arg -> failure as data --------------------

#[tokio::test(flavor = "multi_thread")]
async fn disallowed_verb_and_denied_arg_fail_as_data() {
    let tmp = Tmp::new("deny");
    let (mut node, port) = boot(&tmp).await;
    let url = format!("ws://127.0.0.1:{port}");
    let ws = make_gwz_workspace(&tmp);
    let _sup = serve(config_for(&url, ws)).await.unwrap();

    let requester = GladeClient::new("requester");
    requester.connect(&url).await.unwrap();
    // ensure attached
    let r = requester.clone();
    assert!(
        poll(|| {
            let r = r.clone();
            async move {
                r.exchange("ws-razel", "gwz.ops", br#"{"verb":"status"}"#.to_vec())
                    .await
                    .map(|o| o.ok)
                    .unwrap_or(false)
            }
        })
        .await
    );

    // a mutating verb never reaches gwz — refused as data.
    let commit = ask(&requester, r#"{"verb":"commit","args":["-m","x"]}"#).await;
    assert!(!commit.ok, "commit refused: {commit:?}");
    assert!(
        commit.error.as_deref().unwrap_or("").contains("allow-list"),
        "{commit:?}"
    );
    assert!(commit.exit.is_none(), "gwz was never invoked: {commit:?}");

    // an allowed verb carrying a scope-redirecting arg is refused (root is
    // app-owned).
    let escape = ask(&requester, r#"{"verb":"status","args":["--root","/etc"]}"#).await;
    assert!(
        !escape.ok
            && escape
                .error
                .as_deref()
                .unwrap_or("")
                .contains("not permitted"),
        "{escape:?}"
    );

    // a bad envelope is data, not a hang.
    let bad = ask(&requester, "not json").await;
    assert!(
        !bad.ok && bad.error.as_deref().unwrap_or("").contains("bad envelope"),
        "{bad:?}"
    );

    requester.close().await;
    node.kill().await.ok();
}

// ---- 3. timeout -> failure as data (slow shim) -----------------------------

/// Write an executable shell script standing in for `gwz` (marked: not the real
/// binary), named `name` in the test's temp dir.
fn write_shim(tmp: &Tmp, name: &str, script: &str) -> PathBuf {
    let shim = tmp.path().join(name);
    std::fs::write(&shim, script).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    shim
}

/// Write an executable shim that ignores its args and sleeps past the timeout.
fn write_slow_shim(tmp: &Tmp) -> PathBuf {
    write_shim(tmp, "slow-gwz", "#!/bin/sh\nsleep 5\n")
}

#[tokio::test(flavor = "multi_thread")]
async fn timeout_fails_as_data() {
    let tmp = Tmp::new("timeout");
    let (mut node, port) = boot(&tmp).await;
    let url = format!("ws://127.0.0.1:{port}");

    // point the supplier at a SLOW SHIM (marked: not real gwz) with a short
    // timeout, so `status` — allow-listed — trips the timeout deterministically.
    let mut cfg = config_for(&url, tmp.path().join("ws"));
    cfg.gwz_bin = write_slow_shim(&tmp);
    cfg.timeout = Duration::from_millis(200);
    let _sup = serve(cfg).await.unwrap();

    let requester = GladeClient::new("requester");
    requester.connect(&url).await.unwrap();

    // the first answer arrives (as a timeout) — poll for a decoded response.
    let timed_out = poll(|| {
        let r = requester.clone();
        async move {
            match r
                .exchange("ws-razel", "gwz.ops", br#"{"verb":"status"}"#.to_vec())
                .await
            {
                Ok(o) if o.ok => {
                    let resp: GwzResponse =
                        serde_json::from_slice(&o.payload.unwrap_or_default()).unwrap_or_default();
                    !resp.ok && resp.error.as_deref().unwrap_or("").contains("timed out")
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(timed_out, "a slow command answers with a timeout, as data");

    requester.close().await;
    node.kill().await.ok();
}

// ---- 4. streaming output visible to a subscriber ---------------------------

#[tokio::test(flavor = "multi_thread")]
async fn streaming_output_visible_to_subscriber() {
    let tmp = Tmp::new("stream");
    let (mut node, port) = boot(&tmp).await;
    let url = format!("ws://127.0.0.1:{port}");
    let ws = make_gwz_workspace(&tmp);
    let _sup = serve(config_for(&url, ws)).await.unwrap();

    let requester = GladeClient::new("requester");
    requester.connect(&url).await.unwrap();
    let r = requester.clone();
    assert!(
        poll(|| {
            let r = r.clone();
            async move {
                r.exchange("ws-razel", "gwz.ops", br#"{"verb":"status"}"#.to_vec())
                    .await
                    .map(|o| o.ok)
                    .unwrap_or(false)
            }
        })
        .await
    );

    // a streaming run answers immediately with the run id.
    let accepted = ask(&requester, r#"{"verb":"status","stream":true}"#).await;
    assert!(
        accepted.ok && accepted.done == Some(false),
        "streaming accept: {accepted:?}"
    );
    let run_id = accepted.run_id.expect("run_id on the accept");

    // a subscriber on the output surface, keyed by run id, converges the run's
    // output ops + the terminal marker (from-cursor backfill covers timing).
    let sub = GladeClient::new("subscriber");
    sub.connect(&url).await.unwrap();
    sub.subscribe("ws-razel", "gwz.output", Some(run_id.as_bytes()))
        .await
        .unwrap();

    let s = sub.clone();
    let key = run_id.clone();
    let converged = poll(|| {
        let s = s.clone();
        let key = key.clone();
        async move {
            let entries = s
                .fold_log("ws-razel", "gwz.output", Some(key.as_bytes()))
                .await;
            entries.iter().any(|e| {
                serde_json::from_slice::<GwzOutputRecord>(e)
                    .map(|r| r.done == Some(true))
                    .unwrap_or(false)
            })
        }
    })
    .await;
    assert!(
        converged,
        "the streaming output + done marker reached the subscriber"
    );

    // decode the run: at least one output line, a terminal marker carrying
    // exit 0, and every record stamped with the acting principal.
    let entries = sub
        .fold_log("ws-razel", "gwz.output", Some(run_id.as_bytes()))
        .await;
    let recs: Vec<GwzOutputRecord> = entries
        .iter()
        .filter_map(|e| serde_json::from_slice(e).ok())
        .collect();
    assert!(
        recs.iter()
            .any(|r| r.stream == "stdout" && r.line.is_some()),
        "an output line: {recs:?}"
    );
    let end = recs
        .iter()
        .find(|r| r.done == Some(true))
        .expect("terminal marker");
    assert_eq!(end.exit, Some(0), "gwz status exited clean: {end:?}");
    assert!(
        recs.iter()
            .all(|r| r.principal.as_deref() == Some("gianni")),
        "run records attributed: {recs:?}"
    );
    // the run records are keyed by run id.
    assert!(recs.iter().all(|r| r.run_id == run_id));

    sub.close().await;
    requester.close().await;
    node.kill().await.ok();
}

// ---- 5. the binary attaches, answers, and shuts down on SIGTERM ------------

#[tokio::test(flavor = "multi_thread")]
async fn binary_serves_and_shuts_down_on_sigterm() {
    let tmp = Tmp::new("bin");
    let (mut node, port) = boot(&tmp).await;
    let url = format!("ws://127.0.0.1:{port}");
    let ws = make_gwz_workspace(&tmp);

    let mut supplier = Command::new(env!("CARGO_BIN_EXE_glade-gwz"))
        .args(["--node", &url, "--root"])
        .arg(&ws)
        .args(["--share", "ws-razel", "--principal", "tester", "--gwz-bin"])
        .arg(gwz_bin())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn glade-gwz binary");
    let pid = supplier.id().expect("binary pid");

    let requester = GladeClient::new("requester");
    requester.connect(&url).await.unwrap();

    // the binary attached: a real gwz.status round-trips through it.
    let answered = poll(|| {
        let r = requester.clone();
        async move {
            match r
                .exchange("ws-razel", "gwz.ops", br#"{"verb":"status"}"#.to_vec())
                .await
            {
                Ok(o) if o.ok => {
                    let resp: GwzResponse =
                        serde_json::from_slice(&o.payload.unwrap_or_default()).unwrap_or_default();
                    resp.ok && resp.attributed_to.as_deref() == Some("tester")
                }
                _ => false,
            }
        }
    })
    .await;
    assert!(
        answered,
        "the glade-gwz binary attached and answered a real gwz.status"
    );

    // SIGTERM -> clean shutdown (exit 0).
    let killed = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("send SIGTERM");
    assert!(killed.success(), "sent SIGTERM");
    let status = tokio::time::timeout(Duration::from_secs(10), supplier.wait())
        .await
        .expect("binary exited after SIGTERM")
        .expect("wait");
    assert!(status.success(), "clean shutdown exit 0, got {status:?}");

    requester.close().await;
    node.kill().await.ok();
}

// ---- 6. a restarted supplier streams its first run -------------------------

/// Poll until the attached supplier answers a plain `status`, as the tests
/// above do before their first real request.
async fn answering(requester: &GladeClient) -> bool {
    poll(|| {
        let r = requester.clone();
        async move {
            r.exchange("ws-razel", "gwz.ops", br#"{"verb":"status"}"#.to_vec())
                .await
                .map(|o| o.ok)
                .unwrap_or(false)
        }
    })
    .await
}

/// Start a streaming `status`; the run id the supplier answers with.
async fn stream_status(requester: &GladeClient) -> String {
    let accepted = ask(requester, r#"{"verb":"status","stream":true}"#).await;
    assert!(
        accepted.ok && accepted.done == Some(false),
        "streaming accept: {accepted:?}"
    );
    accepted.run_id.expect("run_id on the accept")
}

/// A FRESH subscriber on one run's key, folded until a terminal marker is
/// there: what the node holds under that run id, decoded.
async fn follow_run(url: &str, run_id: &str) -> Vec<GwzOutputRecord> {
    let sub = GladeClient::new("subscriber");
    sub.connect(url).await.unwrap();
    sub.subscribe("ws-razel", "gwz.output", Some(run_id.as_bytes()))
        .await
        .unwrap();
    let ended = poll(|| {
        let s = sub.clone();
        let key = run_id.to_string();
        async move {
            let entries = s
                .fold_log("ws-razel", "gwz.output", Some(key.as_bytes()))
                .await;
            entries.iter().any(|e| {
                serde_json::from_slice::<GwzOutputRecord>(e)
                    .map(|r| r.done == Some(true))
                    .unwrap_or(false)
            })
        }
    })
    .await;
    assert!(
        ended,
        "run {run_id}: a terminal marker reached the subscriber"
    );
    let entries = sub
        .fold_log("ws-razel", "gwz.output", Some(run_id.as_bytes()))
        .await;
    sub.close().await;
    entries
        .iter()
        .filter_map(|e| serde_json::from_slice(e).ok())
        .collect()
}

/// A restart is a new supplier process under the SAME origin (`serve` names the
/// session after its share and exchange), and the node keeps every run's output
/// chain across it. A restarted supplier that reused an earlier process's run id
/// would put its first run on that run's chain: the node holds the records that
/// match byte for byte and refuses the rest, unseen, so a subscriber on the new
/// id folds the OLD run's output. The two runs must therefore differ: the
/// workspace gains a file while no supplier runs, and only the second `gwz
/// status` can name it.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_supplier_streams_its_first_run() {
    const NEW_FILE: &str = "between-the-runs.txt";
    let tmp = Tmp::new("restart");
    let (mut node, port) = boot(&tmp).await;
    let url = format!("ws://127.0.0.1:{port}");
    let ws = make_gwz_workspace(&tmp);

    // The desk's side stays connected across the restart.
    let requester = GladeClient::new("requester");
    requester.connect(&url).await.unwrap();

    // ---- process one: a run streamed to its end, then shut down ------------
    let first_supplier = serve(config_for(&url, ws.clone())).await.unwrap();
    assert!(
        answering(&requester).await,
        "the first supplier attached and answered"
    );
    let first = stream_status(&requester).await;
    let first_recs = follow_run(&url, &first).await;
    assert!(
        first_recs.iter().any(|r| r.line.is_some()),
        "the first run streamed its lines: {first_recs:?}"
    );
    first_supplier.shutdown().await;

    std::fs::write(ws.join(NEW_FILE), "made while no supplier ran\n").unwrap();

    // ---- process two: a fresh supplier, the same node, the same origin -----
    let _second_supplier = serve(config_for(&url, ws)).await.unwrap();
    assert!(
        answering(&requester).await,
        "the restarted supplier attached and answered"
    );
    let second = stream_status(&requester).await;
    let second_recs = follow_run(&url, &second).await;

    let names_new_file = second_recs
        .iter()
        .any(|r| r.stream == "stdout" && r.line.as_deref().is_some_and(|l| l.contains(NEW_FILE)));
    assert!(
        names_new_file,
        "a subscriber on the restarted supplier's run `{second}` folds that run's lines, \
         not those of the first process's run `{first}`: {second_recs:?}"
    );
    let end = second_recs
        .iter()
        .find(|r| r.done == Some(true))
        .expect("terminal marker");
    assert_eq!(
        end.exit,
        Some(0),
        "the second gwz status exited clean: {end:?}"
    );
    assert!(
        second_recs.iter().all(|r| r.run_id == second),
        "keyed by the second run's id: {second_recs:?}"
    );
    assert_ne!(
        first, second,
        "a restarted supplier never reuses an earlier process's run id"
    );

    requester.close().await;
    node.kill().await.ok();
}

// ---- 7. a record the node refuses is said -----------------------------------

/// A shim (marked: not the real `gwz`) that ignores its args, waits until the
/// test opens `gate`, for ten seconds at most, and then prints one line: so the
/// test can take the run's output zone before the run's first record.
fn write_gated_shim(tmp: &Tmp, gate: &Path) -> PathBuf {
    let script = format!(
        "#!/bin/sh\nn=0\nwhile [ ! -e '{}' ] && [ \"$n\" -lt 200 ]; do\n  sleep 0.05\n  \
         n=$((n + 1))\ndone\necho 'after the squatter'\n",
        gate.display()
    );
    write_shim(tmp, "gated-gwz", &script)
}

/// Every op the node refuses is said on stderr, with its chain, its seq and its
/// code (client-writes plan, Step 4.2). The node refuses here for a reason the
/// supplier cannot see coming: a test client puts a `crdt` op on the run's
/// output zone before the run's first line, and the node refuses a `log` op on a
/// `crdt` zone as a shape conflict (`node/src/store.rs`). The BINARY runs,
/// because what is under test is what it says on stderr.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_the_node_refuses_is_reported() {
    let tmp = Tmp::new("refused");
    let (mut node, port) = boot(&tmp).await;
    let url = format!("ws://127.0.0.1:{port}");
    // The shim never reads the root, so an empty directory serves.
    let ws = tmp.path().join("ws");
    std::fs::create_dir_all(&ws).unwrap();
    let gate = tmp.path().join("gate");

    let mut supplier = Command::new(env!("CARGO_BIN_EXE_glade-gwz"))
        .args(["--node", &url, "--root"])
        .arg(&ws)
        .args(["--share", "ws-razel", "--principal", "tester", "--gwz-bin"])
        .arg(write_gated_shim(&tmp, &gate))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn glade-gwz binary");
    let pid = supplier.id().expect("binary pid");
    let mut lines = BufReader::new(supplier.stderr.take().expect("piped stderr")).lines();

    // Attached: a verb outside the allow-list is answered as data, and runs no
    // shim.
    let requester = GladeClient::new("requester");
    requester.connect(&url).await.unwrap();
    let attached = poll(|| {
        let r = requester.clone();
        async move {
            r.exchange("ws-razel", "gwz.ops", br#"{"verb":"commit"}"#.to_vec())
                .await
                .map(|o| o.ok)
                .unwrap_or(false)
        }
    })
    .await;
    assert!(attached, "the glade-gwz binary attached and answered");

    // The run is accepted and its shim waits at the gate, while a squatter
    // takes the run's output zone as a crdt one.
    let run_id = stream_status(&requester).await;
    let squatter = GladeClient::new("squatter");
    squatter.connect(&url).await.unwrap();
    let (_, held) = squatter
        .append_outcome(
            "ws-razel",
            "gwz.output",
            "crdt",
            b"{}".to_vec(),
            Some(run_id.as_bytes()),
        )
        .await
        .expect("the squatter's op went out");
    assert_eq!(
        held,
        OpOutcome::Accepted,
        "the run's zone is a crdt one now"
    );
    std::fs::write(&gate, b"").unwrap();

    // Everything it says, up to the line naming the refusal of the run's first
    // record.
    let named = format!("the node refused seq 0 of ws-razel/gwz.output[{run_id}]");
    let mut said: Vec<String> = Vec::new();
    let found = tokio::time::timeout(Duration::from_secs(10), async {
        while let Ok(Some(line)) = lines.next_line().await {
            let found = line.contains(&named);
            said.push(line);
            if found {
                return true;
            }
        }
        false
    })
    .await;
    assert!(
        matches!(found, Ok(true)),
        "stderr names the refusal of the run's first record: {said:#?}"
    );
    let refusal = said.last().expect("the refusal's line");
    assert!(
        refusal.contains("Protocol") && refusal.contains("shape conflict"),
        "the refusal names its code and the node's reason: {refusal}"
    );

    let killed = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("send SIGTERM");
    assert!(killed.success(), "sent SIGTERM");
    let status = tokio::time::timeout(Duration::from_secs(10), supplier.wait())
        .await
        .expect("binary exited after SIGTERM")
        .expect("wait");
    assert!(status.success(), "clean shutdown exit 0, got {status:?}");

    squatter.close().await;
    requester.close().await;
    node.kill().await.ok();
}

// ---- 8-10. gwz runs with the captured environment, and only that ------------

/// The made-up variable whose VALUE the env shim prints.
const PROBE: &str = "GLADE_GWZ_TEST_PROBE";

/// A shim (marked: not the real `gwz`), written to `name` in the test's temp
/// dir, that ignores its args and prints `name <NAME>` for each variable in its
/// environment, then `probe <value of PROBE>`. It prints no other value: a real
/// environment can hold secrets.
fn write_env_shim(tmp: &Tmp, name: &str) -> PathBuf {
    let script = format!(
        "#!/bin/sh\nexec /usr/bin/awk 'BEGIN {{ for (k in ENVIRON) print \"name \" k; \
         print \"probe \" ENVIRON[\"{PROBE}\"] }}'\n"
    );
    write_shim(tmp, name, &script)
}

/// What the env shim printed: the names it saw, and the probe's value.
fn read_env_shim<'a>(
    lines: impl IntoIterator<Item = &'a str>,
) -> (BTreeSet<String>, Option<String>) {
    let mut names = BTreeSet::new();
    let mut probe = None;
    for line in lines {
        if let Some(name) = line.strip_prefix("name ") {
            names.insert(name.to_string());
        } else if let Some(value) = line.strip_prefix("probe ") {
            probe = Some(value.to_string());
        }
    }
    (names, probe)
}

/// The names the shim's own shell adds (`PWD`, `SHLVL`, …; shells differ):
/// what it prints when started directly with an empty environment.
fn shell_added_names(shim: &Path) -> BTreeSet<String> {
    let out = std::process::Command::new(shim)
        .env_clear()
        .output()
        .expect("run the env shim");
    read_env_shim(String::from_utf8_lossy(&out.stdout).lines()).0
}

/// What gwz saw, started each way a request can start it: a blocking `status`,
/// then a streamed one.
async fn environments_seen(
    requester: &GladeClient,
    url: &str,
) -> [(BTreeSet<String>, Option<String>); 2] {
    let ran = ask(requester, r#"{"verb":"status"}"#).await;
    assert!(ran.ok && ran.exit == Some(0), "the blocking run: {ran:?}");
    let blocking = read_env_shim(ran.stdout.lines());
    let run_id = stream_status(requester).await;
    let recs = follow_run(url, &run_id).await;
    let lines = recs.iter().filter(|r| r.stream == "stdout");
    let streamed = read_env_shim(lines.filter_map(|r| r.line.as_deref()));
    [blocking, streamed]
}

/// Both runners start gwz from an empty environment plus the config's snapshot
/// (ProcessGlobalsPlan Step 3.2). The snapshot is made up, so this process's
/// own variables, its `HOME` and `PATH` among them, are the ones that must not
/// arrive.
#[tokio::test(flavor = "multi_thread")]
async fn gwz_gets_only_the_configured_environment() {
    let tmp = Tmp::new("env-only");
    let (mut node, port) = boot(&tmp).await;
    let url = format!("ws://127.0.0.1:{port}");
    let shim = write_env_shim(&tmp, "env-gwz");
    let mut cfg = config_for(&url, tmp.path().join("ws"));
    cfg.gwz_bin = shim.clone();
    cfg.env = Environment::from_vars([(PROBE, "made-up probe"), ("GLADE_GWZ_TEST_TOO", "made up")]);
    let _sup = serve(cfg).await.unwrap();
    let mut expected = shell_added_names(&shim);
    expected.extend([PROBE, "GLADE_GWZ_TEST_TOO"].map(String::from));

    let requester = GladeClient::new("requester");
    requester.connect(&url).await.unwrap();
    assert!(
        answering(&requester).await,
        "the gwz supplier attached and answered"
    );
    let seen = environments_seen(&requester, &url).await;
    for (run, (names, probe)) in ["blocking", "streamed"].into_iter().zip(seen) {
        assert_eq!(
            names, expected,
            "{run}: gwz saw exactly the config's variables"
        );
        assert_eq!(
            probe.as_deref(),
            Some("made-up probe"),
            "{run}: and their values"
        );
    }

    requester.close().await;
    node.kill().await.ok();
}

/// A variable set in the process after the environment was captured never
/// reaches gwz; one set before the capture does. Both are made up.
#[tokio::test(flavor = "multi_thread")]
async fn a_variable_set_after_the_capture_never_reaches_gwz() {
    const BEFORE: &str = "GLADE_GWZ_TEST_SET_BEFORE_THE_CAPTURE";
    const AFTER: &str = "GLADE_GWZ_TEST_SET_AFTER_THE_CAPTURE";
    let tmp = Tmp::new("env-after");
    let (mut node, port) = boot(&tmp).await;
    let url = format!("ws://127.0.0.1:{port}");

    std::env::set_var(BEFORE, "made up");
    let captured = Environment::from_vars(std::env::vars_os());
    std::env::set_var(AFTER, "made up");
    let mut cfg = config_for(&url, tmp.path().join("ws"));
    cfg.gwz_bin = write_env_shim(&tmp, "env-gwz");
    cfg.env = captured;
    let _sup = serve(cfg).await.unwrap();

    let requester = GladeClient::new("requester");
    requester.connect(&url).await.unwrap();
    assert!(
        answering(&requester).await,
        "the gwz supplier attached and answered"
    );
    let seen = environments_seen(&requester, &url).await;
    std::env::remove_var(BEFORE);
    std::env::remove_var(AFTER);
    for (run, (names, _)) in ["blocking", "streamed"].into_iter().zip(seen) {
        assert!(
            names.contains(BEFORE),
            "{run}: a variable there at the capture reaches gwz"
        );
        assert!(
            !names.contains(AFTER),
            "{run}: a variable set after the capture reached gwz"
        );
    }

    requester.close().await;
    node.kill().await.ok();
}

/// The binary captures its whole start-up environment once, in `main`, and gwz
/// runs with exactly that, as it did when it inherited it: `PATH`, on which the
/// default `--gwz-bin gwz` is found as grazel starts the binary, `HOME` and, for
/// SSH remotes, `SSH_AUTH_SOCK`. The binary starts with the test's variables
/// alone, so the test can name every one gwz should see.
#[tokio::test(flavor = "multi_thread")]
async fn the_binary_gives_gwz_its_start_up_environment() {
    let tmp = Tmp::new("env-bin");
    let (mut node, port) = boot(&tmp).await;
    let url = format!("ws://127.0.0.1:{port}");
    let ws = tmp.path().join("ws");
    let bin_dir = tmp.path().join("bin");
    std::fs::create_dir_all(&bin_dir).unwrap();
    let shim = write_env_shim(&tmp, "bin/gwz");

    let mut supplier = Command::new(env!("CARGO_BIN_EXE_glade-gwz"))
        .args(["--node", &url, "--root"])
        .arg(&ws)
        .args(["--share", "ws-razel", "--principal", "tester"])
        .env_clear()
        .env("PATH", &bin_dir)
        .env("HOME", "/made-up/home")
        .env("SSH_AUTH_SOCK", "/made-up/agent.sock")
        .env(PROBE, "made-up probe")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn glade-gwz binary");
    let mut expected = shell_added_names(&shim);
    expected.extend(["PATH", "HOME", "SSH_AUTH_SOCK", PROBE].map(String::from));

    let requester = GladeClient::new("requester");
    requester.connect(&url).await.unwrap();
    assert!(
        answering(&requester).await,
        "the glade-gwz binary attached and answered"
    );
    let seen = environments_seen(&requester, &url).await;
    for (run, (names, probe)) in ["blocking", "streamed"].into_iter().zip(seen) {
        assert_eq!(
            names, expected,
            "{run}: gwz's environment is the binary's start-up one"
        );
        assert_eq!(
            probe.as_deref(),
            Some("made-up probe"),
            "{run}: and its values"
        );
    }

    requester.close().await;
    supplier.kill().await.ok();
    node.kill().await.ok();
}
