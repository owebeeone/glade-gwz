//! The gwz supplier (GLP-0006 P1.S2) — the authority-side module standing behind
//! the gwz command surface. It attaches over the wire as an ordinary authority
//! session (`GladeSupplierModel.md` §2, P00-a) through [`glade_client`], and
//! serves BOTH node mechanisms the model names:
//!
//! * **exchange** (`(share, glade_id)`, default `(ws-razel, gwz.ops)`) — the
//!   command surface. Each `ExchangeReq` payload is a [`GwzRequest`]; the answer
//!   is a [`GwzResponse`]. Allow-listed read verbs run against the CONFIGURED
//!   root (app-owned storage, §5); disallowed verbs / bad envelopes / timeouts
//!   are failure-as-DATA.
//! * **log** (`(share, output_id)`, default `(ws-razel, gwz.output)`) — long-op
//!   output. A `stream:true` request answers immediately with `{run_id,
//!   done:false}` and the run's stdout/stderr lines are APPENDED as ops to the
//!   log surface keyed by `run_id`, closed by a `{done:true, exit}` marker.
//!
//! Reattach-on-drop + clean detach come from the kit's [`Supplier`]; the crate
//! holds zero node internals (the wire + a client lib only). Every op the node
//! refuses is said on stderr ([`say_refusals`]).

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use glade_client::supplier::{Supplier, SupplierConfig, SupplierSurface};
use glade_client::{GladeClient, OpStatus};
use glade_wire::generated::ExchangeReq;

use crate::envelope::{GwzOutputRecord, GwzRequest, GwzResponse};
use crate::exec;

/// The default exchange surface a gwz supplier stands behind (discovery.ts
/// phase D; `grazel-app.glade` `service grazel gwz.ops`).
pub const DEFAULT_SHARE: &str = "ws-razel";
pub const DEFAULT_GLADE_ID: &str = "gwz.ops";
pub const DEFAULT_OUTPUT_ID: &str = "gwz.output";
pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Everything the supplier needs to attach + serve. `root` and `gwz_bin` are the
/// app's (never the request's); `share`/`glade_id`/`output_id` are the declared
/// surfaces; `principal` is the attribution identity (Hello + run stamp).
#[derive(Clone, Debug)]
pub struct GwzConfig {
    pub node_url: String,
    pub share: String,
    pub glade_id: String,
    pub output_id: String,
    pub root: PathBuf,
    pub gwz_bin: PathBuf,
    pub principal: Option<String>,
    pub timeout: Duration,
}

impl GwzConfig {
    /// A config with the defaulted surfaces + timeout, given the required node,
    /// root, and gwz binary.
    pub fn new(node_url: impl Into<String>, root: PathBuf) -> GwzConfig {
        GwzConfig {
            node_url: node_url.into(),
            share: DEFAULT_SHARE.into(),
            glade_id: DEFAULT_GLADE_ID.into(),
            output_id: DEFAULT_OUTPUT_ID.into(),
            root,
            gwz_bin: PathBuf::from("gwz"),
            principal: None,
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        }
    }
}

/// A live gwz supplier: an attached authority session serving the command
/// exchange (and appending long-op output to the log surface). Hold it for the
/// process lifetime; [`GwzSupplier::shutdown`] is the clean teardown.
pub struct GwzSupplier {
    #[allow(dead_code)]
    client: GladeClient,
    supplier: Supplier,
}

impl GwzSupplier {
    /// Stop reattaching and close the session (SIGTERM path).
    pub async fn shutdown(&self) {
        self.supplier.detach_all().await;
    }
}

/// Connect to the node, attach as the gwz authority, and start serving the
/// command exchange. Resolves once the provider is registered (the Subscribe
/// ack); it then answers requests + streams output until [`GwzSupplier::shutdown`]
/// or the process ends.
pub async fn serve(config: GwzConfig) -> io::Result<GwzSupplier> {
    let config = Arc::new(config);
    let client = GladeClient::new(format!("glade-gwz:{}:{}", config.share, config.glade_id));
    client.connect(&config.node_url).await?;

    let supplier = Supplier::attach(
        client.clone(),
        SupplierConfig { principal: config.principal.clone(), ..Default::default() },
    );

    // Listening before anything is written: the output records go out
    // fire-and-forget, and this is where their refusals land (client-writes
    // plan, Step 4.2).
    tokio::spawn(say_refusals(client.on_refused().await));

    let handler = make_handler(client.clone(), config.clone(), Handle::current());
    supplier
        .serve_exchange(SupplierSurface::new(&config.share, &config.glade_id, "exchange"), handler)
        .await?;

    Ok(GwzSupplier { client, supplier })
}

/// Build the exchange handler closure. It is a synchronous `Fn` (the kit's
/// contract): it parses + guards the envelope, then either runs a read verb
/// BLOCKING and answers with the result, or (for `stream:true`) spawns the async
/// streaming task and answers immediately with the run id. Every outcome is a
/// structured [`GwzResponse`] — the handler never returns `Err`, so the WIRE
/// `ExchangeRes.ok` stays `true` and the PAYLOAD carries success/failure.
fn make_handler(
    client: GladeClient,
    config: Arc<GwzConfig>,
    handle: Handle,
) -> impl Fn(&ExchangeReq) -> Result<Vec<u8>, String> + Send + Sync + 'static {
    let runs = Runs::new();
    move |req: &ExchangeReq| -> Result<Vec<u8>, String> {
        let resp = answer(&client, &config, &handle, &runs, &req.payload);
        Ok(resp.to_bytes())
    }
}

/// The run ids one supplier process mints (the client-writes plan's F4), as
/// glade-gyld's `Runs` does.
///
/// A counter alone is not enough. Every supplier process writes under the same
/// origin (`serve`), a run's output goes on the `gwz.output` log keyed by its
/// run id, and the node keeps that log across a restart. A counter that restarts
/// with the process gives a fresh run an id an earlier process spent. The run's
/// records then land on that run's chain: the node refuses every one from the
/// first that differs, and a subscriber on the id folds the earlier run's
/// output. The session tag makes the id unique across restarts; the counter
/// orders the runs within one process.
struct Runs {
    /// Read once, when the supplier starts serving; different in the next process.
    session: String,
    next: AtomicU64,
}

impl Runs {
    fn new() -> Runs {
        Runs {
            session: session_tag(),
            next: AtomicU64::new(0),
        }
    }

    /// The next run's id: distinct from every other id this process mints, and
    /// from every earlier process's.
    fn mint(&self) -> String {
        mint_run_id(&self.session, self.next.fetch_add(1, Ordering::SeqCst) + 1)
    }
}

/// `run-<session>-<n>`. Opaque to readers: the desk keys the log by the whole
/// string (`gryth-ui/packages/plugins/gwz/src/live.ts`) and parses none of it.
fn mint_run_id(session: &str, n: u64) -> String {
    format!("run-{session}-{n}")
}

/// This process's tag: milliseconds since the epoch in base36, as glade-gyld's
/// `session_tag`. It is eight characters until 2059, and at one width it sorts
/// as text the way it sorts in time. A clock before the epoch gives `0`.
fn session_tag() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    base36(millis)
}

/// `n` in lowercase base36, most significant digit first.
fn base36(mut n: u128) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".to_string();
    }
    let mut out = Vec::new();
    while n > 0 {
        out.push(DIGITS[(n % 36) as usize] as char);
        n /= 36;
    }
    out.reverse();
    out.into_iter().collect()
}

/// The command decision (pure w.r.t. the wire): parse → guard → run / stream.
fn answer(
    client: &GladeClient,
    config: &Arc<GwzConfig>,
    handle: &Handle,
    runs: &Runs,
    payload: &[u8],
) -> GwzResponse {
    let req = match GwzRequest::parse(payload) {
        Ok(r) => r,
        Err(e) => return GwzResponse::failed(e),
    };
    if req.verb.is_empty() {
        return GwzResponse::failed("envelope missing `verb`");
    }
    // Attribution: the request's principal, else the supplier's configured one.
    let who = req.principal.clone().or_else(|| config.principal.clone());

    if !exec::verb_allowed(&req.verb) {
        return GwzResponse::failed(format!(
            "verb `{}` not in stage-1 allow-list {:?}",
            req.verb, exec::ALLOWED_VERBS
        ));
    }
    if let Some(bad) = exec::first_denied_arg(&req.args) {
        return GwzResponse::failed(format!(
            "arg `{bad}` not permitted — the workspace root/scope is app-owned"
        ));
    }

    if req.stream {
        let run_id = runs.mint();
        spawn_stream(client.clone(), config.clone(), handle.clone(), run_id.clone(), req, who.clone());
        return GwzResponse::accepted(run_id, who);
    }

    match exec::run_blocking(&config.gwz_bin, &config.root, &req.verb, &req.args, config.timeout) {
        Ok(o) => GwzResponse::ran(o.exit, o.stdout, o.stderr, who),
        Err(e) => GwzResponse::failed(e),
    }
}

/// Spawn the long-op streaming task: run `gwz` async, append each stdout/stderr
/// line as a LOG op keyed by `run_id`, then a terminal `{done:true, exit}` op.
/// Best-effort — an append failure (link drop mid-run) is dropped; the exchange
/// answer already carried the run id.
fn spawn_stream(
    client: GladeClient,
    config: Arc<GwzConfig>,
    handle: Handle,
    run_id: String,
    req: GwzRequest,
    who: Option<String>,
) {
    handle.spawn(async move {
        let argv = exec::argv(&config.root, &req.verb, &req.args);
        let mut cmd = tokio::process::Command::new(&config.gwz_bin);
        cmd.args(&argv)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                let rec = GwzOutputRecord::line(&run_id, 1, &who, "stderr", format!("failed to spawn {}: {e}", config.gwz_bin.display()));
                append_output(&client, &config, &run_id, &rec).await;
                append_output(&client, &config, &run_id, &GwzOutputRecord::end(&run_id, 2, &who, -1)).await;
                return;
            }
        };

        // Drain both pipes concurrently (a reader task each → an mpsc), so a
        // chatty stream cannot deadlock the other pipe.
        let (tx, mut rx) = mpsc::unbounded_channel::<(&'static str, String)>();
        if let Some(so) = child.stdout.take() {
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(so).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if tx.send(("stdout", line)).is_err() {
                        break;
                    }
                }
            });
        }
        if let Some(se) = child.stderr.take() {
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(se).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if tx.send(("stderr", line)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx); // rx closes once both readers finish.

        let mut seq: u64 = 0;
        while let Some((stream, line)) = rx.recv().await {
            seq += 1;
            append_output(&client, &config, &run_id, &GwzOutputRecord::line(&run_id, seq, &who, stream, line)).await;
        }

        let exit = child.wait().await.ok().and_then(|s| s.code()).unwrap_or(-1);
        seq += 1;
        append_output(&client, &config, &run_id, &GwzOutputRecord::end(&run_id, seq, &who, exit)).await;
    });
}

/// Append one output record to the log surface, keyed by run id (the value/log
/// serve act — an op the node folds + replicates to subscribers, §2). A record
/// the node refuses is said by [`say_refusals`].
async fn append_output(client: &GladeClient, config: &GwzConfig, run_id: &str, rec: &GwzOutputRecord) {
    let _ = client
        .append(&config.share, &config.output_id, "log", rec.to_bytes(), Some(run_id.as_bytes()))
        .await;
}

/// Say every op the node refuses, with its chain, its seq and its code, until
/// the client is gone (client-writes plan, Step 4.2). The client has already
/// dropped a refused record with the later records of its chain, and appends
/// none there until a subscribe of its zone, which this supplier never makes:
/// the rest of that run's output goes nowhere.
async fn say_refusals(mut refused: mpsc::UnboundedReceiver<OpStatus>) {
    while let Some(OpStatus { op, code, message }) = refused.recv().await {
        let chain = if op.key.is_empty() {
            format!("{}/{}", op.share, op.glade_id)
        } else {
            let key = String::from_utf8_lossy(&op.key);
            format!("{}/{}[{key}]", op.share, op.glade_id)
        };
        eprintln!(
            "glade-gwz: the node refused seq {} of {chain}: {code:?}, {message}; the rest of \
             that chain is dropped",
            op.seq
        );
    }
}
