//! gwz execution policy + the blocking runner (GLP-0006 P1.S2).
//!
//! Two guards stand between a request and the `gwz` process:
//!
//! 1. [`ALLOWED_VERBS`] — the stage-1 read-only allow-list. Everything mutating
//!    is refused as data. Stage-2 replaces this list with per-verb GRANTS (the
//!    s-verbs taxonomy; `gwz.*` seeds already ride `grazel-app.glade`).
//! 2. [`first_denied_arg`] — even for an allowed verb, a request may not supply
//!    the scope / force global levers (`--root`, `--target`, `--force`, …): the
//!    supplier prepends `--root <config root>` and it stays authoritative.
//!
//! [`run_blocking`] runs `gwz` to completion with a hard timeout, draining both
//! pipes on threads so a chatty child never deadlocks. It is BLOCKING by design:
//! the kit's exchange handler is a synchronous `Fn` (`Supplier::serve_exchange`),
//! and read-only verbs finish well under the timeout; per-surface answers already
//! serialize at the authority (the `workspace.lock` in discovery.ts phase D). The
//! long / streaming path is async and lives in [`crate::supplier`].
//!
//! Both start gwz through [`command`]: from an empty environment plus the one
//! `main` captured at start, never this process's live one.

use std::io::Read;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use crate::environment::Environment;

/// Stage-1 read-only allow-list. `status` / `ls` / `diff` are the pure read
/// verbs (exit 0; no member mutation, no lock write, no arbitrary exec).
///
/// EXCLUDED and why: `forall` (runs an ARBITRARY command — the sharpest surface),
/// `capture` / `snapshot` (write the workspace lock), `add` / `commit` / `pull` /
/// `push` / `clone` / `init` / `materialize` / `branch` / `tag` / `repo` /
/// `stash` (mutate members or metadata).
pub const ALLOWED_VERBS: &[&str] = &["status", "ls", "diff"];

/// Global-option tokens a REQUEST may not carry: they redirect the workspace
/// scope or force destructive behavior. The root/scope is the app's (§5).
const DENIED_ARGS: &[&str] = &[
    "--root",
    "--force",
    "--target",
    "--no-target",
    "--member",
    "--no-member",
    "--member-path",
    "--no-member-path",
    "--all",
];

/// Is `verb` on the stage-1 allow-list?
pub fn verb_allowed(verb: &str) -> bool {
    ALLOWED_VERBS.contains(&verb)
}

/// The first request arg that redirects scope / forces destruction, if any
/// (matches both `--flag` and `--flag=value` forms).
pub fn first_denied_arg(args: &[String]) -> Option<String> {
    args.iter()
        .find(|a| {
            let head = a.split('=').next().unwrap_or(a);
            DENIED_ARGS.contains(&head)
        })
        .cloned()
}

/// The argv the supplier controls: `--root <root>` FIRST (authoritative), then
/// the verb and its args. Shared by the blocking + streaming runners so the
/// command shape lives in one place.
pub fn argv(root: &Path, verb: &str, args: &[String]) -> Vec<String> {
    let mut v = vec!["--root".to_string(), root.display().to_string(), verb.to_string()];
    v.extend(args.iter().cloned());
    v
}

/// The one `gwz` process both runners start: `gwz --root <root> <verb> <args…>`
/// ([`argv`]), stdin closed, both output pipes captured. gwz inherits nothing
/// from this process's environment: it gets `env_clear()`, then exactly `env`,
/// the snapshot `main` captured at start (ProcessGlobalsPlan Step 3.2). The
/// streaming runner turns it into a `tokio::process::Command`.
pub fn command(
    gwz_bin: &Path,
    env: &Environment,
    root: &Path,
    verb: &str,
    args: &[String],
) -> std::process::Command {
    let mut cmd = std::process::Command::new(gwz_bin);
    cmd.args(argv(root, verb, args))
        .env_clear()
        .envs(env.vars())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// A finished blocking run.
#[derive(Debug)]
pub struct RunOutput {
    pub exit: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Run `gwz --root <root> <verb> <args…>` to completion, blocking, with a hard
/// `timeout`, in the environment `env` ([`command`]). On timeout the child is
/// killed and `Err("timed out …")` is returned (failure as data at the call
/// site). A spawn failure is likewise an `Err`. Pipes drain on threads so output
/// never deadlocks the wait.
pub fn run_blocking(
    gwz_bin: &Path,
    env: &Environment,
    root: &Path,
    verb: &str,
    args: &[String],
    timeout: Duration,
) -> Result<RunOutput, String> {
    let mut child = command(gwz_bin, env, root, verb, args)
        .spawn()
        .map_err(|e| format!("failed to spawn {}: {e}", gwz_bin.display()))?;

    // Drain both pipes concurrently so a chatty child cannot block on a full
    // pipe while we wait (classic wait-then-read deadlock).
    let mut so = child.stdout.take().expect("piped stdout");
    let mut se = child.stderr.take().expect("piped stderr");
    let ht = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = so.read_to_string(&mut s);
        s
    });
    let et = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = se.read_to_string(&mut s);
        s
    });

    let deadline = Instant::now() + timeout;
    let (status, timed_out) = loop {
        match child.try_wait().map_err(|e| format!("wait failed: {e}"))? {
            Some(st) => break (Some(st), false),
            None => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    break (None, true);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    };

    // Killing the child closed the pipes, so the reader threads finish; join to
    // avoid detaching them.
    let stdout = ht.join().unwrap_or_default();
    let stderr = et.join().unwrap_or_default();
    if timed_out {
        return Err(format!("timed out after {}ms", timeout.as_millis()));
    }
    Ok(RunOutput { exit: status.and_then(|s| s.code()).unwrap_or(-1), stdout, stderr })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_list_is_read_only_only() {
        for v in ["status", "ls", "diff"] {
            assert!(verb_allowed(v), "{v} should be allowed");
        }
        for v in ["commit", "push", "pull", "init", "forall", "capture", "snapshot", "add", "tag"] {
            assert!(!verb_allowed(v), "{v} must be refused in stage-1");
        }
    }

    #[test]
    fn denied_args_catch_scope_and_force_redirects() {
        assert_eq!(first_denied_arg(&["--porcelain".into()]), None);
        assert_eq!(first_denied_arg(&["--root".into(), "/etc".into()]).as_deref(), Some("--root"));
        assert_eq!(first_denied_arg(&["--root=/etc".into()]).as_deref(), Some("--root=/etc"));
        assert_eq!(first_denied_arg(&["--force".into()]).as_deref(), Some("--force"));
        assert_eq!(first_denied_arg(&["--all".into()]).as_deref(), Some("--all"));
    }

    #[test]
    fn argv_puts_root_first() {
        let v = argv(Path::new("/ws"), "status", &["--porcelain".into()]);
        assert_eq!(v, vec!["--root", "/ws", "status", "--porcelain"]);
    }

    /// This test process's own environment, which gwz inherited before Step 3.2.
    fn live() -> Environment {
        Environment::from_vars(std::env::vars_os())
    }

    #[test]
    fn run_blocking_times_out_on_a_slow_shim() {
        // A shim that ignores its args and sleeps past the timeout (no real gwz
        // needed — this exercises the timeout machinery deterministically).
        let dir = std::env::temp_dir().join(format!("glade-gwz-shim-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let shim = dir.join("slow-gwz");
        std::fs::write(&shim, "#!/bin/sh\nsleep 5\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let err = run_blocking(&shim, &live(), Path::new("/tmp"), "status", &[], Duration::from_millis(150)).unwrap_err();
        assert!(err.contains("timed out"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_blocking_reports_a_spawn_failure_as_err() {
        let err = run_blocking(Path::new("/no/such/gwz-binary"), &live(), Path::new("/tmp"), "status", &[], Duration::from_secs(1)).unwrap_err();
        assert!(err.contains("failed to spawn"), "{err}");
    }
}
