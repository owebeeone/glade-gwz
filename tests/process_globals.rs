//! The process-global state ratchet: `scripts/checks/check_process_globals.py`
//! (gwz-core's checker, vendored byte for byte) over this repository's
//! production crate roots, against `scripts/checks/process_globals_allowlist.json`.
//! A new static with interior mutability, thread-local slot, environment read at
//! the point of use, process-wide hook, or child process that inherits the live
//! environment fails it; so does an allowlist entry that no longer matches, so
//! the list only shrinks. The plan that pays the debt down is
//! `dev-docs/ProcessGlobalsPlan.md` in the glade-wz workspace.

use std::path::PathBuf;
use std::process::Command;

/// This repository's root.
fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn no_new_process_global_state() {
    let repo = repo();
    let checker = repo.join("scripts/checks/check_process_globals.py");
    let ran = Command::new("python3").arg(&checker).arg("--repo").arg(&repo).output();
    let out = match ran {
        Ok(out) => out,
        Err(e) => panic!("python3 (3.10 or later) must run {}: {e}", checker.display()),
    };
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
