//! Formatting in the gate: `cargo fmt --check` over this crate (its library,
//! binary and tests; not its path dependencies, which other repositories own)
//! must find nothing to change. When it fails, run `cargo fmt`; never skip it.

use std::path::PathBuf;
use std::process::Command;

/// This crate's manifest.
fn manifest() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")
}

#[test]
fn cargo_fmt_would_change_nothing() {
    let manifest = manifest();
    // The cargo running this suite, not whichever `cargo` is first on PATH.
    let cargo = env!("CARGO");
    let ran = Command::new(cargo)
        .args(["fmt", "--check", "--manifest-path"])
        .arg(&manifest)
        .output();
    let out = match ran {
        Ok(out) => out,
        Err(e) => panic!("{cargo} fmt --check must run: {e}"),
    };
    assert!(
        out.status.success(),
        "cargo fmt --check over {} failed ({}); run `cargo fmt`, never skip this check:\n{}{}",
        manifest.display(),
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
