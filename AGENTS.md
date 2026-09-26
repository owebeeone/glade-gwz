# AGENTS.md - glade-gwz rules

This repository is a member of the glade-wz workspace, whose `AGENTS.md` holds the
general rules (TDD first, the workflow). These rules are this repository's own.

## No process globals

Production code MUST keep no process-global mutable state:

- no `static mut`, and no statics with interior mutability or lazy initialisation
  (`Atomic*`, `Mutex`, `OnceLock`, `LazyLock`, …);
- no `thread_local!`;
- no environment, working-directory or home-directory reads where they are used;
- no process-wide hooks (panic hook, global logger, C signal handlers);
- no child process that inherits the live environment.

A program reads its arguments and environment once, at its entry point, and passes
them down. It spawns a child with `env_clear()` plus an explicit environment.

`scripts/checks/check_process_globals.py` enforces this against
`scripts/checks/process_globals_allowlist.json`. `tests/process_globals.rs` runs it with
`cargo test`. It fails on anything new and on any entry that no longer matches, so the
allowlist only shrinks.

- Agents MUST NOT add or loosen an allowlist entry to make the check pass. Restructure
  the code instead.
- A read at a program's entry point is recorded as `permanent`; the owner ruled that
  kind of entry on 2026-09-26. Any other new entry needs the owner's approval, with a
  disposition (`debt` or `permanent`) and a reason.
- Paying a debt deletes its entry in the same change.
- The checker is gwz-core's, vendored byte for byte, and the allowlist's `source` names
  the commit. Update every repository's copy together.

The plan that pays the debt down is `dev-docs/ProcessGlobalsPlan.md` in the glade-wz
workspace.
