a glade supplier: gwz workspace commands over an exchange surface, with long-op
output on a log surface.

`glade-gwz` (GLP-0006 P1.S2) is the exchange-supplier reference implementation.
It attaches to a glade node as an ordinary authority session (over the wire, via
`glade-client` — no node internals, P00-a), stands behind the declared
`(ws-razel, gwz.ops)` exchange surface, and runs **allow-listed read-only** `gwz`
verbs against a **configured** workspace root. It is the first app-owned-storage
consumer (`GladeSupplierModel.md` §5): the root is the app's, never derived from
a request.

## Run

```sh
glade-gwz --node ws://127.0.0.1:9099 --root /path/to/workspace \
  [--share ws-razel] [--glade-id gwz.ops] [--output-id gwz.output] \
  [--principal gianni] [--gwz-bin gwz] [--timeout-secs 30]
```

Attaches, serves, reattaches on link drop, and shuts down cleanly on
SIGTERM/SIGINT.

gwz runs with the environment `glade-gwz` started with. It is captured once, at
start, and each run starts from an empty environment plus that snapshot, so a
variable set in the process later never reaches gwz.

## Command surface (exchange `gwz.ops`)

Request payload — a small JSON envelope:

```json
{ "verb": "status", "args": ["--porcelain"], "cwd": null, "stream": false, "principal": "gianni" }
```

- `verb` (required) + `args` become the gwz argv. The supplier always prepends
  `--root <config root>` and it stays authoritative.
- `cwd` is parsed for forward-compat + attribution but **ignored for execution in
  stage-1** — request-supplied paths are an AZ-1 / stage-2 concern; the root
  stays uniquely the app's.
- `stream:true` routes output to the log surface (below).
- `principal` attributes the run (falls back to `--principal`).

Response payload:

```json
{ "ok": true, "exit": 0, "stdout": "…", "stderr": "…", "attributed_to": "gianni" }
```

`ok` = the command succeeded (`exit == 0`). A disallowed verb, a bad envelope, a
spawn error, or a timeout is **failure as data** (`{ "ok": false, "error": "…" }`).
The **wire** `ExchangeRes.ok` is always `true` — the exchange always produced a
structured answer; the payload `ok` carries command success.

### Stage-1 allow-list

`status`, `ls`, `diff` — the pure read verbs (no member mutation, no lock write,
no arbitrary exec). Everything else is refused as data. Excluded and why: `forall`
(arbitrary command exec), `capture` / `snapshot` (write the lock), `add` /
`commit` / `pull` / `push` / `clone` / `init` / `materialize` / `branch` / `tag` /
`repo` / `stash` (mutate). Stage-2 replaces the list with per-verb GRANTS (the
s-verbs taxonomy; `gwz.*` seeds already ride `grazel-app.glade`).

## Long-op output (log `gwz.output`)

`stream:true` answers immediately with
`{ "run_id": "run-mufa2wc2-1", "done": false }`. The run's stdout/stderr lines
are appended as ops to the log surface **keyed by `run_id`**:

```json
{ "run_id": "run-mufa2wc2-1", "seq": 1, "principal": "gianni", "stream": "stdout", "line": "…" }
```

closed by a terminal marker:

```json
{ "run_id": "run-mufa2wc2-1", "seq": 7, "principal": "gianni", "stream": "end", "done": true, "exit": 0 }
```

A consumer subscribes `(share, gwz.output, run_id)` and folds the log to follow
the run.

The run id is opaque: key by the whole string. Each supplier process reads a tag
off the clock when it starts serving, and mints `run-<tag>-<n>`. The node keeps
the output log across a supplier restart, so an id an earlier process spent
would put a new run on that run's chain, where the node refuses it.
