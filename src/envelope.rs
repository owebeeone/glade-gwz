//! The stage-1 request / response envelopes (GLP-0006 P1.S2). Small JSON, the
//! honest smallest surface. Two directions:
//!
//! * [`GwzRequest`] rides the `ExchangeReq` payload — `{verb, args, cwd?, stream?,
//!   principal?}`.
//! * [`GwzResponse`] rides the `ExchangeRes` payload — `{ok, exit, stdout, stderr,
//!   error?, run_id?, done?, attributed_to?}`.
//! * [`GwzOutputRecord`] rides each LOG op appended to the output surface for a
//!   streaming run — `{run_id, seq, principal?, stream, line?, done?, exit?}`.
//!
//! Failure is DATA (`GladeSupplierModel.md` §6): a disallowed verb, a bad
//! envelope, a spawn error, a timeout, or a non-zero exit all resolve to a
//! well-formed `GwzResponse{ok:false}` — the WIRE `ExchangeRes.ok` stays `true`
//! (the exchange always produced a structured answer; the PAYLOAD `ok` carries
//! command success = exit 0).

use serde::{Deserialize, Serialize};

/// A command request over the gwz exchange surface.
///
/// `verb` + `args` become the gwz argv; the supplier always prepends
/// `--root <config root>` (authoritative — the root is app-owned, never derived
/// from the request; §5). `cwd` is parsed for forward-compat + attribution but
/// is IGNORED for execution in stage-1 (request-supplied paths are an AZ-1 /
/// stage-2 concern). `stream` opts the run onto the log surface. `principal`
/// attributes the run (falls back to the supplier's configured principal).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GwzRequest {
    pub verb: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub principal: Option<String>,
}

impl GwzRequest {
    /// Parse an envelope from the exchange payload bytes.
    pub fn parse(payload: &[u8]) -> Result<GwzRequest, String> {
        serde_json::from_slice(payload).map_err(|e| format!("bad envelope: {e}"))
    }
}

/// A command answer, carried on the `ExchangeRes` payload.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct GwzResponse {
    /// The command succeeded (exit 0), OR a streaming run was accepted.
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub exit: Option<i32>,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub error: Option<String>,
    /// Streaming: the run id keying the output log surface.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub run_id: Option<String>,
    /// Streaming: `false` on the accept answer; the `done:true` marker lands on
    /// the log surface, not here.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub done: Option<bool>,
    /// The principal the run was attributed to (attribution as data — §4).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub attributed_to: Option<String>,
}

impl GwzResponse {
    /// A completed synchronous run: `ok` reflects a clean exit.
    pub fn ran(exit: i32, stdout: String, stderr: String, who: Option<String>) -> GwzResponse {
        GwzResponse { ok: exit == 0, exit: Some(exit), stdout, stderr, attributed_to: who, ..Default::default() }
    }
    /// Failure as data: a bad envelope / disallowed verb / timeout / spawn error.
    pub fn failed(error: impl Into<String>) -> GwzResponse {
        GwzResponse { ok: false, error: Some(error.into()), ..Default::default() }
    }
    /// A streaming run was accepted; output flows to the log surface under `run_id`.
    pub fn accepted(run_id: String, who: Option<String>) -> GwzResponse {
        GwzResponse { ok: true, run_id: Some(run_id), done: Some(false), attributed_to: who, ..Default::default() }
    }
    /// Serialize for the exchange payload (never panics — a serialize failure of
    /// these plain structs is not reachable, but stays failure-as-data).
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_else(|_| b"{\"ok\":false,\"error\":\"serialize failed\"}".to_vec())
    }
}

/// One appended record on the output log surface for a streaming run. Line
/// records carry `line` + `stream`; the terminal record carries `done:true` +
/// `exit`. `principal` stamps the run record (attribution as data — §4).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct GwzOutputRecord {
    pub run_id: String,
    pub seq: u64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub principal: Option<String>,
    /// `"stdout" | "stderr" | "end"`.
    pub stream: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub line: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub done: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub exit: Option<i32>,
}

impl GwzOutputRecord {
    pub fn line(run_id: &str, seq: u64, who: &Option<String>, stream: &str, line: String) -> GwzOutputRecord {
        GwzOutputRecord {
            run_id: run_id.into(),
            seq,
            principal: who.clone(),
            stream: stream.into(),
            line: Some(line),
            ..Default::default()
        }
    }
    pub fn end(run_id: &str, seq: u64, who: &Option<String>, exit: i32) -> GwzOutputRecord {
        GwzOutputRecord {
            run_id: run_id.into(),
            seq,
            principal: who.clone(),
            stream: "end".into(),
            done: Some(true),
            exit: Some(exit),
            ..Default::default()
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_parses_minimal_and_full() {
        let m = GwzRequest::parse(br#"{"verb":"status"}"#).unwrap();
        assert_eq!(m.verb, "status");
        assert!(m.args.is_empty() && m.cwd.is_none() && !m.stream && m.principal.is_none());
        let f = GwzRequest::parse(br#"{"verb":"diff","args":["--stat"],"cwd":"sub","stream":true,"principal":"gianni"}"#).unwrap();
        assert_eq!(f.verb, "diff");
        assert_eq!(f.args, vec!["--stat"]);
        assert_eq!(f.cwd.as_deref(), Some("sub"));
        assert!(f.stream);
        assert_eq!(f.principal.as_deref(), Some("gianni"));
    }

    #[test]
    fn bad_envelope_is_an_error_not_a_panic() {
        let e = GwzRequest::parse(b"not json").unwrap_err();
        assert!(e.contains("bad envelope"), "{e}");
    }

    #[test]
    fn response_ok_reflects_exit_and_skips_none_fields() {
        let ok = GwzResponse::ran(0, "hi".into(), String::new(), Some("gianni".into()));
        assert!(ok.ok && ok.exit == Some(0));
        let bad = GwzResponse::ran(1, String::new(), "boom".into(), None);
        assert!(!bad.ok && bad.exit == Some(1));
        // None-valued optionals are omitted from the wire JSON.
        let s = String::from_utf8(ok.to_bytes()).unwrap();
        assert!(!s.contains("run_id") && !s.contains("\"error\""), "{s}");
        assert!(s.contains("\"attributed_to\":\"gianni\""), "{s}");
    }

    #[test]
    fn accepted_carries_run_id_and_done_false() {
        let a = GwzResponse::accepted("run-1".into(), Some("p".into()));
        assert!(a.ok && a.run_id.as_deref() == Some("run-1") && a.done == Some(false));
    }
}
