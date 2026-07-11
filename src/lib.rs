//! a glade supplier: gwz workspace commands over an EXCHANGE surface, with
//! long-op output streamed onto a LOG surface (GLP-0006 P1.S2).
//!
//! `glade-gwz` is the exchange-supplier reference implementation
//! (`SupplierRequirements.md` §glade-gwz). It attaches over the wire as an
//! ordinary authority session via [`glade_client`] (no node internals — P00-a),
//! stands behind the declared `(ws-razel, gwz.ops)` exchange surface, and runs
//! ALLOW-LISTED read-only `gwz` verbs against a CONFIGURED workspace root — the
//! first app-owned-storage consumer (`GladeSupplierModel.md` §5): the root is the
//! app's, never derived from a request. Failure is uniformly data.
//!
//! Modules:
//! * [`envelope`] — the request / response / output-record JSON shapes.
//! * [`exec`] — the verb allow-list, arg guards, and the blocking runner.
//! * [`supplier`] — [`serve`], [`GwzConfig`], [`GwzSupplier`]: attach + serve.

pub mod envelope;
pub mod exec;
pub mod supplier;

pub use envelope::{GwzOutputRecord, GwzRequest, GwzResponse};
pub use exec::ALLOWED_VERBS;
pub use supplier::{
    serve, GwzConfig, GwzSupplier, DEFAULT_GLADE_ID, DEFAULT_OUTPUT_ID, DEFAULT_SHARE,
    DEFAULT_TIMEOUT_SECS,
};
