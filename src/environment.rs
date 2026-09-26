//! The environment gwz runs with (ProcessGlobalsPlan Step 3.2).
//!
//! `glade-gwz`'s `main` captures its process environment once, at start, and
//! passes it down in [`GwzConfig`](crate::GwzConfig). Every gwz run starts from
//! an empty environment plus that snapshot ([`crate::exec::command`]), so gwz
//! gets what the supplier started with, and a variable set in the process later
//! never reaches it.

use std::ffi::{OsStr, OsString};
use std::fmt;

/// A captured environment: each variable's name and value, in the order given.
///
/// It can hold secrets, such as tokens and API keys, so it never prints a
/// value: its `Debug` lists the names alone.
#[derive(Clone)]
pub struct Environment {
    vars: Vec<(OsString, OsString)>,
}

impl Environment {
    /// Exactly `vars`: `main` passes `std::env::vars_os()`, a test a made-up
    /// list.
    pub fn from_vars<K, V>(vars: impl IntoIterator<Item = (K, V)>) -> Environment
    where
        K: Into<OsString>,
        V: Into<OsString>,
    {
        let vars = vars.into_iter().map(|(k, v)| (k.into(), v.into()));
        Environment {
            vars: vars.collect(),
        }
    }

    /// Each variable's name and value, for `Command::envs`.
    pub fn vars(&self) -> impl Iterator<Item = (&OsStr, &OsStr)> {
        self.vars
            .iter()
            .map(|(k, v)| (k.as_os_str(), v.as_os_str()))
    }
}

/// The names alone: a value can be a secret.
impl fmt::Debug for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&OsStr> = self.vars.iter().map(|(k, _)| k.as_os_str()).collect();
        f.debug_struct("Environment")
            .field("names", &names)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GwzConfig;
    use std::path::PathBuf;

    /// The snapshot, and the config that carries it, name a variable but never
    /// print its value. The value is made up.
    #[test]
    fn debug_names_the_variables_and_prints_no_value() {
        let env = Environment::from_vars([("GLADE_GWZ_TEST_TOKEN", "made-up-secret-value")]);
        let config = GwzConfig::new("ws://127.0.0.1:1", PathBuf::from("/ws"), env.clone());
        for shown in [
            format!("{env:?}"),
            format!("{env:#?}"),
            format!("{config:?}"),
        ] {
            assert!(
                !shown.contains("made-up-secret-value"),
                "Debug printed a value: {shown}"
            );
            assert!(
                shown.contains("GLADE_GWZ_TEST_TOKEN"),
                "Debug names the variable: {shown}"
            );
        }
    }
}
