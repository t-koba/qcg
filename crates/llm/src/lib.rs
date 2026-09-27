mod search;

mod catalog;
mod config;
pub use catalog::*;
mod decision;
pub use decision::*;
mod http_provider;
mod parse;
mod payload;
mod provider;
mod router;
mod stream;
pub(crate) mod text_gate;
mod types;
mod validate;

#[cfg(test)]
pub(crate) use config::*;
pub use http_provider::*;
#[cfg(test)]
pub(crate) use parse::*;
pub use payload::*;
pub use provider::*;
pub use router::*;
pub use search::{SearchMethod, SearchProfile, SearchProviderSpec, SearchRuntime};
#[cfg(test)]
pub(crate) use stream::*;
pub use types::*;
#[cfg(test)]
pub(crate) use validate::*;

/// Serializes all process-environment mutation in tests: `set_var` is
/// process-global, so concurrent readers in other test threads must not
/// run while any test mutates it.
#[cfg(test)]
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod tests;
