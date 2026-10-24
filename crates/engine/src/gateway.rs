mod command;
mod decider;
mod error;
mod fs;
#[cfg(unix)]
mod handle;
mod http;
mod patch_lock;
mod process;
mod staging;

pub use command::*;
pub use decider::*;
pub use error::*;
pub use fs::*;
pub use http::*;
pub use patch_lock::*;
pub fn command_permission_matches(p: &contract::CommandPermission, argv: &[String]) -> bool {
    p.bin == argv.first().cloned().unwrap_or_default()
        && p.args.len() == argv.len().saturating_sub(1)
        && p.args
            .iter()
            .zip(argv.iter().skip(1))
            .all(|(allowed, actual)| allowed == actual || allowed == "*")
}
#[cfg(test)]
pub(crate) use process::*;

#[cfg(test)]
mod tests;
