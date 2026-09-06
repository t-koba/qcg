use camino::Utf8PathBuf;

use super::args::LogFormat;

/// Read-only generator catalog bundled next to the binary
/// (`<prefix>/share/qcg/generators`). Searched after the user's root so
/// installed packages shadow bundled demos with the same id.
pub(crate) fn bundled_generators_root() -> Option<Utf8PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let bin_dir = exe.parent()?;
    let prefix = bin_dir.parent()?;
    let root = Utf8PathBuf::from_path_buf(prefix.join("share/qcg/generators")).ok()?;
    root.is_dir().then_some(root)
}

pub(crate) fn init_tracing(verbose: bool, format: LogFormat) {
    let default_filter = if verbose { "qcg=debug" } else { "qcg=info" };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter));
    match format {
        LogFormat::Text => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .compact()
            .init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init(),
    }
}
