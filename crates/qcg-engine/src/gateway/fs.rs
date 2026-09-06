use camino::{Utf8Path, Utf8PathBuf};
use qcg_contract::{CommandIsolation, Permissions};

use super::error::GatewayError;

#[derive(Debug, Clone)]
pub struct CommandPermissionSummary {
    pub bin: String,
    pub args: Vec<String>,
    pub purpose: String,
    pub isolation: Option<CommandIsolation>,
    pub image: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FsGateway {
    workspace: Utf8PathBuf,
    can_read_workspace: bool,
    can_write_workspace: bool,
}

impl FsGateway {
    pub fn new(workspace: Utf8PathBuf, permissions: &Permissions) -> Self {
        Self {
            workspace,
            can_read_workspace: permissions.fs_read.iter().any(|scope| scope == "workspace"),
            can_write_workspace: permissions
                .fs_write
                .iter()
                .any(|scope| scope == "workspace"),
        }
    }

    pub fn resolve_read(&self, path: &str) -> Result<Utf8PathBuf, GatewayError> {
        if !self.can_read_workspace {
            return Err(GatewayError::FsReadDenied);
        }
        self.resolve_workspace_path(path, false)
    }

    pub fn resolve_write(&self, path: &str) -> Result<Utf8PathBuf, GatewayError> {
        if !self.can_write_workspace {
            return Err(GatewayError::FsWriteDenied);
        }
        let joined = self.resolve_workspace_path(path, true)?;
        ensure_parent(&joined)?;
        Ok(joined)
    }

    fn resolve_workspace_path(
        &self,
        path: &str,
        allow_missing_leaf: bool,
    ) -> Result<Utf8PathBuf, GatewayError> {
        if path == "." && !allow_missing_leaf {
            let workspace = dunce::canonicalize(&self.workspace)?;
            return Utf8PathBuf::from_path_buf(workspace).map_err(|_| GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            });
        }
        let relative = Utf8Path::new(path);
        if path.contains('\0')
            || path.contains('\\')
            || relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, camino::Utf8Component::Normal(_)))
        {
            return Err(GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            });
        }
        let joined = self.workspace.join(path);
        let workspace = dunce::canonicalize(&self.workspace)?;
        let candidate = if allow_missing_leaf {
            let parent = joined.parent().unwrap_or(&self.workspace);
            std::fs::create_dir_all(parent)?;
            let parent = dunce::canonicalize(parent)?;
            parent.join(joined.file_name().unwrap_or_default())
        } else {
            dunce::canonicalize(&joined)?
        };
        if !candidate.starts_with(&workspace) {
            return Err(GatewayError::PathDenied {
                path: Utf8PathBuf::from(path),
                workspace: self.workspace.clone(),
            });
        }
        Utf8PathBuf::from_path_buf(candidate).map_err(|_| GatewayError::PathDenied {
            path: Utf8PathBuf::from(path),
            workspace: self.workspace.clone(),
        })
    }

    pub fn workspace(&self) -> &Utf8Path {
        &self.workspace
    }
}

fn ensure_parent(path: &Utf8Path) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}
