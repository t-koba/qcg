use crate::artifacts::{api_bad_request, api_internal, api_not_found};
use crate::types::LocalQcgService;
use camino::Utf8PathBuf;
use qcg_api::{ApiError, GeneratorDetail, GeneratorSummary};
use qcg_contract::{Contract, PackagePathError};
use qcg_policy::MAX_DIRECTORY_SCAN_ENTRIES;
use qcg_policy::is_safe_relative_path;
use tokio::io::AsyncReadExt as _;

impl LocalQcgService {
    pub async fn list_generators(&self) -> Result<Vec<GeneratorSummary>, ApiError> {
        let mut generators = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for root in &self.inner.generator_roots {
            if !root.exists() {
                continue;
            }
            let entries = std::fs::read_dir(root).map_err(api_internal)?;
            let mut entry_count = 0_usize;
            for entry in entries {
                entry_count = entry_count.saturating_add(1);
                if entry_count > MAX_DIRECTORY_SCAN_ENTRIES {
                    return Err(api_internal(format!(
                        "generator directory `{root}` contains more than {MAX_DIRECTORY_SCAN_ENTRIES} entries"
                    )));
                }
                let entry = entry.map_err(api_internal)?;
                let path = Utf8PathBuf::from_path_buf(entry.path()).map_err(|path| {
                    api_internal(format!(
                        "generator path is not valid UTF-8: {}",
                        path.display()
                    ))
                })?;
                if !path.join("qcg.toml").exists() {
                    continue;
                }
                let contract = Contract::load(&path).map_err(api_internal)?;
                if seen.insert(contract.manifest.generator.id.clone()) {
                    generators.push(GeneratorSummary {
                        id: contract.manifest.generator.id,
                        name: contract.manifest.generator.name,
                        version: contract.manifest.generator.version,
                        description: contract.manifest.generator.description,
                    });
                }
            }
        }
        Ok(generators)
    }

    pub async fn describe(&self, id: &str) -> Result<GeneratorDetail, ApiError> {
        let contract = self.load_generator(id)?;
        Ok(GeneratorDetail {
            generator: contract.manifest.generator,
            inputs: contract.manifest.inputs,
            assets: contract.manifest.assets,
            permissions: contract.manifest.permissions,
        })
    }

    pub async fn read_generator_asset(
        &self,
        id: String,
        path: String,
    ) -> Result<Vec<u8>, ApiError> {
        self.read_generator_asset_with_limit(id, path, None).await
    }

    pub async fn read_generator_asset_with_limit(
        &self,
        id: String,
        path: String,
        max_bytes: Option<usize>,
    ) -> Result<Vec<u8>, ApiError> {
        let contract = self.load_generator(&id)?;
        if !is_safe_relative_path(&path)
            || path.to_ascii_lowercase().contains("%2e")
            || path.to_ascii_lowercase().contains("%2f")
            || path.to_ascii_lowercase().contains("%5c")
        {
            return Err(api_bad_request(format!(
                "generator asset path `{path}` is not allowed"
            )));
        }
        let exact = contract
            .manifest
            .assets
            .files
            .iter()
            .any(|file| file == &path);
        let in_dir = contract.manifest.assets.dirs.iter().any(|dir| {
            path.strip_prefix(dir)
                .is_some_and(|suffix| suffix.starts_with('/') && suffix.len() > 1)
        });
        if !exact && !in_dir {
            return Err(api_not_found(format!(
                "generator asset `{path}` was not declared"
            )));
        }
        let requested = match contract.resolve_package_path(&path) {
            Ok(path) => path,
            Err(PackagePathError::Path { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                return Err(api_not_found(format!(
                    "generator asset `{path}` was not found"
                )));
            }
            Err(PackagePathError::Escapes { .. }) => {
                return Err(api_not_found(format!(
                    "generator asset `{path}` was not found"
                )));
            }
            Err(error) => return Err(api_internal(error)),
        };
        if !requested.is_file() {
            return Err(api_not_found(format!(
                "generator asset `{path}` was not found"
            )));
        }
        if max_bytes.is_none() {
            return tokio::fs::read(&requested).await.map_err(api_internal);
        }
        let limit = max_bytes.unwrap_or(usize::MAX);
        let file = tokio::fs::File::open(requested)
            .await
            .map_err(api_internal)?;
        let mut bytes = Vec::new();
        file.take(
            u64::try_from(limit)
                .map_err(|_| api_internal("generator asset limit does not fit in u64"))?
                .saturating_add(1),
        )
        .read_to_end(&mut bytes)
        .await
        .map_err(api_internal)?;
        if bytes.len() > limit {
            return Err(ApiError::TooLarge {
                actual_bytes: bytes.len(),
                limit_bytes: limit,
            });
        }
        Ok(bytes)
    }
}
