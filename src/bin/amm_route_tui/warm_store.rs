use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy_primitives::B256;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

const MANIFEST_VERSION: u32 = 1;
const MANIFEST_FILE: &str = "amm_manifest.json";
const GENERATIONS_DIR: &str = "generations";
const REGISTRATION_ARCHIVE: &str = "amm_registrations.bin";
const STATE_FILE: &str = "evm_state.bin";
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Hash-certified canonical point represented by one committed warm generation.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct WarmCheckpoint {
    pub(crate) chain_id: u64,
    pub(crate) block_number: u64,
    pub(crate) block_hash: B256,
}

#[derive(Debug, Deserialize, Serialize)]
struct WarmManifest {
    version: u32,
    chain_id: u64,
    generation: String,
    checkpoint: WarmCheckpoint,
}

/// Chain-scoped crash-consistent warm-generation store.
pub(crate) struct WarmStore {
    root: PathBuf,
    chain_id: u64,
}

impl WarmStore {
    pub(crate) fn new(root: impl Into<PathBuf>, chain_id: u64) -> Self {
        Self {
            root: root.into(),
            chain_id,
        }
    }

    /// Start a mutable generation seeded from the last committed manifest.
    /// The committed directory is never mutated in place.
    pub(crate) fn begin(&self, persist_cache: bool) -> Result<WarmSession> {
        let namespace = self.namespace();
        fs::create_dir_all(&namespace)
            .with_context(|| format!("create warm namespace {}", namespace.display()))?;
        if !persist_cache {
            return Ok(WarmSession {
                namespace: namespace.clone(),
                generations: namespace.join(GENERATIONS_DIR),
                chain_id: self.chain_id,
                cache_base: self.root.clone(),
                registration_archive: namespace.join(REGISTRATION_ARCHIVE),
                resume_checkpoint: None,
                staging: None,
            });
        }

        let generations = namespace.join(GENERATIONS_DIR);
        fs::create_dir_all(&generations)
            .with_context(|| format!("create warm generations {}", generations.display()))?;
        let committed = self.load_committed(&namespace, &generations);
        let staging = unique_staging_path(&generations);
        fs::create_dir(&staging)
            .with_context(|| format!("create warm staging generation {}", staging.display()))?;

        let resume_checkpoint = if let Some((source, checkpoint)) = &committed {
            match copy_tree(source, &staging) {
                Ok(()) => Some(*checkpoint),
                Err(_) => {
                    fs::remove_dir_all(&staging).with_context(|| {
                        format!("discard unreadable warm generation {}", staging.display())
                    })?;
                    fs::create_dir(&staging).with_context(|| {
                        format!("recreate warm staging generation {}", staging.display())
                    })?;
                    copy_legacy_registrations(&namespace, &staging.join(self.chain_dir_name()))?;
                    None
                }
            }
        } else {
            copy_legacy_registrations(&namespace, &staging.join(self.chain_dir_name()))?;
            None
        };

        let chain_dir = staging.join(self.chain_dir_name());
        Ok(WarmSession {
            namespace,
            generations,
            chain_id: self.chain_id,
            cache_base: staging.clone(),
            registration_archive: chain_dir.join(REGISTRATION_ARCHIVE),
            resume_checkpoint,
            staging: Some(staging),
        })
    }

    fn namespace(&self) -> PathBuf {
        self.root.join(self.chain_dir_name())
    }

    fn chain_dir_name(&self) -> String {
        format!("chain_{}", self.chain_id)
    }

    fn load_committed(
        &self,
        namespace: &Path,
        generations: &Path,
    ) -> Option<(PathBuf, WarmCheckpoint)> {
        let bytes = fs::read(namespace.join(MANIFEST_FILE)).ok()?;
        let manifest: WarmManifest = serde_json::from_slice(&bytes).ok()?;
        if manifest.version != MANIFEST_VERSION
            || manifest.chain_id != self.chain_id
            || manifest.checkpoint.chain_id != self.chain_id
            || !safe_generation_name(&manifest.generation)
        {
            return None;
        }
        let generation = generations.join(&manifest.generation);
        let chain_dir = generation.join(self.chain_dir_name());
        if !chain_dir.join(STATE_FILE).is_file() || !chain_dir.join(REGISTRATION_ARCHIVE).is_file()
        {
            return None;
        }
        Some((generation, manifest.checkpoint))
    }
}

/// One private mutable cache generation. Dropping it before `commit` cannot
/// alter the manifest-selected generation.
pub(crate) struct WarmSession {
    namespace: PathBuf,
    generations: PathBuf,
    chain_id: u64,
    cache_base: PathBuf,
    registration_archive: PathBuf,
    resume_checkpoint: Option<WarmCheckpoint>,
    staging: Option<PathBuf>,
}

impl WarmSession {
    pub(crate) fn persist_cache(&self) -> bool {
        self.staging.is_some()
    }

    pub(crate) fn cache_base_dir(&self) -> &Path {
        &self.cache_base
    }

    pub(crate) fn chain_dir(&self) -> PathBuf {
        self.cache_base.join(format!("chain_{}", self.chain_id))
    }

    pub(crate) fn state_path(&self) -> PathBuf {
        self.chain_dir().join(STATE_FILE)
    }

    pub(crate) fn registration_archive(&self) -> &Path {
        &self.registration_archive
    }

    pub(crate) const fn resume_checkpoint(&self) -> Option<WarmCheckpoint> {
        self.resume_checkpoint
    }

    /// Stop trusting state copied from a generation whose checkpoint failed
    /// canonical verification. Registration metadata remains available only
    /// as cold-start hints and is rehydrated at the new verified baseline.
    pub(crate) fn discard_unverified_cache(&mut self) -> Result<()> {
        let chain_dir = self.chain_dir();
        if chain_dir.is_dir() {
            for entry in fs::read_dir(&chain_dir)
                .with_context(|| format!("read unverified warm cache {}", chain_dir.display()))?
            {
                let entry = entry?;
                if entry.file_name() == REGISTRATION_ARCHIVE {
                    continue;
                }
                let path = entry.path();
                let file_type = entry.file_type()?;
                if file_type.is_dir() {
                    fs::remove_dir_all(&path)
                } else {
                    fs::remove_file(&path)
                }
                .with_context(|| format!("discard unverified warm cache {}", path.display()))?;
            }
        }
        self.resume_checkpoint = None;
        Ok(())
    }

    /// Atomically select this fully synced generation by replacing the small
    /// manifest only after the immutable generation directory is durable.
    pub(crate) fn commit(&mut self, checkpoint: WarmCheckpoint) -> Result<()> {
        if checkpoint.chain_id != self.chain_id {
            bail!(
                "warm checkpoint chain mismatch: expected {}, got {}",
                self.chain_id,
                checkpoint.chain_id
            );
        }
        let staging = self
            .staging
            .as_ref()
            .context("cannot commit a cache-disabled warm session")?;
        if !self.state_path().is_file() {
            bail!("warm generation has no persisted EVM state");
        }
        if !self.registration_archive.is_file() {
            bail!("warm generation has no registration archive");
        }

        sync_tree(staging)?;
        let unique = staging
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("staging")
            .trim_start_matches(".staging-");
        let hash = format!("{:x}", checkpoint.block_hash);
        let generation_name = format!(
            "gen-{}-{}-{}",
            checkpoint.block_number,
            &hash[..8.min(hash.len())],
            unique
        );
        let generation = self.generations.join(&generation_name);
        fs::rename(staging, &generation).with_context(|| {
            format!(
                "seal warm generation {} as {}",
                staging.display(),
                generation.display()
            )
        })?;
        sync_dir(&self.generations)?;

        let manifest = WarmManifest {
            version: MANIFEST_VERSION,
            chain_id: self.chain_id,
            generation: generation_name.clone(),
            checkpoint,
        };
        let bytes = serde_json::to_vec(&manifest).context("encode warm manifest")?;
        atomic_write_synced(&self.namespace.join(MANIFEST_FILE), &bytes)?;
        self.staging = None;
        self.cache_base = generation.clone();
        self.registration_archive = self.chain_dir().join(REGISTRATION_ARCHIVE);
        self.resume_checkpoint = Some(checkpoint);
        prune_generations(&self.generations, &generation_name);
        Ok(())
    }
}

impl Drop for WarmSession {
    fn drop(&mut self) {
        if let Some(staging) = self.staging.take() {
            let _ = fs::remove_dir_all(staging);
        }
    }
}

fn unique_staging_path(generations: &Path) -> PathBuf {
    loop {
        let sequence = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let candidate = generations.join(format!(
            ".staging-{}-{nanos}-{sequence}",
            std::process::id()
        ));
        if !candidate.exists() {
            return candidate;
        }
    }
}

fn safe_generation_name(name: &str) -> bool {
    name.starts_with("gen-")
        && Path::new(name)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        && Path::new(name).components().count() == 1
}

fn copy_legacy_registrations(legacy_chain: &Path, target_chain: &Path) -> Result<()> {
    let source = legacy_chain.join(REGISTRATION_ARCHIVE);
    if source.is_file() {
        fs::create_dir_all(target_chain)
            .with_context(|| format!("create generation chain dir {}", target_chain.display()))?;
        fs::copy(&source, target_chain.join(REGISTRATION_ARCHIVE))
            .with_context(|| format!("copy legacy registration archive {}", source.display()))?;
    }
    Ok(())
}

fn copy_tree(source: &Path, target: &Path) -> Result<()> {
    for entry in fs::read_dir(source)
        .with_context(|| format!("read committed generation {}", source.display()))?
    {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let destination = target.join(entry.file_name());
        if file_type.is_dir() {
            fs::create_dir_all(&destination)?;
            copy_tree(&entry.path(), &destination)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), &destination)?;
        } else {
            bail!("warm generations may not contain symlinks or special files");
        }
    }
    Ok(())
}

fn sync_tree(path: &Path) -> Result<()> {
    let mut directories = Vec::new();
    sync_tree_inner(path, &mut directories)?;
    for directory in directories.into_iter().rev() {
        sync_dir(&directory)?;
    }
    Ok(())
}

fn sync_tree_inner(path: &Path, directories: &mut Vec<PathBuf>) -> Result<()> {
    directories.push(path.to_path_buf());
    for entry in fs::read_dir(path).with_context(|| format!("sync tree {}", path.display()))? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            sync_tree_inner(&entry.path(), directories)?;
        } else if file_type.is_file() {
            File::open(entry.path())?.sync_all()?;
        } else {
            bail!("warm generations may not contain symlinks or special files");
        }
    }
    Ok(())
}

fn atomic_write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("warm manifest has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.tmp-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("manifest"),
        NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, path)?;
    sync_dir(parent)
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

fn prune_generations(generations: &Path, active: &str) {
    let Ok(entries) = fs::read_dir(generations) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_name() == active {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            let _ = fs::remove_dir_all(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use alloy_primitives::B256;

    use super::{WarmCheckpoint, WarmStore};

    fn checkpoint(block_number: u64, byte: u8) -> WarmCheckpoint {
        WarmCheckpoint {
            chain_id: 1,
            block_number,
            block_hash: B256::repeat_byte(byte),
        }
    }

    #[test]
    fn uncommitted_generation_never_replaces_committed_checkpoint() {
        let root = std::env::temp_dir().join(format!(
            "amm_route_tui_warm_store_crash_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);

        let store = WarmStore::new(&root, 1);
        let mut first = store.begin(true).expect("begin first generation");
        fs::create_dir_all(first.chain_dir()).expect("create first chain dir");
        fs::write(first.state_path(), b"state-a").expect("write first state");
        fs::write(first.registration_archive(), b"archive-a").expect("write first archive");
        first
            .commit(checkpoint(100, 0x64))
            .expect("commit first generation");

        let second = store.begin(true).expect("begin interrupted generation");
        assert_eq!(second.resume_checkpoint(), Some(checkpoint(100, 0x64)));
        fs::write(second.state_path(), b"state-b").expect("write interrupted state");
        fs::write(second.registration_archive(), b"archive-b").expect("write interrupted archive");
        std::mem::forget(second); // Models process death before the manifest commit.

        let recovered = store.begin(true).expect("recover committed generation");
        assert_eq!(recovered.resume_checkpoint(), Some(checkpoint(100, 0x64)));
        assert_eq!(
            fs::read(recovered.registration_archive()).expect("read recovered archive"),
            b"archive-a"
        );
        assert_eq!(
            fs::read(recovered.state_path()).expect("read recovered state"),
            b"state-a"
        );

        drop(recovered);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn incomplete_generation_cannot_replace_the_committed_manifest() {
        let root = std::env::temp_dir().join(format!(
            "amm_route_tui_warm_store_incomplete_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);

        let store = WarmStore::new(&root, 1);
        let mut first = store.begin(true).expect("begin first generation");
        fs::create_dir_all(first.chain_dir()).expect("create first chain dir");
        fs::write(first.state_path(), b"state-a").expect("write first state");
        fs::write(first.registration_archive(), b"archive-a").expect("write first archive");
        first
            .commit(checkpoint(100, 0x64))
            .expect("commit first generation");

        let mut incomplete = store.begin(true).expect("begin incomplete generation");
        fs::remove_file(incomplete.state_path()).expect("remove required state");
        fs::write(incomplete.registration_archive(), b"archive-b")
            .expect("write replacement archive");
        assert!(incomplete.commit(checkpoint(101, 0x65)).is_err());
        drop(incomplete);

        let recovered = store.begin(true).expect("recover first generation");
        assert_eq!(recovered.resume_checkpoint(), Some(checkpoint(100, 0x64)));
        assert_eq!(
            fs::read(recovered.registration_archive()).expect("read recovered archive"),
            b"archive-a"
        );

        drop(recovered);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejected_checkpoint_discards_state_but_keeps_registration_hints() {
        let root = std::env::temp_dir().join(format!(
            "amm_route_tui_warm_store_rejected_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);

        let store = WarmStore::new(&root, 1);
        let mut committed = store.begin(true).expect("begin committed generation");
        fs::create_dir_all(committed.chain_dir()).expect("create chain dir");
        fs::write(committed.state_path(), b"unverified-state").expect("write state");
        fs::write(committed.registration_archive(), b"registration-hints").expect("write archive");
        committed
            .commit(checkpoint(100, 0x64))
            .expect("commit generation");

        let mut rejected = store.begin(true).expect("begin rejected generation");
        rejected
            .discard_unverified_cache()
            .expect("discard unverified cache");
        assert_eq!(rejected.resume_checkpoint(), None);
        assert!(!rejected.state_path().exists());
        assert_eq!(
            fs::read(rejected.registration_archive()).expect("read retained archive"),
            b"registration-hints"
        );

        drop(rejected);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cache_disabled_session_does_not_replace_committed_manifest() {
        let root = std::env::temp_dir().join(format!(
            "amm_route_tui_warm_store_disabled_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);

        let store = WarmStore::new(&root, 1);
        let mut committed = store.begin(true).expect("begin committed generation");
        fs::create_dir_all(committed.chain_dir()).expect("create chain dir");
        fs::write(committed.state_path(), b"state-a").expect("write state");
        fs::write(committed.registration_archive(), b"archive-a").expect("write archive");
        committed
            .commit(checkpoint(100, 0x64))
            .expect("commit generation");

        let disabled = store.begin(false).expect("begin cache-disabled session");
        assert!(!disabled.persist_cache());
        fs::write(disabled.registration_archive(), b"legacy-archive")
            .expect("write legacy archive");
        drop(disabled);

        let recovered = store.begin(true).expect("recover committed generation");
        assert_eq!(recovered.resume_checkpoint(), Some(checkpoint(100, 0x64)));
        assert_eq!(
            fs::read(recovered.registration_archive()).expect("read committed archive"),
            b"archive-a"
        );

        drop(recovered);
        let _ = fs::remove_dir_all(root);
    }
}
