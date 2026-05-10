use std::{fmt, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::{Uuid, Version};

use crate::{
    AmberConfig, Storage, StorageError,
    storage::{ObjectPath, paths},
};

const MANIFEST_VERSION: u32 = 1;
const AMBER_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(String);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn parse(value: impl AsRef<str>) -> Result<Self, SessionIdError> {
        value.as_ref().parse()
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<str> for SessionId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SessionId {
    type Err = SessionIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let uuid = Uuid::parse_str(value).map_err(|source| SessionIdError::InvalidUuid {
            value: value.to_owned(),
            source,
        })?;

        if uuid.get_version() != Some(Version::SortRand) {
            return Err(SessionIdError::WrongVersion {
                value: value.to_owned(),
            });
        }

        Ok(Self(uuid.to_string()))
    }
}

#[derive(Debug, Error)]
pub enum SessionIdError {
    #[error("invalid session ID '{value}': {source}")]
    InvalidUuid {
        value: String,
        #[source]
        source: uuid::Error,
    },
    #[error("session ID '{value}' is not a UUIDv7")]
    WrongVersion { value: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionManifest {
    pub manifest_version: u32,
    pub session_id: SessionId,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub status: SessionStatus,
    pub config_snapshot: AmberConfig,
    pub amber_version: String,
    pub observed_streams: Vec<ObservedStreamSummary>,
    pub tags: Vec<String>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Open,
    Closed,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedStreamSummary {
    pub node_id: String,
    pub output_id: String,
    pub schema_fingerprints: Vec<String>,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub row_count: Option<u64>,
    pub byte_size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedWalStreamUpdate {
    pub node_id: String,
    pub output_id: String,
    pub schema_fingerprint: String,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub row_count: Option<u64>,
    pub byte_size: Option<u64>,
}

impl ClosedWalStreamUpdate {
    pub fn new(
        node_id: impl Into<String>,
        output_id: impl Into<String>,
        schema_fingerprint: impl Into<String>,
        first_seen_at: DateTime<Utc>,
        last_seen_at: DateTime<Utc>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            output_id: output_id.into(),
            schema_fingerprint: schema_fingerprint.into(),
            first_seen_at,
            last_seen_at,
            row_count: None,
            byte_size: None,
        }
    }

    pub fn with_row_count(mut self, row_count: u64) -> Self {
        self.row_count = Some(row_count);
        self
    }

    pub fn with_byte_size(mut self, byte_size: u64) -> Self {
        self.byte_size = Some(byte_size);
        self
    }
}

impl SessionManifest {
    pub fn new(
        session_id: SessionId,
        started_at: DateTime<Utc>,
        config_snapshot: AmberConfig,
    ) -> Self {
        Self {
            manifest_version: MANIFEST_VERSION,
            session_id,
            started_at,
            ended_at: None,
            updated_at: started_at,
            status: SessionStatus::Open,
            config_snapshot,
            amber_version: AMBER_VERSION.to_owned(),
            observed_streams: Vec::new(),
            tags: Vec::new(),
            notes: None,
        }
    }

    pub fn path(&self) -> ObjectPath {
        manifest_path(&self.session_id)
    }

    pub fn observe_closed_wal_stream(
        &mut self,
        update: ClosedWalStreamUpdate,
        observed_at: DateTime<Utc>,
    ) {
        let summary = self.observed_streams.iter_mut().find(|summary| {
            summary.node_id == update.node_id && summary.output_id == update.output_id
        });

        match summary {
            Some(summary) => summary.apply(update),
            None => self
                .observed_streams
                .push(ObservedStreamSummary::from(update)),
        }

        self.observed_streams.sort_by(|left, right| {
            left.node_id
                .cmp(&right.node_id)
                .then_with(|| left.output_id.cmp(&right.output_id))
        });
        self.updated_at = observed_at;
    }

    pub fn close(&mut self, ended_at: DateTime<Utc>) {
        self.ended_at = Some(ended_at);
        self.updated_at = ended_at;
        self.status = SessionStatus::Closed;
    }

    pub async fn save(&self, storage: &Storage) -> Result<(), SessionManifestError> {
        let path = self.path();
        storage
            .put_json(&path, self)
            .await
            .map_err(|source| SessionManifestError::SaveManifest {
                session_id: self.session_id.clone(),
                path,
                source,
            })
    }

    pub async fn create(
        storage: &Storage,
        session_id: SessionId,
        started_at: DateTime<Utc>,
        config_snapshot: AmberConfig,
    ) -> Result<Self, SessionManifestError> {
        let manifest = Self::new(session_id, started_at, config_snapshot);
        manifest.save(storage).await?;
        Ok(manifest)
    }

    pub async fn load(
        storage: &Storage,
        session_id: &SessionId,
    ) -> Result<Self, SessionManifestError> {
        let path = manifest_path(session_id);
        let manifest: SessionManifest =
            storage
                .get_json(&path)
                .await
                .map_err(|source| SessionManifestError::LoadManifest {
                    session_id: session_id.clone(),
                    path: path.clone(),
                    source,
                })?;

        if &manifest.session_id != session_id {
            return Err(SessionManifestError::SessionIdMismatch {
                expected: session_id.clone(),
                actual: manifest.session_id,
                path,
            });
        }

        Ok(manifest)
    }

    pub async fn close_and_save(
        &mut self,
        storage: &Storage,
        ended_at: DateTime<Utc>,
    ) -> Result<(), SessionManifestError> {
        self.close(ended_at);
        self.save(storage).await
    }
}

impl ObservedStreamSummary {
    fn apply(&mut self, update: ClosedWalStreamUpdate) {
        if update.first_seen_at < self.first_seen_at {
            self.first_seen_at = update.first_seen_at;
        }
        if update.last_seen_at > self.last_seen_at {
            self.last_seen_at = update.last_seen_at;
        }

        if !self
            .schema_fingerprints
            .iter()
            .any(|fingerprint| fingerprint == &update.schema_fingerprint)
        {
            self.schema_fingerprints.push(update.schema_fingerprint);
            self.schema_fingerprints.sort();
        }

        self.row_count = accumulate_optional(self.row_count, update.row_count);
        self.byte_size = accumulate_optional(self.byte_size, update.byte_size);
    }
}

impl From<ClosedWalStreamUpdate> for ObservedStreamSummary {
    fn from(update: ClosedWalStreamUpdate) -> Self {
        Self {
            node_id: update.node_id,
            output_id: update.output_id,
            schema_fingerprints: vec![update.schema_fingerprint],
            first_seen_at: update.first_seen_at,
            last_seen_at: update.last_seen_at,
            row_count: update.row_count,
            byte_size: update.byte_size,
        }
    }
}

#[derive(Debug, Error)]
pub enum SessionManifestError {
    #[error("failed to save manifest for session '{session_id}' at '{path}': {source}")]
    SaveManifest {
        session_id: SessionId,
        path: ObjectPath,
        #[source]
        source: StorageError,
    },
    #[error("failed to load manifest for session '{session_id}' at '{path}': {source}")]
    LoadManifest {
        session_id: SessionId,
        path: ObjectPath,
        #[source]
        source: StorageError,
    },
    #[error("manifest at '{path}' belongs to session '{actual}', expected '{expected}'")]
    SessionIdMismatch {
        expected: SessionId,
        actual: SessionId,
        path: ObjectPath,
    },
}

fn manifest_path(session_id: &SessionId) -> ObjectPath {
    paths::session_manifest(session_id.as_str())
}

fn accumulate_optional(current: Option<u64>, next: Option<u64>) -> Option<u64> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current + next),
        (Some(current), None) => Some(current),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn new_session_ids_use_uuid_v7_and_sort_by_time() {
        let first = SessionId::new();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = SessionId::new();

        let first_uuid = Uuid::parse_str(first.as_str()).expect("first session ID should parse");
        let second_uuid = Uuid::parse_str(second.as_str()).expect("second session ID should parse");

        assert_eq!(first_uuid.get_version(), Some(Version::SortRand));
        assert_eq!(second_uuid.get_version(), Some(Version::SortRand));
        assert!(
            first < second,
            "UUIDv7 strings should sort by creation time"
        );
    }

    #[test]
    fn parse_rejects_non_v7_uuids() {
        let error = SessionId::parse("550e8400-e29b-41d4-a716-446655440000")
            .expect_err("v4 UUID should be rejected");

        assert!(matches!(error, SessionIdError::WrongVersion { .. }));
    }

    #[test]
    fn observed_stream_summary_merges_closed_wal_updates() {
        let started_at = Utc.with_ymd_and_hms(2026, 5, 10, 12, 0, 0).unwrap();
        let mut manifest = SessionManifest::new(SessionId::new(), started_at, test_config());

        manifest.observe_closed_wal_stream(
            ClosedWalStreamUpdate::new(
                "camera/front",
                "frames/raw",
                "schema-a",
                started_at,
                started_at + Duration::seconds(5),
            )
            .with_row_count(10)
            .with_byte_size(100),
            started_at + Duration::seconds(6),
        );
        manifest.observe_closed_wal_stream(
            ClosedWalStreamUpdate::new(
                "camera/front",
                "frames/raw",
                "schema-b",
                started_at - Duration::seconds(2),
                started_at + Duration::seconds(8),
            )
            .with_row_count(5)
            .with_byte_size(25),
            started_at + Duration::seconds(9),
        );

        assert_eq!(manifest.observed_streams.len(), 1);
        assert_eq!(
            manifest.observed_streams[0].schema_fingerprints,
            vec!["schema-a".to_owned(), "schema-b".to_owned()]
        );
        assert_eq!(
            manifest.observed_streams[0].first_seen_at,
            started_at - Duration::seconds(2)
        );
        assert_eq!(
            manifest.observed_streams[0].last_seen_at,
            started_at + Duration::seconds(8)
        );
        assert_eq!(manifest.observed_streams[0].row_count, Some(15));
        assert_eq!(manifest.observed_streams[0].byte_size, Some(125));
        assert_eq!(manifest.updated_at, started_at + Duration::seconds(9));
    }

    #[tokio::test]
    async fn create_load_and_close_manifest_through_storage() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let storage = Storage::new_local(temp_dir.path(), None::<&str>).expect("storage");
        let session_id = SessionId::new();
        let started_at = Utc.with_ymd_and_hms(2026, 5, 10, 12, 0, 0).unwrap();
        let ended_at = started_at + Duration::seconds(30);

        let mut manifest =
            SessionManifest::create(&storage, session_id.clone(), started_at, test_config())
                .await
                .expect("manifest should be created");

        assert!(
            storage
                .exists(&manifest.path())
                .await
                .expect("exists should work")
        );

        let loaded = SessionManifest::load(&storage, &session_id)
            .await
            .expect("manifest should load");
        assert_eq!(loaded.status, SessionStatus::Open);
        assert_eq!(loaded.ended_at, None);
        assert_eq!(loaded.manifest_version, MANIFEST_VERSION);
        assert_eq!(loaded.tags, Vec::<String>::new());
        assert_eq!(loaded.notes, None);
        assert_eq!(loaded.observed_streams, Vec::<ObservedStreamSummary>::new());

        manifest
            .close_and_save(&storage, ended_at)
            .await
            .expect("manifest should close");

        let reloaded = SessionManifest::load(&storage, &session_id)
            .await
            .expect("closed manifest should load");
        assert_eq!(reloaded.status, SessionStatus::Closed);
        assert_eq!(reloaded.ended_at, Some(ended_at));
        assert_eq!(reloaded.updated_at, ended_at);
    }

    #[tokio::test]
    async fn load_errors_include_session_context() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let storage = Storage::new_local(temp_dir.path(), None::<&str>).expect("storage");
        let session_id = SessionId::new();

        let error = SessionManifest::load(&storage, &session_id)
            .await
            .expect_err("missing manifest should fail");

        let message = error.to_string();
        assert!(message.contains(session_id.as_str()));
        assert!(message.contains("manifest.json"));
    }

    fn test_config() -> AmberConfig {
        AmberConfig {
            storage: crate::StorageConfig::default(),
            wal: crate::WalConfig::default(),
            compaction: crate::CompactionConfig::default(),
            nodes: vec![crate::NodeConfig {
                id: "camera/front".to_owned(),
                outputs: vec![crate::OutputConfig {
                    id: "frames/raw".to_owned(),
                    every_n_frames: Some(2),
                }],
            }],
        }
    }
}
