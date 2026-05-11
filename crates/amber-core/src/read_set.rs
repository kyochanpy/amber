use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::{
    CatalogError, CatalogEvent, CatalogState, FoldedWalSegment, FoldedWalSegmentState, ObjectPath,
    SessionId, SessionManifest, SessionManifestError, Storage, storage::paths,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionSourceFilter {
    pub node_id: Option<String>,
    pub output_id: Option<String>,
}

impl SessionSourceFilter {
    fn matches(&self, node_id: &str, output_id: &str) -> bool {
        self.node_id
            .as_deref()
            .is_none_or(|expected| expected == node_id)
            && self
                .output_id
                .as_deref()
                .is_none_or(|expected| expected == output_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalSource {
    pub path: ObjectPath,
    pub session_id: SessionId,
    pub node_id: String,
    pub output_id: String,
    pub schema_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetSource {
    pub path: ObjectPath,
    pub session_id_filter: SessionId,
    pub node_id: String,
    pub output_id: String,
    pub schema_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSourceGroup {
    pub node_id: String,
    pub output_id: String,
    pub schema_fingerprint: String,
    pub wal_sources: Vec<WalSource>,
    pub parquet_sources: Vec<ParquetSource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSourceSet {
    pub session_id: SessionId,
    pub session_root: ObjectPath,
    pub manifest: SessionManifest,
    pub groups: Vec<SessionSourceGroup>,
}

impl SessionSourceSet {
    pub async fn resolve(
        storage: &Storage,
        session_id: &SessionId,
        filter: SessionSourceFilter,
    ) -> Result<Self, SessionSourceError> {
        let manifest = SessionManifest::load(storage, session_id)
            .await
            .map_err(|source| SessionSourceError::LoadManifest {
                session_id: session_id.clone(),
                source: Box::new(source),
            })?;
        let events = CatalogEvent::list(storage).await.map_err(|source| {
            SessionSourceError::ListCatalogEvents {
                session_id: session_id.clone(),
                source: Box::new(source),
            }
        })?;
        let catalog = CatalogState::from_events(events.clone()).map_err(|source| {
            SessionSourceError::FoldCatalog {
                session_id: session_id.clone(),
                source: Box::new(source),
            }
        })?;

        let mut groups = BTreeMap::<SourceGroupKey, SessionSourceGroup>::new();
        for segment in catalog.wal_segments.values() {
            if segment.session_id != *session_id
                || segment.state != FoldedWalSegmentState::Pending
                || !filter.matches(&segment.node_id, &segment.output_id)
            {
                continue;
            }

            let key = SourceGroupKey::from_parts(
                &segment.node_id,
                &segment.output_id,
                &segment.schema_fingerprint,
            );
            groups
                .entry(key)
                .or_insert_with(|| SessionSourceGroup::from_segment(segment))
                .wal_sources
                .push(WalSource {
                    path: ObjectPath::from(segment.path.clone()),
                    session_id: segment.session_id.clone(),
                    node_id: segment.node_id.clone(),
                    output_id: segment.output_id.clone(),
                    schema_fingerprint: segment.schema_fingerprint.clone(),
                });
        }

        let mut seen_parquet_sources = BTreeSet::<(SourceGroupKey, String)>::new();
        for event in events {
            let CatalogEvent::CompactionCommitted(event) = event else {
                continue;
            };

            for parquet_file in event.created_parquet_files {
                if !filter.matches(&parquet_file.node_id, &parquet_file.output_id) {
                    continue;
                }

                let key = SourceGroupKey::from_parts(
                    &parquet_file.node_id,
                    &parquet_file.output_id,
                    &parquet_file.schema_fingerprint,
                );
                let has_requested_session_source =
                    event.source_wal_segments.iter().any(|segment_id| {
                        catalog.wal_segments.get(segment_id).is_some_and(|segment| {
                            segment_matches_parquet(segment, session_id, &parquet_file)
                        })
                    });
                if !has_requested_session_source
                    || !seen_parquet_sources.insert((key.clone(), parquet_file.path.clone()))
                {
                    continue;
                }

                groups
                    .entry(key)
                    .or_insert_with(|| SessionSourceGroup::from_parquet(&parquet_file))
                    .parquet_sources
                    .push(ParquetSource {
                        path: ObjectPath::from(parquet_file.path.clone()),
                        session_id_filter: session_id.clone(),
                        node_id: parquet_file.node_id.clone(),
                        output_id: parquet_file.output_id.clone(),
                        schema_fingerprint: parquet_file.schema_fingerprint.clone(),
                    });
            }
        }

        for group in groups.values_mut() {
            group
                .wal_sources
                .sort_by(|left, right| left.path.cmp(&right.path));
            group
                .parquet_sources
                .sort_by(|left, right| left.path.cmp(&right.path));
        }

        validate_schema_consistency(session_id, &groups)?;

        Ok(Self {
            session_id: session_id.clone(),
            session_root: paths::session_root(session_id.as_str()),
            manifest,
            groups: groups.into_values().collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SourceGroupKey {
    node_id: String,
    output_id: String,
    schema_fingerprint: String,
}

impl SourceGroupKey {
    fn from_parts(node_id: &str, output_id: &str, schema_fingerprint: &str) -> Self {
        Self {
            node_id: node_id.to_owned(),
            output_id: output_id.to_owned(),
            schema_fingerprint: schema_fingerprint.to_owned(),
        }
    }
}

impl SessionSourceGroup {
    fn from_segment(segment: &FoldedWalSegment) -> Self {
        Self {
            node_id: segment.node_id.clone(),
            output_id: segment.output_id.clone(),
            schema_fingerprint: segment.schema_fingerprint.clone(),
            wal_sources: Vec::new(),
            parquet_sources: Vec::new(),
        }
    }

    fn from_parquet(parquet: &crate::PublishedParquetFile) -> Self {
        Self {
            node_id: parquet.node_id.clone(),
            output_id: parquet.output_id.clone(),
            schema_fingerprint: parquet.schema_fingerprint.clone(),
            wal_sources: Vec::new(),
            parquet_sources: Vec::new(),
        }
    }
}

fn segment_matches_parquet(
    segment: &FoldedWalSegment,
    session_id: &SessionId,
    parquet_file: &crate::PublishedParquetFile,
) -> bool {
    segment.session_id == *session_id
        && segment.node_id == parquet_file.node_id
        && segment.output_id == parquet_file.output_id
        && segment.schema_fingerprint == parquet_file.schema_fingerprint
}

fn validate_schema_consistency(
    session_id: &SessionId,
    groups: &BTreeMap<SourceGroupKey, SessionSourceGroup>,
) -> Result<(), SessionSourceError> {
    let mut schemas_by_stream = BTreeMap::<(String, String), BTreeSet<String>>::new();
    for group in groups.values() {
        schemas_by_stream
            .entry((group.node_id.clone(), group.output_id.clone()))
            .or_default()
            .insert(group.schema_fingerprint.clone());
    }

    for ((node_id, output_id), schema_fingerprints) in schemas_by_stream {
        if schema_fingerprints.len() > 1 {
            return Err(SessionSourceError::SchemaConflict {
                session_id: session_id.clone(),
                node_id,
                output_id,
                schema_fingerprints: schema_fingerprints.into_iter().collect(),
            });
        }
    }

    Ok(())
}

#[derive(Debug, Error)]
pub enum SessionSourceError {
    #[error(
        "failed to load manifest for session '{session_id}' while resolving inspect sources: {source}"
    )]
    LoadManifest {
        session_id: SessionId,
        #[source]
        source: Box<SessionManifestError>,
    },
    #[error(
        "failed to list catalog events while resolving inspect sources for session '{session_id}': {source}"
    )]
    ListCatalogEvents {
        session_id: SessionId,
        #[source]
        source: Box<CatalogError>,
    },
    #[error(
        "failed to fold catalog state while resolving inspect sources for session '{session_id}': {source}"
    )]
    FoldCatalog {
        session_id: SessionId,
        #[source]
        source: Box<CatalogError>,
    },
    #[error(
        "session '{session_id}' has multiple schema fingerprints for node '{node_id}' and output '{output_id}': {schema_fingerprints:?}"
    )]
    SchemaConflict {
        session_id: SessionId,
        node_id: String,
        output_id: String,
        schema_fingerprints: Vec<String>,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use chrono::Utc;
    use tempfile::TempDir;

    use crate::{
        AmberConfig, CatalogEvent, RecordBatchMetadata, SessionManifest, Storage,
        WalSegmentClosedEvent, WalWriteRequest, WalWriter, prepend_metadata_columns,
    };

    use super::*;

    #[tokio::test]
    async fn resolve_returns_pending_wal_and_committed_parquet_without_duplicates() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let staging_dir = TempDir::new().expect("staging dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");

        let session_a = SessionId::new();
        let session_b = SessionId::new();
        SessionManifest::create(
            &storage,
            session_a.clone(),
            Utc::now(),
            AmberConfig::default(),
        )
        .await
        .expect("session A manifest should be created");
        SessionManifest::create(
            &storage,
            session_b.clone(),
            Utc::now(),
            AmberConfig::default(),
        )
        .await
        .expect("session B manifest should be created");

        let mut writer = WalWriter::spawn_local(storage.clone(), staging_dir.path());
        writer
            .write(WalWriteRequest::new(
                session_a.clone(),
                "camera",
                "image",
                "schema-v1",
                metadata_enriched_batch(vec![1], vec![Some("a")], vec![100], vec![110]),
            ))
            .await
            .expect("session A write should succeed");
        writer
            .rotate(crate::WalRotateRequest::new(
                session_a.clone(),
                "camera",
                "image",
            ))
            .await
            .expect("session A rotation should succeed");
        writer
            .write(WalWriteRequest::new(
                session_b,
                "camera",
                "image",
                "schema-v1",
                metadata_enriched_batch(vec![2], vec![Some("b")], vec![200], vec![210]),
            ))
            .await
            .expect("session B write should succeed");
        writer
            .rotate(crate::WalRotateRequest::new(
                session_a.clone(),
                "missing",
                "stream",
            ))
            .await
            .expect("unrelated rotate should be a no-op");
        writer
            .rotate(crate::WalRotateRequest::new(
                SessionId::new(),
                "camera",
                "image",
            ))
            .await
            .expect("wrong session rotate should be a no-op");
        writer
            .write(WalWriteRequest::new(
                session_a.clone(),
                "joint_states",
                "state",
                "schema-v2",
                metadata_enriched_batch(vec![3], vec![Some("c")], vec![300], vec![310]),
            ))
            .await
            .expect("pending WAL write should succeed");

        writer
            .rotate(crate::WalRotateRequest::new(
                SessionId::parse("018f3f2b-1111-7aaa-8bbb-222233334444").expect("uuidv7"),
                "camera",
                "image",
            ))
            .await
            .expect("another no-op rotate should succeed");

        writer
            .rotate(crate::WalRotateRequest::new(
                SessionId::parse("018f3f2b-1111-7aaa-8bbb-222233334445").expect("uuidv7"),
                "joint_states",
                "state",
            ))
            .await
            .expect("another unrelated rotate should succeed");

        writer
            .rotate(crate::WalRotateRequest::new(
                session_a.clone(),
                "camera",
                "image",
            ))
            .await
            .expect("no-op rotate after closure should succeed");
        writer
            .rotate(crate::WalRotateRequest::new(
                SessionId::parse("018f3f2b-1111-7aaa-8bbb-222233334446").expect("uuidv7"),
                "camera",
                "image",
            ))
            .await
            .expect("another no-op rotate should succeed");

        let compaction = crate::Compactor::new(storage.clone(), 64);
        compaction
            .compact_pending()
            .await
            .expect("compaction should succeed")
            .expect("camera segments should compact");

        writer
            .shutdown()
            .await
            .expect("writer shutdown should succeed");

        let sources =
            SessionSourceSet::resolve(&storage, &session_a, SessionSourceFilter::default())
                .await
                .expect("source set should resolve");

        assert_eq!(sources.session_id, session_a);
        assert_eq!(
            sources.session_root,
            paths::session_root(sources.session_id.as_str())
        );
        assert_eq!(sources.groups.len(), 2);

        let camera_group = sources
            .groups
            .iter()
            .find(|group| group.node_id == "camera" && group.output_id == "image")
            .expect("camera group should exist");
        assert!(camera_group.wal_sources.is_empty());
        assert_eq!(camera_group.parquet_sources.len(), 1);
        assert_eq!(
            camera_group.parquet_sources[0].session_id_filter,
            sources.session_id
        );

        let pending_group = sources
            .groups
            .iter()
            .find(|group| group.node_id == "joint_states" && group.output_id == "state")
            .expect("pending WAL group should exist");
        assert_eq!(pending_group.wal_sources.len(), 1);
        assert!(pending_group.parquet_sources.is_empty());
        assert_eq!(pending_group.wal_sources[0].session_id, sources.session_id);

        let filtered = SessionSourceSet::resolve(
            &storage,
            &sources.session_id,
            SessionSourceFilter {
                node_id: Some("camera".to_owned()),
                output_id: Some("image".to_owned()),
            },
        )
        .await
        .expect("filtered source set should resolve");
        assert_eq!(filtered.groups.len(), 1);
        assert_eq!(filtered.groups[0], camera_group.clone());
    }

    #[tokio::test]
    async fn resolve_keeps_parquet_sources_after_cleanup() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let staging_dir = TempDir::new().expect("staging dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");

        let session_a = SessionId::new();
        let session_b = SessionId::new();
        SessionManifest::create(
            &storage,
            session_a.clone(),
            Utc::now(),
            AmberConfig::default(),
        )
        .await
        .expect("session A manifest should be created");
        SessionManifest::create(
            &storage,
            session_b.clone(),
            Utc::now(),
            AmberConfig::default(),
        )
        .await
        .expect("session B manifest should be created");

        let mut writer = WalWriter::spawn_local(storage.clone(), staging_dir.path());
        writer
            .write(WalWriteRequest::new(
                session_a.clone(),
                "camera",
                "image",
                "schema-v1",
                metadata_enriched_batch(vec![1], vec![Some("a")], vec![100], vec![110]),
            ))
            .await
            .expect("session A write should succeed");
        writer
            .rotate(crate::WalRotateRequest::new(
                session_a.clone(),
                "camera",
                "image",
            ))
            .await
            .expect("session A rotation should succeed");
        writer
            .write(WalWriteRequest::new(
                session_b,
                "camera",
                "image",
                "schema-v1",
                metadata_enriched_batch(vec![2], vec![Some("b")], vec![200], vec![210]),
            ))
            .await
            .expect("session B write should succeed");
        writer
            .shutdown()
            .await
            .expect("writer shutdown should succeed");

        let compactor = crate::Compactor::new(storage.clone(), 64);
        compactor
            .compact_pending()
            .await
            .expect("compaction should succeed")
            .expect("pending segments should compact");
        compactor
            .cleanup_compacted()
            .await
            .expect("cleanup should succeed");

        let sources =
            SessionSourceSet::resolve(&storage, &session_a, SessionSourceFilter::default())
                .await
                .expect("source set should resolve");
        assert_eq!(sources.groups.len(), 1);
        assert!(sources.groups[0].wal_sources.is_empty());
        assert_eq!(sources.groups[0].parquet_sources.len(), 1);
    }

    #[tokio::test]
    async fn resolve_rejects_multiple_schema_fingerprints_for_same_stream() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");
        let session_id = SessionId::new();
        SessionManifest::create(
            &storage,
            session_id.clone(),
            Utc::now(),
            AmberConfig::default(),
        )
        .await
        .expect("session manifest should be created");

        let first_segment_id = crate::WalSegmentId::new();
        let second_segment_id = crate::WalSegmentId::new();
        let first_path = paths::wal_segment(
            session_id.as_str(),
            "camera",
            "image",
            &format!("segment-{first_segment_id}.arrow"),
        );
        let second_path = paths::wal_segment(
            session_id.as_str(),
            "camera",
            "image",
            &format!("segment-{second_segment_id}.arrow"),
        );

        for (segment_id, schema_fingerprint, path) in [
            (first_segment_id, "schema-v1", first_path),
            (second_segment_id, "schema-v2", second_path),
        ] {
            CatalogEvent::WalSegmentClosed(WalSegmentClosedEvent {
                event_id: crate::CatalogEventId::new(),
                segment_id,
                session_id: session_id.clone(),
                node_id: "camera".to_owned(),
                output_id: "image".to_owned(),
                schema_fingerprint: schema_fingerprint.to_owned(),
                path: path.to_string(),
                row_count: 1,
                byte_size: 1,
                min_node_timestamp: 1,
                max_node_timestamp: 1,
                min_amber_timestamp: 2,
                max_amber_timestamp: 2,
                opened_at: Utc::now(),
                closed_at: Utc::now(),
            })
            .save(&storage)
            .await
            .expect("closed event should save");
        }

        let error =
            SessionSourceSet::resolve(&storage, &session_id, SessionSourceFilter::default())
                .await
                .expect_err("schema conflict should fail");
        assert!(matches!(error, SessionSourceError::SchemaConflict { .. }));
        let message = error.to_string();
        assert!(message.contains(session_id.as_str()));
        assert!(message.contains("schema-v1"));
        assert!(message.contains("schema-v2"));
    }

    fn metadata_enriched_batch(
        values: Vec<i32>,
        labels: Vec<Option<&str>>,
        node_timestamps: Vec<i64>,
        amber_timestamps: Vec<i64>,
    ) -> RecordBatch {
        let payload = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int32, false),
                Field::new("label", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(values)),
                Arc::new(StringArray::from(labels)),
            ],
        )
        .expect("payload batch should build");

        prepend_metadata_columns(
            &payload,
            &RecordBatchMetadata::new(
                "session-1",
                "node-a",
                "output-x",
                node_timestamps,
                amber_timestamps,
            ),
        )
        .expect("metadata enrichment should work")
    }
}
