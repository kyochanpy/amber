use std::collections::BTreeMap;

use amber_core::{
    CatalogState, FoldedWalSegmentState, ObjectPath, SessionId, SessionManifest, Storage,
};
use anyhow::{Context, Result};

use crate::cli::ListArgs;
use crate::config::load_storage;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionListEntry {
    pub manifest: SessionManifest,
    pub has_pending_wal: bool,
    pub has_committed_parquet: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionPhysicalState {
    pub has_pending_wal: bool,
    pub has_committed_parquet: bool,
}

pub async fn run_list(args: &ListArgs) -> Result<Vec<SessionListEntry>> {
    let storage = load_storage(&args.config, args.data_dir.as_deref())?;
    let mut manifests = list_session_manifests(&storage).await?;
    manifests.sort_by(|left, right| {
        right
            .started_at
            .cmp(&left.started_at)
            .then_with(|| right.session_id.cmp(&left.session_id))
    });

    if let Some(tag) = &args.tag {
        manifests.retain(|manifest| manifest.tags.iter().any(|candidate| candidate == tag));
    }

    let states = load_session_physical_states(&storage).await?;
    let mut sessions = manifests
        .into_iter()
        .map(|manifest| {
            let state = states
                .get(&manifest.session_id)
                .copied()
                .unwrap_or_default();
            SessionListEntry {
                manifest,
                has_pending_wal: state.has_pending_wal,
                has_committed_parquet: state.has_committed_parquet,
            }
        })
        .collect::<Vec<_>>();

    if args.latest {
        sessions.truncate(1);
    } else if let Some(limit) = args.limit {
        sessions.truncate(limit);
    }

    Ok(sessions)
}

pub async fn list_session_manifests(storage: &Storage) -> Result<Vec<SessionManifest>> {
    let mut manifests = Vec::new();
    for meta in storage
        .list_prefix(&ObjectPath::from("sessions"))
        .await
        .context("failed to enumerate session manifests")?
    {
        if meta.location.filename() != Some("manifest.json") {
            continue;
        }

        let path = meta.location.clone();
        let manifest = match storage.get_json::<SessionManifest>(&path).await {
            Ok(manifest) => manifest,
            Err(error) => {
                eprintln!(
                    "warning: failed to load session manifest '{}': {}",
                    path, error
                );
                continue;
            }
        };
        manifests.push(manifest);
    }

    Ok(manifests)
}

pub async fn load_session_physical_states(
    storage: &Storage,
) -> Result<BTreeMap<SessionId, SessionPhysicalState>> {
    let catalog = CatalogState::load(storage)
        .await
        .context("failed to fold catalog events while listing sessions")?;
    let mut states = BTreeMap::<SessionId, SessionPhysicalState>::new();

    for segment in catalog.wal_segments.values() {
        let state = states.entry(segment.session_id.clone()).or_default();
        match segment.state {
            FoldedWalSegmentState::Pending => {
                state.has_pending_wal = true;
            }
            FoldedWalSegmentState::Compacted | FoldedWalSegmentState::Deleted => {
                state.has_committed_parquet = true;
            }
        }
    }

    Ok(states)
}

#[cfg(test)]
mod tests {
    use amber_core::{
        CatalogEvent, CatalogEventId, ParquetFileId, PublishedParquetFile, SessionStatus,
        WalSegmentClosedEvent, WalSegmentId, paths,
    };
    use chrono::{Duration, TimeZone, Utc};
    use tempfile::TempDir;

    use super::*;
    use crate::output::render_session_list;
    use crate::test_support::{create_manifest, write_config};

    #[tokio::test]
    async fn list_command_filters_and_summarizes_sessions() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let config_path = write_config(storage_dir.path()).expect("config should be written");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");
        let base = Utc.with_ymd_and_hms(2026, 5, 10, 12, 0, 0).unwrap();

        let session_old =
            create_manifest(&storage, base, SessionStatus::Closed, &["robot-a"], 2).await;
        let session_mid = create_manifest(
            &storage,
            base + Duration::minutes(5),
            SessionStatus::Open,
            &[],
            1,
        )
        .await;
        let session_new = create_manifest(
            &storage,
            base + Duration::minutes(10),
            SessionStatus::Interrupted,
            &["robot-b"],
            3,
        )
        .await;

        let compacted_segment_id = WalSegmentId::new();
        let pending_segment_id = WalSegmentId::new();
        let parquet_file_id = ParquetFileId::new();

        CatalogEvent::WalSegmentClosed(WalSegmentClosedEvent {
            event_id: CatalogEventId::new(),
            segment_id: compacted_segment_id.clone(),
            session_id: session_old.manifest.session_id.clone(),
            node_id: "camera/front".to_owned(),
            output_id: "frames/raw".to_owned(),
            schema_fingerprint: "schema-old".to_owned(),
            path: paths::wal_segment(
                session_old.manifest.session_id.as_str(),
                "camera/front",
                "frames/raw",
                &format!("segment-{compacted_segment_id}.arrow"),
            )
            .to_string(),
            row_count: 10,
            byte_size: 100,
            min_node_timestamp: 1,
            max_node_timestamp: 10,
            min_amber_timestamp: 2,
            max_amber_timestamp: 11,
            opened_at: base,
            closed_at: base + Duration::seconds(30),
        })
        .save(&storage)
        .await
        .expect("closed event should save");
        CatalogEvent::CompactionCommitted(amber_core::CompactionCommittedEvent {
            event_id: CatalogEventId::new(),
            compaction_id: amber_core::CompactionId::new(),
            source_wal_segments: vec![compacted_segment_id],
            created_parquet_files: vec![PublishedParquetFile {
                file_id: parquet_file_id.clone(),
                node_id: "camera/front".to_owned(),
                output_id: "frames/raw".to_owned(),
                schema_fingerprint: "schema-old".to_owned(),
                path: paths::parquet_file(
                    "camera/front",
                    "frames/raw",
                    "schema-old",
                    &format!("part-{parquet_file_id}.parquet"),
                )
                .to_string(),
                row_count: 10,
                byte_size: 100,
                min_node_timestamp: 1,
                max_node_timestamp: 10,
                min_amber_timestamp: 2,
                max_amber_timestamp: 11,
                created_at: base + Duration::minutes(1),
            }],
            committed_at: base + Duration::minutes(1),
        })
        .save(&storage)
        .await
        .expect("compaction event should save");
        CatalogEvent::WalSegmentClosed(WalSegmentClosedEvent {
            event_id: CatalogEventId::new(),
            segment_id: pending_segment_id.clone(),
            session_id: session_new.manifest.session_id.clone(),
            node_id: "camera/rear".to_owned(),
            output_id: "frames/raw".to_owned(),
            schema_fingerprint: "schema-new".to_owned(),
            path: paths::wal_segment(
                session_new.manifest.session_id.as_str(),
                "camera/rear",
                "frames/raw",
                &format!("segment-{pending_segment_id}.arrow"),
            )
            .to_string(),
            row_count: 20,
            byte_size: 200,
            min_node_timestamp: 20,
            max_node_timestamp: 40,
            min_amber_timestamp: 21,
            max_amber_timestamp: 41,
            opened_at: base + Duration::minutes(10),
            closed_at: base + Duration::minutes(11),
        })
        .save(&storage)
        .await
        .expect("pending closed event should save");

        let entries = run_list(&ListArgs {
            config: config_path.clone(),
            data_dir: None,
            latest: false,
            limit: None,
            tag: None,
        })
        .await
        .expect("list should succeed");

        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.manifest.session_id.clone())
                .collect::<Vec<_>>(),
            vec![
                session_new.manifest.session_id.clone(),
                session_mid.manifest.session_id.clone(),
                session_old.manifest.session_id.clone(),
            ]
        );
        assert_eq!(entries[0].manifest.observed_streams.len(), 3);
        assert!(entries[0].has_pending_wal);
        assert!(!entries[0].has_committed_parquet);
        assert_eq!(entries[2].manifest.observed_streams.len(), 2);
        assert!(!entries[2].has_pending_wal);
        assert!(entries[2].has_committed_parquet);

        let latest = run_list(&ListArgs {
            config: config_path.clone(),
            data_dir: None,
            latest: true,
            limit: Some(2),
            tag: None,
        })
        .await
        .expect("latest list should succeed");
        assert_eq!(latest.len(), 1);
        assert_eq!(
            latest[0].manifest.session_id,
            session_new.manifest.session_id
        );

        let limited = run_list(&ListArgs {
            config: config_path.clone(),
            data_dir: None,
            latest: false,
            limit: Some(2),
            tag: None,
        })
        .await
        .expect("limited list should succeed");
        assert_eq!(limited.len(), 2);

        let tagged = run_list(&ListArgs {
            config: config_path,
            data_dir: None,
            latest: false,
            limit: None,
            tag: Some("robot-a".to_owned()),
        })
        .await
        .expect("tagged list should succeed");
        assert_eq!(tagged.len(), 1);
        assert_eq!(
            tagged[0].manifest.session_id,
            session_old.manifest.session_id
        );

        let rendered = render_session_list(&entries);
        assert!(rendered.contains("SESSION ID"));
        assert!(rendered.contains(session_new.manifest.session_id.as_str()));
        assert!(rendered.contains("yes"));
    }
}
