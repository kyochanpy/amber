use std::path::{Path, PathBuf};

use amber_core::{
    AmberConfig, CatalogEvent, CatalogState, Compactor, FoldedWalSegmentState, ObjectPath,
    SessionManifest, SessionStatus, Storage, StorageBackend,
};
use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "amber", version, about = "Amber command-line tools")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Compact(CompactArgs),
    List(ListArgs),
}

#[derive(Debug, Args)]
struct CompactArgs {
    #[arg(long, default_value = "amber.yaml")]
    config: PathBuf,
    #[arg(long)]
    cleanup: bool,
}

#[derive(Debug, Args)]
struct ListArgs {
    #[arg(long, default_value = "amber.yaml")]
    config: PathBuf,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    latest: bool,
    #[arg(long)]
    limit: Option<usize>,
    #[arg(long)]
    tag: Option<String>,
}

#[derive(Debug, Clone)]
struct CompactSummary {
    created_parquet_files: usize,
    compacted_segments: usize,
    deleted_segments: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionListEntry {
    manifest: SessionManifest,
    has_pending_wal: bool,
    has_committed_parquet: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli).await
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Compact(args) => {
            let summary = run_compact(&args).await?;

            if summary.compacted_segments == 0 && summary.deleted_segments > 0 {
                println!(
                    "No new WAL segments required compaction. Deleted {} previously compacted WAL segment(s).",
                    summary.deleted_segments
                );
            } else if summary.compacted_segments == 0 {
                println!("No eligible closed WAL segments found for compaction.");
            } else {
                println!(
                    "Compacted {} WAL segment(s) into {} Parquet file(s).",
                    summary.compacted_segments, summary.created_parquet_files
                );
            }

            if args.cleanup && summary.compacted_segments > 0 {
                if summary.deleted_segments == 0 {
                    println!("No compacted WAL segments required cleanup.");
                } else {
                    println!(
                        "Deleted {} compacted WAL segment(s).",
                        summary.deleted_segments
                    );
                }
            }
        }
        Command::List(args) => {
            let sessions = run_list(&args).await?;
            print!("{}", render_session_list(&sessions));
        }
    }

    Ok(())
}

async fn run_compact(args: &CompactArgs) -> Result<CompactSummary> {
    let config = load_config(&args.config)?;
    let storage = Storage::from_config(&config.storage).with_context(|| {
        format!(
            "failed to initialize storage backend '{}' from '{}'",
            config.storage.backend,
            args.config.display()
        )
    })?;
    let compactor = Compactor::new(storage, config.compaction.target_file_mb);

    let compaction_event = compactor
        .compact_pending()
        .await
        .context("compaction command failed")?;
    let deleted_segments = if args.cleanup {
        compactor
            .cleanup_compacted()
            .await
            .context("cleanup after compaction failed")?
            .len()
    } else {
        0
    };

    Ok(CompactSummary {
        created_parquet_files: compaction_event
            .as_ref()
            .map(|event| event.created_parquet_files.len())
            .unwrap_or(0),
        compacted_segments: compaction_event
            .as_ref()
            .map(|event| event.source_wal_segments.len())
            .unwrap_or(0),
        deleted_segments,
    })
}

fn load_config(path: &Path) -> Result<AmberConfig> {
    AmberConfig::from_file(path)
        .with_context(|| format!("failed to load amber config from '{}'", path.display()))
}

fn load_storage(config_path: &Path, data_dir: Option<&Path>) -> Result<Storage> {
    let mut config = load_config(config_path)?;
    if let Some(data_dir) = data_dir {
        if config.storage.backend != StorageBackend::Local {
            anyhow::bail!(
                "--data-dir only supports the local storage backend, found '{}'",
                config.storage.backend
            );
        }
        config.storage.path = Some(data_dir.to_path_buf());
    }

    Storage::from_config(&config.storage).with_context(|| {
        format!(
            "failed to initialize storage backend '{}' from '{}'",
            config.storage.backend,
            config_path.display()
        )
    })
}

async fn run_list(args: &ListArgs) -> Result<Vec<SessionListEntry>> {
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

async fn list_session_manifests(storage: &Storage) -> Result<Vec<SessionManifest>> {
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
        let manifest = storage
            .get_json::<SessionManifest>(&path)
            .await
            .with_context(|| format!("failed to load session manifest '{}'", path))?;
        manifests.push(manifest);
    }

    Ok(manifests)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SessionPhysicalState {
    has_pending_wal: bool,
    has_committed_parquet: bool,
}

async fn load_session_physical_states(
    storage: &Storage,
) -> Result<std::collections::BTreeMap<amber_core::SessionId, SessionPhysicalState>> {
    let events = CatalogEvent::list(storage)
        .await
        .context("failed to load catalog events while listing sessions")?;
    let catalog = CatalogState::from_events(events.iter().cloned())
        .context("failed to fold catalog events while listing sessions")?;
    let mut states =
        std::collections::BTreeMap::<amber_core::SessionId, SessionPhysicalState>::new();

    for segment in catalog.wal_segments.values() {
        let state = states.entry(segment.session_id.clone()).or_default();
        if segment.state == FoldedWalSegmentState::Pending {
            state.has_pending_wal = true;
        }
    }

    for event in events {
        let CatalogEvent::CompactionCommitted(event) = event else {
            continue;
        };

        for segment_id in event.source_wal_segments {
            let Some(segment) = catalog.wal_segments.get(&segment_id) else {
                continue;
            };
            states
                .entry(segment.session_id.clone())
                .or_default()
                .has_committed_parquet = true;
        }
    }

    Ok(states)
}

fn render_session_list(entries: &[SessionListEntry]) -> String {
    if entries.is_empty() {
        return "No sessions found.\n".to_owned();
    }

    let mut lines = vec![format!(
        "{:<36}  {:<20}  {:<20}  {:<11}  {:>7}  {:<11}  {:<7}",
        "SESSION ID", "STARTED AT", "ENDED AT", "STATUS", "STREAMS", "PENDING_WAL", "PARQUET"
    )];

    for entry in entries {
        lines.push(format!(
            "{:<36}  {:<20}  {:<20}  {:<11}  {:>7}  {:<11}  {:<7}",
            entry.manifest.session_id,
            format_timestamp(entry.manifest.started_at),
            entry
                .manifest
                .ended_at
                .map(format_timestamp)
                .unwrap_or_else(|| "-".to_owned()),
            format_status(entry.manifest.status),
            entry.manifest.observed_streams.len(),
            yes_no(entry.has_pending_wal),
            yes_no(entry.has_committed_parquet),
        ));
    }

    lines.join("\n") + "\n"
}

fn format_timestamp(timestamp: chrono::DateTime<chrono::Utc>) -> String {
    timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn format_status(status: SessionStatus) -> &'static str {
    match status {
        SessionStatus::Open => "open",
        SessionStatus::Closed => "closed",
        SessionStatus::Interrupted => "interrupted",
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use amber_core::{
        CatalogEvent, CatalogEventId, ClosedWalStreamUpdate, CompactionCommittedEvent,
        CompactionId, FoldedWalSegmentState, ParquetFileId, PublishedParquetFile,
        RecordBatchMetadata, SessionId, SessionManifest, WalSegmentClosedEvent, WalSegmentId,
        WalWriteRequest, WalWriter, paths, prepend_metadata_columns,
    };
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use chrono::{Duration, TimeZone, Utc};
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn compact_command_compacts_closed_segments_without_cleanup() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let staging_dir = TempDir::new().expect("staging dir should be created");
        let config_path = write_config(storage_dir.path()).expect("config should be written");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");

        let session_id = SessionId::new();
        SessionManifest::create(
            &storage,
            session_id.clone(),
            chrono::Utc::now(),
            AmberConfig::default(),
        )
        .await
        .expect("session manifest should be created");

        let mut writer = WalWriter::spawn_local(storage.clone(), staging_dir.path());
        writer
            .write(WalWriteRequest::new(
                session_id.clone(),
                "camera",
                "image",
                "schema-v1",
                metadata_enriched_batch(vec![1], vec![Some("a")], vec![100], vec![110]),
            ))
            .await
            .expect("closed stream write should succeed");
        writer
            .rotate(amber_core::WalRotateRequest::new(
                session_id.clone(),
                "camera",
                "image",
            ))
            .await
            .expect("rotation should succeed");
        writer
            .write(WalWriteRequest::new(
                session_id,
                "joint_states",
                "state",
                "schema-v2",
                metadata_enriched_batch(vec![2], vec![Some("b")], vec![200], vec![210]),
            ))
            .await
            .expect("open stream write should succeed");

        let summary = run_compact(&CompactArgs {
            config: config_path,
            cleanup: false,
        })
        .await
        .expect("compact command should succeed");

        assert_eq!(summary.compacted_segments, 1);
        assert_eq!(summary.created_parquet_files, 1);
        assert_eq!(summary.deleted_segments, 0);

        let state = CatalogState::load(&storage)
            .await
            .expect("catalog state should load");
        assert_eq!(state.published_parquet_files.len(), 1);
        assert_eq!(
            state
                .wal_segments
                .values()
                .filter(|segment| segment.state == FoldedWalSegmentState::Compacted)
                .count(),
            1
        );
        assert_eq!(
            state
                .wal_segments
                .values()
                .filter(|segment| segment.state == FoldedWalSegmentState::Pending)
                .count(),
            0
        );

        let compacted_segment_path = state
            .wal_segments
            .values()
            .find(|segment| segment.state == FoldedWalSegmentState::Compacted)
            .expect("compacted segment should exist")
            .path
            .clone();
        assert!(
            storage
                .exists(&amber_core::ObjectPath::from(compacted_segment_path))
                .await
                .expect("compacted WAL should still exist"),
            "compact without cleanup should leave WAL objects in place"
        );

        writer
            .shutdown()
            .await
            .expect("writer shutdown should succeed after assertions");
    }

    #[tokio::test]
    async fn compact_command_can_cleanup_compacted_segments() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let staging_dir = TempDir::new().expect("staging dir should be created");
        let config_path = write_config(storage_dir.path()).expect("config should be written");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");

        let session_a = SessionId::new();
        let session_b = SessionId::new();
        SessionManifest::create(
            &storage,
            session_a.clone(),
            chrono::Utc::now(),
            AmberConfig::default(),
        )
        .await
        .expect("session A manifest should be created");
        SessionManifest::create(
            &storage,
            session_b.clone(),
            chrono::Utc::now(),
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
            .expect("first write should succeed");
        writer
            .rotate(amber_core::WalRotateRequest::new(
                session_a, "camera", "image",
            ))
            .await
            .expect("rotation should succeed");
        writer
            .write(WalWriteRequest::new(
                session_b,
                "joint_states",
                "state",
                "schema-v2",
                metadata_enriched_batch(vec![2], vec![Some("b")], vec![200], vec![210]),
            ))
            .await
            .expect("second write should succeed");
        writer
            .shutdown()
            .await
            .expect("shutdown should publish the second segment");

        let summary = run_compact(&CompactArgs {
            config: config_path,
            cleanup: true,
        })
        .await
        .expect("compact --cleanup should succeed");

        assert_eq!(summary.compacted_segments, 2);
        assert_eq!(summary.created_parquet_files, 2);
        assert_eq!(summary.deleted_segments, 2);

        let state = CatalogState::load(&storage)
            .await
            .expect("catalog state should load");
        assert_eq!(
            state
                .wal_segments
                .values()
                .filter(|segment| segment.state == FoldedWalSegmentState::Deleted)
                .count(),
            2
        );
        assert_eq!(
            CatalogEvent::list(&storage)
                .await
                .expect("catalog events should list")
                .iter()
                .filter(|event| matches!(event, CatalogEvent::WalSegmentDeleted(_)))
                .count(),
            2
        );

        for segment in state.wal_segments.values() {
            assert!(
                !storage
                    .exists(&amber_core::ObjectPath::from(segment.path.clone()))
                    .await
                    .expect("deleted WAL existence check should work"),
                "cleanup should remove compacted WAL objects"
            );
        }
    }

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
        CatalogEvent::CompactionCommitted(CompactionCommittedEvent {
            event_id: CatalogEventId::new(),
            compaction_id: CompactionId::new(),
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

    #[tokio::test]
    async fn list_command_supports_local_data_dir_override() {
        let configured_storage_dir = TempDir::new().expect("configured storage dir should exist");
        let actual_storage_dir = TempDir::new().expect("actual storage dir should exist");
        let config_path =
            write_config(configured_storage_dir.path()).expect("config should be written");
        let storage = Storage::new_local(actual_storage_dir.path(), None::<&str>).expect("storage");

        let created = create_manifest(
            &storage,
            Utc.with_ymd_and_hms(2026, 5, 10, 12, 0, 0).unwrap(),
            SessionStatus::Open,
            &[],
            1,
        )
        .await;

        let entries = run_list(&ListArgs {
            config: config_path,
            data_dir: Some(actual_storage_dir.path().to_path_buf()),
            latest: false,
            limit: None,
            tag: None,
        })
        .await
        .expect("list with data-dir override should succeed");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].manifest.session_id, created.manifest.session_id);
    }

    struct CreatedManifest {
        manifest: SessionManifest,
    }

    async fn create_manifest(
        storage: &Storage,
        started_at: chrono::DateTime<chrono::Utc>,
        status: SessionStatus,
        tags: &[&str],
        stream_count: usize,
    ) -> CreatedManifest {
        let session_id = SessionId::new();
        let mut manifest =
            SessionManifest::create(storage, session_id, started_at, AmberConfig::default())
                .await
                .expect("manifest should be created");

        for index in 0..stream_count {
            manifest.observe_closed_wal_stream(
                ClosedWalStreamUpdate::new(
                    format!("node-{index}"),
                    format!("output-{index}"),
                    format!("schema-{index}"),
                    started_at + Duration::seconds(index as i64),
                    started_at + Duration::seconds(index as i64 + 1),
                ),
                started_at + Duration::seconds(index as i64 + 2),
            );
        }

        manifest.tags = tags.iter().map(|tag| (*tag).to_owned()).collect();
        match status {
            SessionStatus::Open => {}
            SessionStatus::Closed => manifest.close(started_at + Duration::minutes(1)),
            SessionStatus::Interrupted => {
                manifest.ended_at = Some(started_at + Duration::minutes(1));
                manifest.updated_at = started_at + Duration::minutes(1);
                manifest.status = SessionStatus::Interrupted;
            }
        }
        manifest.save(storage).await.expect("manifest should save");

        CreatedManifest { manifest }
    }

    fn write_config(storage_root: &Path) -> Result<PathBuf> {
        let config_path = storage_root.join("amber.yaml");
        std::fs::write(
            &config_path,
            format!(
                "amber:\n  storage:\n    backend: local\n    path: {}\n  compaction:\n    target_file_mb: 1\n",
                storage_root.display()
            ),
        )
        .with_context(|| format!("failed to write config at '{}'", config_path.display()))?;
        Ok(config_path)
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
