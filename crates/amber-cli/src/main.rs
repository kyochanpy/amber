use std::path::{Path, PathBuf};

use amber_core::{AmberConfig, CatalogState, Compactor, Storage};
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
}

#[derive(Debug, Args)]
struct CompactArgs {
    #[arg(long, default_value = "amber.yaml")]
    config: PathBuf,
    #[arg(long)]
    cleanup: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompactSummary {
    created_parquet_files: usize,
    compacted_segments: usize,
    deleted_segments: usize,
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

            if summary.compacted_segments == 0 {
                println!("No eligible closed WAL segments found for compaction.");
            } else {
                println!(
                    "Compacted {} WAL segment(s) into {} Parquet file(s).",
                    summary.compacted_segments, summary.created_parquet_files
                );
            }

            if args.cleanup {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use amber_core::{
        CatalogEvent, FoldedWalSegmentState, RecordBatchMetadata, SessionId, SessionManifest,
        WalWriteRequest, WalWriter, prepend_metadata_columns,
    };
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
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
