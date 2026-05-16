use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use amber_core::{
    AmberConfig, ClosedWalStreamUpdate, RecordBatchMetadata, SessionManifest, SessionStatus,
    Storage, prepend_metadata_columns,
};
use anyhow::{Context, Result};
use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use chrono::{DateTime, Duration, Utc};

pub(crate) struct CreatedManifest {
    pub(crate) manifest: SessionManifest,
}

pub(crate) async fn create_manifest(
    storage: &Storage,
    started_at: DateTime<Utc>,
    status: SessionStatus,
    tags: &[&str],
    stream_count: usize,
) -> CreatedManifest {
    let session_id = amber_core::SessionId::new();
    let mut manifest = SessionManifest::create(storage, session_id, started_at, AmberConfig::default())
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

pub(crate) fn write_config(storage_root: &Path) -> Result<PathBuf> {
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

pub(crate) fn metadata_enriched_batch(
    values: Vec<i32>,
    labels: Vec<Option<&str>>,
    node_timestamps: Vec<i64>,
    amber_timestamps: Vec<i64>,
) -> RecordBatch {
    metadata_enriched_batch_for_stream(
        "session-1",
        "node-a",
        "output-x",
        values,
        labels,
        node_timestamps,
        amber_timestamps,
    )
}

pub(crate) fn metadata_enriched_batch_for_stream(
    session_id: &str,
    node_id: &str,
    output_id: &str,
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
            session_id,
            node_id,
            output_id,
            node_timestamps,
            amber_timestamps,
        ),
    )
    .expect("metadata enrichment should work")
}
