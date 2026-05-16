use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use amber_core::{RecordBatchMetadata, prepend_metadata_columns};
use anyhow::{Context, Result};
use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

pub fn write_config(storage_root: &Path) -> Result<PathBuf> {
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

pub fn metadata_enriched_batch_for_stream(
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
