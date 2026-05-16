use std::{
    io::Cursor,
    path::Path,
};

use amber_core::{
    AMBER_TIMESTAMP_COLUMN, NODE_TIMESTAMP_COLUMN, SESSION_ID_COLUMN, SessionId,
    SessionSourceFilter, SessionSourceGroup, SessionSourceSet, Storage, is_metadata_column,
};
use anyhow::{Context, Result, anyhow, bail};
use arrow::{
    array::{Array, BooleanArray, RecordBatch, StringArray},
    compute::filter_record_batch,
    ipc::reader::StreamReader,
    util::display::array_value_to_string,
};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rerun::{
    RecordingStream, RecordingStreamBuilder, SpawnOptions, TextLog, default_flush_timeout,
};

use crate::cli::InspectArgs;
use crate::commands::list::list_session_manifests;
use crate::config::load_storage;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectSelection {
    pub session_id: SessionId,
    pub node_id: Option<String>,
    pub output_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectRow {
    pub entity_path: String,
    pub amber_row_index: i64,
    pub node_timestamp: i64,
    pub amber_timestamp: i64,
    pub text: String,
}

pub async fn run_inspect(args: &InspectArgs) -> Result<()> {
    if !args.rerun {
        bail!("inspect currently requires --rerun for MVP output");
    }
    if args.output.is_some() && args.node.is_none() {
        bail!("--output requires --node");
    }

    let storage = load_storage(&args.config, args.data_dir.as_deref())?;
    let selection = resolve_inspect_selection(args, &storage).await?;
    let source_set = SessionSourceSet::resolve(
        &storage,
        &selection.session_id,
        SessionSourceFilter {
            node_id: selection.node_id.clone(),
            output_id: selection.output_id.clone(),
        },
    )
    .await
    .with_context(|| {
        format!(
            "failed to resolve inspect sources for session '{}'",
            selection.session_id
        )
    })?;

    if let Some(node_id) = &selection.node_id {
        validate_selected_output(&source_set, node_id, selection.output_id.as_deref())?;
    }

    if source_set.groups.is_empty() {
        bail!(
            "session '{}' has no inspectable sources for the requested selection",
            selection.session_id
        );
    }

    let recording = build_rerun_recording(&selection, args.blueprint.as_deref())?;
    for_each_inspect_batch(
        &storage,
        &selection.session_id,
        &source_set,
        |group, batch, amber_row_index| {
            log_batch_to_rerun(&recording, group, batch, amber_row_index)
        },
    )
    .await?;
    recording.flush_blocking();

    Ok(())
}

pub async fn resolve_inspect_selection(
    args: &InspectArgs,
    storage: &Storage,
) -> Result<InspectSelection> {
    let session_selector = args
        .session
        .as_deref()
        .or(args.selector.as_deref())
        .unwrap_or("latest");
    let session_id = if session_selector == "latest" {
        latest_session_id(storage).await?
    } else {
        SessionId::parse(session_selector)
            .with_context(|| format!("invalid inspect session selector '{session_selector}'"))?
    };

    Ok(InspectSelection {
        session_id,
        node_id: args.node.clone(),
        output_id: args.output.clone(),
    })
}

pub async fn latest_session_id(storage: &Storage) -> Result<SessionId> {
    let mut manifests = list_session_manifests(storage).await?;
    manifests.sort_by(|left, right| {
        right
            .started_at
            .cmp(&left.started_at)
            .then_with(|| right.session_id.cmp(&left.session_id))
    });
    manifests
        .into_iter()
        .next()
        .map(|manifest| manifest.session_id)
        .ok_or_else(|| anyhow!("cannot inspect latest session because no sessions were found"))
}

pub fn validate_selected_output(
    source_set: &SessionSourceSet,
    node_id: &str,
    output_id: Option<&str>,
) -> Result<()> {
    let matching_outputs = source_set
        .groups
        .iter()
        .filter(|group| group.node_id == node_id)
        .map(|group| group.output_id.as_str())
        .collect::<Vec<_>>();

    if matching_outputs.is_empty() {
        bail!(
            "session '{}' does not contain node '{}'",
            source_set.session_id,
            node_id
        );
    }

    match output_id {
        Some(output_id)
            if matching_outputs
                .iter()
                .all(|candidate| *candidate != output_id) =>
        {
            bail!(
                "session '{}' node '{}' does not contain output '{}'",
                source_set.session_id,
                node_id,
                output_id
            );
        }
        Some(_) => {}
        None if matching_outputs.len() > 1 => {
            bail!(
                "node '{}' has multiple outputs in session '{}'; pass --output (available: {})",
                node_id,
                source_set.session_id,
                matching_outputs.join(", ")
            );
        }
        None => {}
    }

    Ok(())
}

fn build_rerun_recording(
    selection: &InspectSelection,
    blueprint: Option<&Path>,
) -> Result<RecordingStream> {
    let mut spawn_options = SpawnOptions::default();
    if let Some(path) = blueprint {
        let _ = std::fs::metadata(path)
            .with_context(|| format!("failed to access blueprint file '{}'", path.display()))?;
        spawn_options
            .extra_args
            .extend(["--blueprint".to_owned(), path.display().to_string()]);
    }

    RecordingStreamBuilder::new("amber.inspect")
        .recording_id(format!("amber-inspect-{}", selection.session_id))
        .spawn_opts(&spawn_options, default_flush_timeout())
        .context(
            "failed to spawn or connect to rerun; ensure the 'rerun' binary is installed and available on PATH",
        )
}

pub async fn collect_inspect_rows(
    storage: &Storage,
    session_id: &SessionId,
    source_set: &SessionSourceSet,
) -> Result<Vec<InspectRow>> {
    let mut rows = Vec::new();
    for_each_inspect_batch(storage, session_id, source_set, |group, batch, amber_row_index| {
        append_batch_inspect_rows(group, batch, amber_row_index, &mut rows)
    })
    .await?;

    Ok(rows)
}

pub async fn for_each_inspect_batch<F>(
    storage: &Storage,
    session_id: &SessionId,
    source_set: &SessionSourceSet,
    mut handle_batch: F,
) -> Result<()>
where
    F: FnMut(&SessionSourceGroup, &RecordBatch, &mut i64) -> Result<()>,
{
    for group in &source_set.groups {
        let mut amber_row_index = 0_i64;
        for_each_group_inspect_batch(
            storage,
            session_id,
            group,
            &mut amber_row_index,
            &mut handle_batch,
        )
        .await?;
    }

    Ok(())
}

pub async fn for_each_group_inspect_batch<F>(
    storage: &Storage,
    session_id: &SessionId,
    group: &SessionSourceGroup,
    amber_row_index: &mut i64,
    handle_batch: &mut F,
) -> Result<()>
where
    F: FnMut(&SessionSourceGroup, &RecordBatch, &mut i64) -> Result<()>,
{
    for source in &group.parquet_sources {
        let bytes = storage
            .get_bytes(&source.path)
            .await
            .with_context(|| format!("failed to read parquet source '{}'", source.path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .with_context(|| format!("failed to open parquet source '{}'", source.path))?;
        let reader = builder
            .build()
            .with_context(|| format!("failed to build parquet reader for '{}'", source.path))?;

        for batch in reader {
            let batch = batch.with_context(|| {
                format!(
                    "failed to decode parquet record batch from '{}'",
                    source.path
                )
            })?;
            let filtered = filter_batch_to_session(&batch, session_id).with_context(|| {
                format!(
                    "failed to apply session filter '{}' while reading parquet source '{}'",
                    session_id, source.path
                )
            })?;
            if filtered.num_rows() == 0 {
                continue;
            }
            handle_batch(group, &filtered, amber_row_index)?;
        }
    }

    for source in &group.wal_sources {
        let bytes = storage
            .get_bytes(&source.path)
            .await
            .with_context(|| format!("failed to read WAL source '{}'", source.path))?;
        let reader = StreamReader::try_new(Cursor::new(bytes), None)
            .with_context(|| format!("failed to open WAL stream '{}'", source.path))?;

        for batch in reader {
            let batch = batch.with_context(|| {
                format!(
                    "failed to decode Arrow IPC record batch from '{}'",
                    source.path
                )
            })?;
            handle_batch(group, &batch, amber_row_index)?;
        }
    }

    Ok(())
}

pub fn filter_batch_to_session(batch: &RecordBatch, session_id: &SessionId) -> Result<RecordBatch> {
    let session_column = batch
        .column_by_name(SESSION_ID_COLUMN)
        .ok_or_else(|| anyhow!("missing '{}' column", SESSION_ID_COLUMN))?;
    let session_column = session_column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("'{}' column must be Utf8", SESSION_ID_COLUMN))?;
    let mask = BooleanArray::from(
        (0..batch.num_rows())
            .map(|row| Some(session_column.value(row) == session_id.as_str()))
            .collect::<Vec<_>>(),
    );
    filter_record_batch(batch, &mask).context("failed to filter record batch")
}

fn log_batch_to_rerun(
    recording: &RecordingStream,
    group: &SessionSourceGroup,
    batch: &RecordBatch,
    amber_row_index: &mut i64,
) -> Result<()> {
    let entity_path = format!("{}/{}", group.node_id, group.output_id);
    let node_timestamps = typed_column::<arrow::array::Int64Array>(batch, NODE_TIMESTAMP_COLUMN)?;
    let amber_timestamps =
        typed_column::<arrow::array::Int64Array>(batch, AMBER_TIMESTAMP_COLUMN)?;

    for row_index in 0..batch.num_rows() {
        recording.set_time_sequence("amber_row", *amber_row_index);
        recording.set_time_nanos("node_time", node_timestamps.value(row_index));
        recording.set_time_nanos("amber_time", amber_timestamps.value(row_index));
        let archetype = TextLog::new(render_row_text(batch, row_index)?);

        recording
            .log(entity_path.as_str(), &archetype)
            .with_context(|| {
                format!("failed to log row {row_index} to rerun entity '{entity_path}'")
            })?;
        *amber_row_index += 1;
    }

    Ok(())
}

fn append_batch_inspect_rows(
    group: &SessionSourceGroup,
    batch: &RecordBatch,
    amber_row_index: &mut i64,
    rows: &mut Vec<InspectRow>,
) -> Result<()> {
    let entity_path = format!("{}/{}", group.node_id, group.output_id);
    let node_timestamps = typed_column::<arrow::array::Int64Array>(batch, NODE_TIMESTAMP_COLUMN)?;
    let amber_timestamps =
        typed_column::<arrow::array::Int64Array>(batch, AMBER_TIMESTAMP_COLUMN)?;

    for row_index in 0..batch.num_rows() {
        rows.push(InspectRow {
            entity_path: entity_path.clone(),
            amber_row_index: *amber_row_index,
            node_timestamp: node_timestamps.value(row_index),
            amber_timestamp: amber_timestamps.value(row_index),
            text: render_row_text(batch, row_index)?,
        });
        *amber_row_index += 1;
    }

    Ok(())
}

fn render_row_text(batch: &RecordBatch, row_index: usize) -> Result<String> {
    let mut fields = Vec::new();
    for field in batch.schema().fields() {
        if is_metadata_column(field.name()) {
            continue;
        }
        let column = batch
            .column_by_name(field.name())
            .ok_or_else(|| anyhow!("record batch is missing expected column '{}'", field.name()))?;
        let value = array_value_to_string(column.as_ref(), row_index).with_context(|| {
            format!(
                "failed to render column '{}' at row {} as text",
                field.name(),
                row_index
            )
        })?;
        fields.push(format!("{}={}", field.name(), value));
    }

    if fields.is_empty() {
        Ok("<empty payload>".to_owned())
    } else {
        Ok(fields.join(", "))
    }
}

fn typed_column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .ok_or_else(|| anyhow!("missing '{name}' column"))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow!("column '{name}' had an unexpected Arrow type"))
}

#[cfg(test)]
mod tests {
    use amber_core::{
        AmberConfig, Compactor, SessionManifest, SessionSourceFilter, WalWriteRequest, WalWriter,
    };
    use arrow::array::StringArray;
    use tempfile::TempDir;

    use super::*;
    use crate::test_support::{metadata_enriched_batch, metadata_enriched_batch_for_stream, write_config};

    #[tokio::test]
    async fn inspect_rejects_output_without_node() {
        let storage_dir = TempDir::new().expect("storage dir should exist");
        let config_path = write_config(storage_dir.path()).expect("config should be written");

        let error = run_inspect(&InspectArgs {
            selector: None,
            config: config_path,
            data_dir: None,
            session: None,
            node: None,
            output: Some("image".to_owned()),
            rerun: true,
            blueprint: None,
        })
        .await
        .expect_err("inspect should reject output without node");

        assert!(error.to_string().contains("--output requires --node"));
    }

    #[tokio::test]
    async fn inspect_latest_requires_output_when_node_has_multiple_outputs() {
        let storage_dir = TempDir::new().expect("storage dir should exist");
        let staging_dir = TempDir::new().expect("staging dir should exist");
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
                "left",
                "schema-left",
                metadata_enriched_batch(vec![1], vec![Some("a")], vec![100], vec![110]),
            ))
            .await
            .expect("left stream write should succeed");
        writer
            .rotate(amber_core::WalRotateRequest::new(
                session_id.clone(),
                "camera",
                "left",
            ))
            .await
            .expect("left rotation should succeed");
        writer
            .write(WalWriteRequest::new(
                session_id,
                "camera",
                "right",
                "schema-right",
                metadata_enriched_batch(vec![2], vec![Some("b")], vec![200], vec![210]),
            ))
            .await
            .expect("right stream write should succeed");
        writer.shutdown().await.expect("shutdown should succeed");

        let error = run_inspect(&InspectArgs {
            selector: Some("latest".to_owned()),
            config: config_path,
            data_dir: None,
            session: None,
            node: Some("camera".to_owned()),
            output: None,
            rerun: true,
            blueprint: None,
        })
        .await
        .expect_err("inspect should require --output");

        assert!(error.to_string().contains("pass --output"));
        assert!(error.to_string().contains("left"));
        assert!(error.to_string().contains("right"));
    }

    #[tokio::test]
    async fn inspect_filters_parquet_rows_to_selected_session() {
        let storage_dir = TempDir::new().expect("storage dir should exist");
        let staging_dir = TempDir::new().expect("staging dir should exist");
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
                metadata_enriched_batch_for_stream(
                    session_a.as_str(),
                    "camera",
                    "image",
                    vec![1],
                    vec![Some("session-a")],
                    vec![100],
                    vec![110],
                ),
            ))
            .await
            .expect("session A write should succeed");
        writer
            .rotate(amber_core::WalRotateRequest::new(
                session_a.clone(),
                "camera",
                "image",
            ))
            .await
            .expect("session A rotation should succeed");
        writer
            .write(WalWriteRequest::new(
                session_b.clone(),
                "camera",
                "image",
                "schema-v1",
                metadata_enriched_batch_for_stream(
                    session_b.as_str(),
                    "camera",
                    "image",
                    vec![2],
                    vec![Some("session-b")],
                    vec![200],
                    vec![210],
                ),
            ))
            .await
            .expect("session B write should succeed");
        writer
            .rotate(amber_core::WalRotateRequest::new(
                session_b.clone(),
                "camera",
                "image",
            ))
            .await
            .expect("session B rotation should succeed");
        writer.shutdown().await.expect("shutdown should succeed");

        let compactor = Compactor::new(storage.clone(), 1);
        compactor
            .compact_pending()
            .await
            .expect("compaction should succeed");

        let source_set =
            SessionSourceSet::resolve(&storage, &session_a, SessionSourceFilter::default())
                .await
                .expect("source set should resolve");
        assert_eq!(source_set.groups.len(), 1);
        assert_eq!(source_set.groups[0].parquet_sources.len(), 1);

        let parquet_path = source_set.groups[0].parquet_sources[0].path.clone();
        let bytes = storage
            .get_bytes(&parquet_path)
            .await
            .expect("parquet bytes should be readable");
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .expect("parquet builder should open")
            .build()
            .expect("parquet reader should build");

        let mut labels = Vec::new();
        for batch in reader {
            let batch = batch.expect("parquet batch should decode");
            let filtered =
                filter_batch_to_session(&batch, &session_a).expect("session filter should succeed");
            let label_column = filtered
                .column_by_name("label")
                .expect("label column should exist")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("label column should be Utf8");
            for row in 0..filtered.num_rows() {
                labels.push(label_column.value(row).to_owned());
            }
        }

        assert_eq!(labels, vec!["session-a".to_owned()]);
    }

    #[tokio::test]
    async fn inspect_row_indices_restart_for_each_group() {
        let storage_dir = TempDir::new().expect("storage dir should exist");
        let staging_dir = TempDir::new().expect("staging dir should exist");
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
                "schema-camera",
                metadata_enriched_batch_for_stream(
                    session_id.as_str(),
                    "camera",
                    "image",
                    vec![1],
                    vec![Some("camera-row")],
                    vec![100],
                    vec![110],
                ),
            ))
            .await
            .expect("camera write should succeed");
        writer
            .rotate(amber_core::WalRotateRequest::new(
                session_id.clone(),
                "camera",
                "image",
            ))
            .await
            .expect("camera rotation should succeed");
        writer
            .write(WalWriteRequest::new(
                session_id.clone(),
                "joint_states",
                "state",
                "schema-joints",
                metadata_enriched_batch_for_stream(
                    session_id.as_str(),
                    "joint_states",
                    "state",
                    vec![2],
                    vec![Some("joint-row")],
                    vec![200],
                    vec![210],
                ),
            ))
            .await
            .expect("joint write should succeed");
        writer
            .shutdown()
            .await
            .expect("writer shutdown should publish remaining WAL");

        let source_set =
            SessionSourceSet::resolve(&storage, &session_id, SessionSourceFilter::default())
                .await
                .expect("source set should resolve");
        assert_eq!(source_set.groups.len(), 2);

        let rows = collect_inspect_rows(&storage, &session_id, &source_set)
            .await
            .expect("inspect rows should collect");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter().map(|row| row.entity_path.as_str()).collect::<Vec<_>>(),
            vec!["camera/image", "joint_states/state"]
        );
        assert_eq!(
            rows.iter().map(|row| row.amber_row_index).collect::<Vec<_>>(),
            vec![0, 0]
        );
    }
}
