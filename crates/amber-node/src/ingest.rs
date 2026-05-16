use std::collections::HashMap;

use amber_core::{
    RecordBatchMetadata, SchemaCatalogEntry, SessionManifest, Storage, WalWriteReceipt,
    WalWriteRequest, WalWriter, normalized_payload_schema, prepare_image_batch,
    prepend_metadata_columns, schema_fingerprint_for_payload,
};
use anyhow::{Context, anyhow};
use dora_node_api::{ArrowData, dora_core::config::DataId};

use crate::{
    dora_arrow::{current_time_nanos, dora_data_to_record_batch},
    rotation::WalRotationRuntime,
    streams::{ConfiguredStream, StreamSchemaKey, should_record_frame},
};

#[derive(Debug)]
pub enum InputHandlingError {
    Recoverable(anyhow::Error),
    Fatal(anyhow::Error),
}

impl InputHandlingError {
    fn recoverable(error: impl Into<anyhow::Error>) -> Self {
        Self::Recoverable(error.into())
    }

    fn fatal(error: impl Into<anyhow::Error>) -> Self {
        Self::Fatal(error.into())
    }
}

pub(crate) struct IngestRuntime<'a> {
    pub(crate) storage: &'a Storage,
    pub(crate) session_manifest: &'a SessionManifest,
    pub(crate) writer: Option<&'a WalWriter>,
    pub(crate) rotation_runtime: Option<&'a WalRotationRuntime>,
    pub(crate) selected_inputs: &'a HashMap<String, ConfiguredStream>,
    pub(crate) frame_counters: &'a mut HashMap<StreamSchemaKey, u64>,
    pub(crate) stream_schemas: &'a mut HashMap<StreamSchemaKey, String>,
}

impl<'a> IngestRuntime<'a> {
    pub(crate) async fn handle_input(
        &mut self,
        input_id: DataId,
        node_timestamp: i64,
        data: ArrowData,
    ) -> Result<Option<WalWriteReceipt>, InputHandlingError> {
        let input_id = input_id.to_string();
        let Some(stream) = self.selected_inputs.get(&input_id).cloned() else {
            return Ok(None);
        };
        if !should_record_frame(self.frame_counters, &stream) {
            return Ok(None);
        }

        let payload_batch =
            dora_data_to_record_batch(data).map_err(InputHandlingError::recoverable)?;
        let prepared_image = prepare_image_batch(
            self.storage,
            &self.session_manifest.session_id,
            &stream.node_id,
            &stream.output_id,
            &payload_batch,
        )
        .await
        .map_err(|error| {
            if error.is_recoverable() {
                InputHandlingError::recoverable(error)
            } else {
                InputHandlingError::fatal(error)
            }
        })?;
        let schema_fingerprint = prepared_image
            .as_ref()
            .map(|prepared| prepared.schema_fingerprint.clone())
            .unwrap_or_else(|| schema_fingerprint_for_payload(payload_batch.schema().as_ref()));
        let normalized_schema = prepared_image
            .as_ref()
            .map(|prepared| prepared.normalized_payload_schema.clone())
            .unwrap_or_else(|| normalized_payload_schema(payload_batch.schema().as_ref()));

        let stream_key = stream.schema_key();
        if let Some(existing_schema_fingerprint) = self.stream_schemas.get(&stream_key)
            && existing_schema_fingerprint != &schema_fingerprint
        {
            return Err(InputHandlingError::fatal(anyhow!(
                "schema fingerprint changed within session for node '{}' output '{}': existing='{}', new='{}'",
                stream.node_id,
                stream.output_id,
                existing_schema_fingerprint,
                schema_fingerprint,
            )));
        }

        SchemaCatalogEntry::new(schema_fingerprint.clone(), normalized_schema)
            .save_if_absent(self.storage)
            .await
            .map_err(|source| {
                InputHandlingError::fatal(anyhow!(
                    "failed to persist schema catalog entry for node '{}' output '{}': {source}",
                    stream.node_id,
                    stream.output_id,
                ))
            })?;

        let wal_payload_batch = prepared_image
            .as_ref()
            .map(|prepared| prepared.metadata_batch.clone())
            .unwrap_or_else(|| payload_batch.clone());
        let row_count = wal_payload_batch.num_rows();
        let amber_timestamp = current_time_nanos()
            .context("failed to compute amber timestamp")
            .map_err(InputHandlingError::fatal)?;
        let enriched_batch = prepend_metadata_columns(
            &wal_payload_batch,
            &RecordBatchMetadata::new(
                self.session_manifest.session_id.as_str(),
                &stream.node_id,
                &stream.output_id,
                vec![node_timestamp; row_count],
                vec![amber_timestamp; row_count],
            ),
        )
        .map_err(InputHandlingError::recoverable)?;

        let request = WalWriteRequest::new(
            self.session_manifest.session_id.clone(),
            stream.node_id.clone(),
            stream.output_id.clone(),
            schema_fingerprint.clone(),
            enriched_batch,
        );

        if let Some(rotation_runtime) = self.rotation_runtime {
            rotation_runtime.record_write(&request).await;
        }

        let writer = self
            .writer
            .ok_or_else(|| InputHandlingError::fatal(anyhow!("WAL writer is unavailable")))?;
        let receipt = writer.write(request).await.map_err(|source| {
            InputHandlingError::fatal(anyhow!(
                "failed to write WAL batch for node '{}' output '{}': {source}",
                stream.node_id,
                stream.output_id,
            ))
        })?;

        self.stream_schemas
            .entry(stream_key)
            .or_insert(schema_fingerprint);

        Ok(Some(receipt))
    }
}
