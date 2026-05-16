use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use arrow::record_batch::RecordBatch;
use dora_node_api::{ArrowData, arrow as dora_arrow};

pub(crate) fn dora_data_to_record_batch(data: ArrowData) -> Result<RecordBatch> {
    let array = data.0;

    let dora_batch = if let Some(struct_array) = array
        .as_any()
        .downcast_ref::<dora_arrow::array::StructArray>()
    {
        if dora_arrow::array::Array::null_count(struct_array) > 0 {
            bail!("nullable top-level struct arrays are not supported for ingest");
        }
        dora_arrow::record_batch::RecordBatch::from(struct_array)
    } else {
        let field = dora_arrow::datatypes::Field::new(
            "value",
            array.data_type().clone(),
            array.null_count() > 0,
        );
        dora_arrow::record_batch::RecordBatch::try_new(
            Arc::new(dora_arrow::datatypes::Schema::new(vec![field])),
            vec![array],
        )
        .context("failed to wrap Dora array into a single-column record batch")?
    };

    let mut encoded = Vec::new();
    {
        let mut writer =
            dora_arrow::ipc::writer::StreamWriter::try_new(&mut encoded, &dora_batch.schema())
                .context("failed to create Dora Arrow IPC writer")?;
        writer
            .write(&dora_batch)
            .context("failed to encode Dora payload batch to IPC")?;
        writer
            .finish()
            .context("failed to finalize Dora payload IPC stream")?;
    }

    let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(encoded), None)
        .context("failed to decode Dora IPC stream into amber Arrow batch")?;
    reader
        .next()
        .transpose()
        .context("failed to read converted payload batch")?
        .ok_or_else(|| anyhow!("Dora IPC stream did not contain a payload batch"))
}

pub(crate) fn metadata_timestamp_nanos(metadata: &dora_node_api::Metadata) -> Result<i64> {
    system_time_to_nanos(metadata.timestamp().get_time().to_system_time())
        .context("failed to convert Dora metadata timestamp to unix nanoseconds")
}

pub(crate) fn current_time_nanos() -> Result<i64> {
    system_time_to_nanos(SystemTime::now())
}

fn system_time_to_nanos(timestamp: SystemTime) -> Result<i64> {
    let duration = timestamp
        .duration_since(UNIX_EPOCH)
        .context("timestamp predates unix epoch")?;
    i64::try_from(duration.as_nanos()).context("timestamp exceeds i64 nanosecond range")
}
