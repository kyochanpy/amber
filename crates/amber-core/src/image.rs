use std::{collections::HashMap, io::Cursor, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, Int32Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use image::ImageFormat;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    NormalizedPayloadSchema, ObjectPath, SessionId, Storage, StorageError,
    normalized_payload_schema, schema_fingerprint_for_payload, storage::paths,
};

const IMAGE_FORMAT_KEYS: &[&str] = &["image_format", "image_encoding", "format", "mime_type"];

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedImageBatch {
    pub schema_fingerprint: String,
    pub normalized_payload_schema: NormalizedPayloadSchema,
    pub metadata_batch: RecordBatch,
}

#[derive(Debug, Error)]
pub enum ImageError {
    #[error("compressed image column '{field_name}' must use LargeBinary")]
    InvalidColumnType { field_name: String },
    #[error("compressed image column '{field_name}' is missing format metadata")]
    MissingFormatMetadata { field_name: String },
    #[error("compressed image format '{value}' is not supported")]
    UnsupportedFormat { value: String },
    #[error("compressed image row {row_index} is null")]
    NullImage { row_index: usize },
    #[error("failed to decode compressed image row {row_index}: {source}")]
    DecodeImage {
        row_index: usize,
        #[source]
        source: image::ImageError,
    },
    #[error("failed to persist image asset '{path}': {source}")]
    PersistAsset {
        path: ObjectPath,
        #[source]
        source: Box<StorageError>,
    },
    #[error("failed to verify persisted image asset '{path}': {source}")]
    VerifyAsset {
        path: ObjectPath,
        #[source]
        source: Box<StorageError>,
    },
    #[error("persisted image asset '{path}' was not visible after write")]
    MissingPersistedAsset { path: ObjectPath },
    #[error("failed to build image metadata batch: {source}")]
    BuildMetadataBatch {
        #[source]
        source: arrow::error::ArrowError,
    },
}

impl ImageError {
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Self::InvalidColumnType { .. }
                | Self::MissingFormatMetadata { .. }
                | Self::UnsupportedFormat { .. }
                | Self::NullImage { .. }
                | Self::DecodeImage { .. }
                | Self::MissingPersistedAsset { .. }
                | Self::BuildMetadataBatch { .. }
        )
    }
}

pub async fn prepare_compressed_image_batch(
    storage: &Storage,
    session_id: &SessionId,
    node_id: &str,
    output_id: &str,
    payload_batch: &RecordBatch,
) -> Result<Option<PreparedImageBatch>, ImageError> {
    let Some((images, format)) =
        compressed_image_column(payload_batch.schema().as_ref(), payload_batch)?
    else {
        return Ok(None);
    };

    let mut widths = Vec::with_capacity(images.len());
    let mut heights = Vec::with_capacity(images.len());
    let mut channels = Vec::with_capacity(images.len());
    let mut formats = Vec::with_capacity(images.len());
    let mut asset_relpaths = Vec::with_capacity(images.len());
    let mut byte_sizes = Vec::with_capacity(images.len());

    for row_index in 0..images.len() {
        if images.is_null(row_index) {
            return Err(ImageError::NullImage { row_index });
        }

        let bytes = images.value(row_index).to_vec();
        let byte_size = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
        let decoded = image::load(Cursor::new(&bytes), format).map_err(|source| {
            ImageError::DecodeImage {
                row_index,
                source,
            }
        })?;
        let asset_path = paths::session_asset(
            session_id.as_str(),
            node_id,
            output_id,
            &format!("asset-{}-{}.{}", Uuid::now_v7(), row_index, format_extension(format)),
        );

        storage
            .put_bytes(&asset_path, bytes)
            .await
            .map_err(|source| ImageError::PersistAsset {
                path: asset_path.clone(),
                source: Box::new(source),
            })?;
        let exists = storage
            .exists(&asset_path)
            .await
            .map_err(|source| ImageError::VerifyAsset {
                path: asset_path.clone(),
                source: Box::new(source),
            })?;
        if !exists {
            return Err(ImageError::MissingPersistedAsset {
                path: asset_path.clone(),
            });
        }

        widths.push(i32::try_from(decoded.width()).unwrap_or(i32::MAX));
        heights.push(i32::try_from(decoded.height()).unwrap_or(i32::MAX));
        channels.push(i32::from(decoded.color().channel_count()));
        formats.push(format_name(format).to_owned());
        asset_relpaths.push(asset_path.to_string());
        byte_sizes.push(byte_size);
    }

    let metadata_schema = Arc::new(Schema::new(vec![
        Field::new("width", DataType::Int32, false),
        Field::new("height", DataType::Int32, false),
        Field::new("channels", DataType::Int32, false),
        Field::new("format", DataType::Utf8, false),
        Field::new("asset_relpath", DataType::Utf8, false),
        Field::new("byte_size", DataType::Int64, false),
    ]));
    let metadata_batch = RecordBatch::try_new(
        metadata_schema,
        vec![
            Arc::new(Int32Array::from(widths)) as ArrayRef,
            Arc::new(Int32Array::from(heights)) as ArrayRef,
            Arc::new(Int32Array::from(channels)) as ArrayRef,
            Arc::new(StringArray::from(formats)) as ArrayRef,
            Arc::new(StringArray::from(asset_relpaths)) as ArrayRef,
            Arc::new(Int64Array::from(byte_sizes)) as ArrayRef,
        ],
    )
    .map_err(|source| ImageError::BuildMetadataBatch { source })?;

    Ok(Some(PreparedImageBatch {
        // The schema catalog entry for image streams describes the normalized input payload
        // semantics used for fingerprinting, not the derived WAL metadata columns.
        schema_fingerprint: schema_fingerprint_for_payload(payload_batch.schema().as_ref()),
        normalized_payload_schema: normalized_payload_schema(payload_batch.schema().as_ref()),
        metadata_batch,
    }))
}

fn compressed_image_column<'a>(
    schema: &Schema,
    batch: &'a RecordBatch,
) -> Result<Option<(&'a LargeBinaryArray, ImageFormat)>, ImageError> {
    if schema.fields.len() != 1 {
        return Ok(None);
    }

    let field = schema.field(0);
    if field.data_type() != &DataType::LargeBinary {
        return Ok(None);
    }

    let format_value = image_format_metadata(field.metadata()).ok_or_else(|| {
        ImageError::MissingFormatMetadata {
            field_name: field.name().to_owned(),
        }
    })?;
    let format = parse_image_format(format_value)?;
    let images =
        batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| ImageError::InvalidColumnType {
                field_name: field.name().to_owned(),
            })?;

    Ok(Some((images, format)))
}

fn image_format_metadata(metadata: &HashMap<String, String>) -> Option<&str> {
    IMAGE_FORMAT_KEYS
        .iter()
        .find_map(|key| metadata.get(*key).map(String::as_str))
}

fn parse_image_format(value: &str) -> Result<ImageFormat, ImageError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "jpeg" | "jpg" | "image/jpeg" => Ok(ImageFormat::Jpeg),
        "png" | "image/png" => Ok(ImageFormat::Png),
        other => Err(ImageError::UnsupportedFormat {
            value: other.to_owned(),
        }),
    }
}

fn format_name(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Jpeg => "jpeg",
        ImageFormat::Png => "png",
        _ => unreachable!("parse_image_format only returns jpeg or png"),
    }
}

fn format_extension(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Jpeg => "jpeg",
        ImageFormat::Png => "png",
        _ => unreachable!("parse_image_format only returns jpeg or png"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;

    use arrow::array::LargeBinaryArray;
    use image::{ColorType, ImageEncoder, codecs::png::PngEncoder};
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn prepare_compressed_image_batch_persists_assets_and_builds_metadata() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");
        let session_id = SessionId::new();
        let payload = compressed_png_batch();

        let prepared = prepare_compressed_image_batch(
            &storage,
            &session_id,
            "camera",
            "image",
            &payload,
        )
        .await
        .expect("image batch should prepare")
        .expect("payload should be recognized as compressed image");

        assert_eq!(prepared.metadata_batch.num_rows(), 1);
        assert_eq!(prepared.metadata_batch.schema().field(0).name(), "width");
        assert_eq!(prepared.normalized_payload_schema.fields[0].metadata["image_format"], "png");

        let asset_relpath = prepared
            .metadata_batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("asset_relpath should be utf8")
            .value(0)
            .to_owned();
        assert!(asset_relpath.starts_with(&format!(
            "sessions/session_id={session_id}/assets/node_id=camera/output_id=image/"
        )));
        assert!(
            storage
                .exists(&ObjectPath::from(asset_relpath))
                .await
                .expect("asset should exist")
        );
    }

    #[test]
    fn prepare_compressed_image_batch_ignores_non_image_payloads() {
        let payload = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
        )
        .expect("batch should build");

        assert!(
            compressed_image_column(payload.schema().as_ref(), &payload)
                .expect("non-image schema should be ignored")
                .is_none()
        );
    }

    fn compressed_png_batch() -> RecordBatch {
        let bytes = tiny_png_bytes();
        let mut field = Field::new("image", DataType::LargeBinary, false);
        field.set_metadata(HashMap::from([(
            "image_format".to_owned(),
            "png".to_owned(),
        )]));

        RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(LargeBinaryArray::from(vec![Some(bytes.as_slice())])) as ArrayRef],
        )
        .expect("compressed image batch should build")
    }

    fn tiny_png_bytes() -> Vec<u8> {
        let mut encoded = Cursor::new(Vec::new());
        PngEncoder::new(&mut encoded)
            .write_image(&[255, 0, 0], 1, 1, ColorType::Rgb8.into())
            .expect("PNG encoding should succeed");
        encoded.into_inner()
    }
}
