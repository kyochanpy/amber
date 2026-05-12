use std::{collections::HashMap, io::Cursor, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, Int32Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use image::{
    ColorType, ImageEncoder, ImageFormat,
    codecs::{jpeg::JpegEncoder, png::PngEncoder},
};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    NormalizedPayloadSchema, ObjectPath, SessionId, Storage, StorageError,
    normalized_payload_schema, schema_fingerprint_for_payload, storage::paths,
};

const IMAGE_FORMAT_KEYS: &[&str] = &["image_format", "image_encoding", "format", "mime_type"];
const IMAGE_ENCODING_KEYS: &[&str] = &["image_encoding", "encoding"];
const IMAGE_QUALITY_KEYS: &[&str] = &["image_quality", "quality"];
const RAW_IMAGE_KIND_KEYS: &[&str] = &["image_encoding_kind", "encoding_kind"];
const DEFAULT_RAW_ENCODING: EncodedImageFormat = EncodedImageFormat::Jpeg;
const DEFAULT_JPEG_QUALITY: u8 = 85;

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
    #[error("raw image column '{field_name}' is missing shape metadata")]
    MissingRawShapeMetadata { field_name: String },
    #[error("raw image column '{field_name}' has invalid tensor_shape '{value}'")]
    InvalidTensorShape { field_name: String, value: String },
    #[error("raw image column '{field_name}' has invalid quality '{value}'")]
    InvalidQuality { field_name: String, value: String },
    #[error("raw image encoding '{value}' is not supported")]
    UnsupportedEncoding { value: String },
    #[error("raw image row {row_index} has {actual} bytes but expected {expected}")]
    RawImageByteLengthMismatch {
        row_index: usize,
        expected: usize,
        actual: usize,
    },
    #[error("raw image channels '{channels}' are not supported for {encoding}")]
    UnsupportedRawChannels {
        channels: usize,
        encoding: &'static str,
    },
    #[error("failed to encode raw image row {row_index}: {source}")]
    EncodeImage {
        row_index: usize,
        #[source]
        source: image::ImageError,
    },
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
                | Self::UnsupportedFormat { .. }
                | Self::NullImage { .. }
                | Self::DecodeImage { .. }
                | Self::MissingPersistedAsset { .. }
                | Self::MissingRawShapeMetadata { .. }
                | Self::InvalidTensorShape { .. }
                | Self::InvalidQuality { .. }
                | Self::UnsupportedEncoding { .. }
                | Self::RawImageByteLengthMismatch { .. }
                | Self::UnsupportedRawChannels { .. }
                | Self::EncodeImage { .. }
                | Self::BuildMetadataBatch { .. }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EncodedImageFormat {
    Jpeg,
    Png,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawImageDimensions {
    width: usize,
    height: usize,
    channels: usize,
}

impl RawImageDimensions {
    fn pixel_bytes(self) -> usize {
        self.width * self.height * self.channels
    }
}

pub async fn prepare_image_batch(
    storage: &Storage,
    session_id: &SessionId,
    node_id: &str,
    output_id: &str,
    payload_batch: &RecordBatch,
) -> Result<Option<PreparedImageBatch>, ImageError> {
    if let Some(prepared) =
        prepare_compressed_image_batch(storage, session_id, node_id, output_id, payload_batch)
            .await?
    {
        return Ok(Some(prepared));
    }

    prepare_raw_image_batch(storage, session_id, node_id, output_id, payload_batch).await
}

async fn prepare_compressed_image_batch(
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

    let mut rows = Vec::with_capacity(images.len());
    for row_index in 0..images.len() {
        if images.is_null(row_index) {
            return Err(ImageError::NullImage { row_index });
        }

        let bytes = images.value(row_index).to_vec();
        let decoded = image::load(Cursor::new(&bytes), format.as_image_format()).map_err(
            |source| ImageError::DecodeImage { row_index, source },
        )?;
        rows.push(ImageMetadataRow {
            width: i32::try_from(decoded.width()).unwrap_or(i32::MAX),
            height: i32::try_from(decoded.height()).unwrap_or(i32::MAX),
            channels: i32::from(decoded.color().channel_count()),
            format: format.name().to_owned(),
            asset_bytes: bytes,
            extension: format.extension(),
        });
    }

    let metadata_batch = persist_image_rows(storage, session_id, node_id, output_id, rows).await?;

    Ok(Some(PreparedImageBatch {
        // The schema catalog entry for image streams describes the normalized input payload
        // semantics used for fingerprinting, not the derived WAL metadata columns.
        schema_fingerprint: schema_fingerprint_for_payload(payload_batch.schema().as_ref()),
        normalized_payload_schema: normalized_payload_schema(payload_batch.schema().as_ref()),
        metadata_batch,
    }))
}

async fn prepare_raw_image_batch(
    storage: &Storage,
    session_id: &SessionId,
    node_id: &str,
    output_id: &str,
    payload_batch: &RecordBatch,
) -> Result<Option<PreparedImageBatch>, ImageError> {
    let schema = payload_batch.schema();
    let Some((field, images)) = raw_image_column(schema.as_ref(), payload_batch)?
    else {
        return Ok(None);
    };

    let dimensions = raw_image_dimensions(field)?;
    let encoding = raw_image_encoding(field.metadata())?;
    let quality = raw_image_quality(field.name(), field.metadata(), encoding)?;
    let expected_bytes = dimensions.pixel_bytes();

    let mut rows = Vec::with_capacity(images.len());
    for row_index in 0..images.len() {
        if images.is_null(row_index) {
            return Err(ImageError::NullImage { row_index });
        }

        let raw_bytes = images.value(row_index);
        if raw_bytes.len() != expected_bytes {
            return Err(ImageError::RawImageByteLengthMismatch {
                row_index,
                expected: expected_bytes,
                actual: raw_bytes.len(),
            });
        }

        let encoded = encode_raw_image(
            raw_bytes,
            dimensions,
            encoding,
            quality,
            row_index,
        )?;
        rows.push(ImageMetadataRow {
            width: i32::try_from(dimensions.width).unwrap_or(i32::MAX),
            height: i32::try_from(dimensions.height).unwrap_or(i32::MAX),
            channels: i32::try_from(dimensions.channels).unwrap_or(i32::MAX),
            format: encoding.name().to_owned(),
            asset_bytes: encoded,
            extension: encoding.extension(),
        });
    }

    let metadata_batch = persist_image_rows(storage, session_id, node_id, output_id, rows).await?;
    let canonical_schema = canonicalize_raw_image_schema(
        schema.as_ref(),
        dimensions,
        encoding,
        quality,
    );

    Ok(Some(PreparedImageBatch {
        // The schema catalog entry for image streams describes the normalized input payload
        // semantics used for fingerprinting, not the derived WAL metadata columns.
        schema_fingerprint: schema_fingerprint_for_payload(&canonical_schema),
        normalized_payload_schema: normalized_payload_schema(&canonical_schema),
        metadata_batch,
    }))
}

#[derive(Debug)]
struct ImageMetadataRow {
    width: i32,
    height: i32,
    channels: i32,
    format: String,
    asset_bytes: Vec<u8>,
    extension: &'static str,
}

async fn persist_image_rows(
    storage: &Storage,
    session_id: &SessionId,
    node_id: &str,
    output_id: &str,
    rows: Vec<ImageMetadataRow>,
) -> Result<RecordBatch, ImageError> {
    let mut widths = Vec::with_capacity(rows.len());
    let mut heights = Vec::with_capacity(rows.len());
    let mut channels = Vec::with_capacity(rows.len());
    let mut formats = Vec::with_capacity(rows.len());
    let mut asset_relpaths = Vec::with_capacity(rows.len());
    let mut byte_sizes = Vec::with_capacity(rows.len());

    for (row_index, row) in rows.into_iter().enumerate() {
        let asset_path = paths::session_asset(
            session_id.as_str(),
            node_id,
            output_id,
            &format!(
                "asset-{}-{}.{}",
                Uuid::now_v7(),
                row_index,
                row.extension
            ),
        );
        let byte_size = i64::try_from(row.asset_bytes.len()).unwrap_or(i64::MAX);

        storage
            .put_bytes(&asset_path, row.asset_bytes)
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

        widths.push(row.width);
        heights.push(row.height);
        channels.push(row.channels);
        formats.push(row.format);
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

    Ok(metadata_batch)
}

fn compressed_image_column<'a>(
    schema: &Schema,
    batch: &'a RecordBatch,
) -> Result<Option<(&'a LargeBinaryArray, EncodedImageFormat)>, ImageError> {
    if schema.fields.len() != 1 {
        return Ok(None);
    }

    let field = schema.field(0);
    if field.data_type() != &DataType::LargeBinary {
        return Ok(None);
    }

    let Some(format_value) = image_format_metadata(field.metadata()) else {
        return Ok(None);
    };
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

fn raw_image_column<'a>(
    schema: &'a Schema,
    batch: &'a RecordBatch,
) -> Result<Option<(&'a Field, &'a LargeBinaryArray)>, ImageError> {
    if schema.fields.len() != 1 {
        return Ok(None);
    }

    let field = schema.field(0);
    if field.data_type() != &DataType::LargeBinary {
        return Ok(None);
    }
    if image_format_metadata(field.metadata()).is_some() {
        return Ok(None);
    }
    if !looks_like_raw_image(field.metadata()) {
        return Ok(None);
    }

    let images =
        batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| ImageError::InvalidColumnType {
                field_name: field.name().to_owned(),
            })?;

    Ok(Some((field, images)))
}

fn image_format_metadata(metadata: &HashMap<String, String>) -> Option<&str> {
    IMAGE_FORMAT_KEYS
        .iter()
        .find_map(|key| metadata.get(*key).map(String::as_str))
}

fn parse_image_format(value: &str) -> Result<EncodedImageFormat, ImageError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "jpeg" | "jpg" | "image/jpeg" => Ok(EncodedImageFormat::Jpeg),
        "png" | "image/png" => Ok(EncodedImageFormat::Png),
        other => Err(ImageError::UnsupportedFormat {
            value: other.to_owned(),
        }),
    }
}

fn looks_like_raw_image(metadata: &HashMap<String, String>) -> bool {
    RAW_IMAGE_KIND_KEYS
        .iter()
        .any(|key| metadata.get(*key).is_some_and(|value| value.eq_ignore_ascii_case("raw")))
        || metadata.contains_key("tensor_shape")
        || metadata.contains_key("width")
        || metadata.contains_key("height")
        || metadata.contains_key("channels")
        || IMAGE_ENCODING_KEYS.iter().any(|key| metadata.contains_key(*key))
        || IMAGE_QUALITY_KEYS.iter().any(|key| metadata.contains_key(*key))
}

fn raw_image_dimensions(field: &Field) -> Result<RawImageDimensions, ImageError> {
    let metadata = field.metadata();
    if let Some(value) = metadata.get("tensor_shape") {
        return parse_tensor_shape(field.name(), value);
    }

    let Some(width) = metadata.get("width") else {
        return Err(ImageError::MissingRawShapeMetadata {
            field_name: field.name().to_owned(),
        });
    };
    let Some(height) = metadata.get("height") else {
        return Err(ImageError::MissingRawShapeMetadata {
            field_name: field.name().to_owned(),
        });
    };
    let Some(channels) = metadata.get("channels") else {
        return Err(ImageError::MissingRawShapeMetadata {
            field_name: field.name().to_owned(),
        });
    };

    parse_tensor_shape(
        field.name(),
        &format!("{height}x{width}x{channels}"),
    )
}

fn parse_tensor_shape(field_name: &str, value: &str) -> Result<RawImageDimensions, ImageError> {
    let parts = value
        .split(['x', 'X', ','])
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(ImageError::InvalidTensorShape {
            field_name: field_name.to_owned(),
            value: value.to_owned(),
        });
    }

    let parsed = parts
        .into_iter()
        .map(|part| part.parse::<usize>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ImageError::InvalidTensorShape {
            field_name: field_name.to_owned(),
            value: value.to_owned(),
        })?;
    if parsed.contains(&0) {
        return Err(ImageError::InvalidTensorShape {
            field_name: field_name.to_owned(),
            value: value.to_owned(),
        });
    }

    Ok(RawImageDimensions {
        height: parsed[0],
        width: parsed[1],
        channels: parsed[2],
    })
}

fn raw_image_encoding(metadata: &HashMap<String, String>) -> Result<EncodedImageFormat, ImageError> {
    let value = IMAGE_ENCODING_KEYS
        .iter()
        .find_map(|key| metadata.get(*key).map(String::as_str))
        .unwrap_or(DEFAULT_RAW_ENCODING.name());
    parse_image_format(value).map_err(|error| match error {
        ImageError::UnsupportedFormat { value } => ImageError::UnsupportedEncoding { value },
        other => other,
    })
}

fn raw_image_quality(
    field_name: &str,
    metadata: &HashMap<String, String>,
    encoding: EncodedImageFormat,
) -> Result<u8, ImageError> {
    let Some(value) = IMAGE_QUALITY_KEYS
        .iter()
        .find_map(|key| metadata.get(*key).map(String::as_str))
    else {
        return Ok(DEFAULT_JPEG_QUALITY);
    };
    let quality = value.parse::<u8>().map_err(|_| ImageError::InvalidQuality {
        field_name: field_name.to_owned(),
        value: value.to_owned(),
    })?;
    if quality == 0 || quality > 100 {
        return Err(ImageError::InvalidQuality {
            field_name: field_name.to_owned(),
            value: value.to_owned(),
        });
    }
    if encoding == EncodedImageFormat::Png {
        // PNG encoding ignores JPEG-style quality settings in the current MVP path,
        // but we still validate the provided value so invalid config is not silently ignored.
        return Ok(DEFAULT_JPEG_QUALITY);
    }
    Ok(quality)
}

fn encode_raw_image(
    raw_bytes: &[u8],
    dimensions: RawImageDimensions,
    encoding: EncodedImageFormat,
    quality: u8,
    row_index: usize,
) -> Result<Vec<u8>, ImageError> {
    let color = color_type_for_encoding(encoding, dimensions.channels)?;
    let mut encoded = Cursor::new(Vec::new());

    match encoding {
        EncodedImageFormat::Jpeg => JpegEncoder::new_with_quality(&mut encoded, quality)
            .write_image(
                raw_bytes,
                u32::try_from(dimensions.width).unwrap_or(u32::MAX),
                u32::try_from(dimensions.height).unwrap_or(u32::MAX),
                color.into(),
            )
            .map_err(|source| ImageError::EncodeImage { row_index, source })?,
        EncodedImageFormat::Png => PngEncoder::new(&mut encoded)
            .write_image(
                raw_bytes,
                u32::try_from(dimensions.width).unwrap_or(u32::MAX),
                u32::try_from(dimensions.height).unwrap_or(u32::MAX),
                color.into(),
            )
            .map_err(|source| ImageError::EncodeImage { row_index, source })?,
    }

    Ok(encoded.into_inner())
}

fn color_type_for_encoding(
    encoding: EncodedImageFormat,
    channels: usize,
) -> Result<ColorType, ImageError> {
    match (encoding, channels) {
        (EncodedImageFormat::Jpeg, 1) | (EncodedImageFormat::Png, 1) => Ok(ColorType::L8),
        (EncodedImageFormat::Png, 2) => Ok(ColorType::La8),
        (EncodedImageFormat::Jpeg, 3) | (EncodedImageFormat::Png, 3) => Ok(ColorType::Rgb8),
        (EncodedImageFormat::Png, 4) => Ok(ColorType::Rgba8),
        (EncodedImageFormat::Jpeg, other) => Err(ImageError::UnsupportedRawChannels {
            channels: other,
            encoding: "jpeg",
        }),
        (EncodedImageFormat::Png, other) => Err(ImageError::UnsupportedRawChannels {
            channels: other,
            encoding: "png",
        }),
    }
}

fn canonicalize_raw_image_schema(
    schema: &Schema,
    dimensions: RawImageDimensions,
    encoding: EncodedImageFormat,
    quality: u8,
) -> Schema {
    let mut field = schema.field(0).clone();
    let mut metadata = field.metadata().clone();
    metadata.insert("image_encoding_kind".to_owned(), "raw".to_owned());
    metadata.insert("image_encoding".to_owned(), encoding.name().to_owned());
    metadata.insert(
        "tensor_shape".to_owned(),
        format!("{}x{}x{}", dimensions.height, dimensions.width, dimensions.channels),
    );
    if encoding == EncodedImageFormat::Jpeg {
        metadata.insert("image_quality".to_owned(), quality.to_string());
    } else {
        metadata.remove("image_quality");
    }
    field.set_metadata(metadata);

    Schema::new_with_metadata(vec![field], schema.metadata.clone())
}

impl EncodedImageFormat {
    fn name(self) -> &'static str {
        match self {
            Self::Jpeg => "jpeg",
            Self::Png => "png",
        }
    }

    fn extension(self) -> &'static str {
        self.name()
    }

    fn as_image_format(self) -> ImageFormat {
        match self {
            Self::Jpeg => ImageFormat::Jpeg,
            Self::Png => ImageFormat::Png,
        }
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
    async fn prepare_image_batch_persists_compressed_assets_and_builds_metadata() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");
        let session_id = SessionId::new();
        let payload = compressed_png_batch();

        let prepared = prepare_image_batch(&storage, &session_id, "camera", "image", &payload)
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
    fn prepare_image_batch_ignores_non_image_payloads() {
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

    #[tokio::test]
    async fn prepare_image_batch_compresses_raw_pixels_with_default_jpeg_settings() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");
        let session_id = SessionId::new();
        let payload = raw_image_batch(None, None);

        let prepared = prepare_image_batch(&storage, &session_id, "camera", "image", &payload)
            .await
            .expect("raw image batch should prepare")
            .expect("payload should be recognized as raw image");

        assert_eq!(
            prepared.normalized_payload_schema.fields[0].metadata["image_encoding"],
            "jpeg"
        );
        assert_eq!(
            prepared.normalized_payload_schema.fields[0].metadata["image_quality"],
            "85"
        );
        let format = prepared
            .metadata_batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("format should be utf8")
            .value(0);
        assert_eq!(format, "jpeg");
    }

    #[tokio::test]
    async fn prepare_image_batch_compresses_raw_pixels_as_png_when_requested() {
        let storage_dir = TempDir::new().expect("storage dir should be created");
        let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");
        let session_id = SessionId::new();
        let payload = raw_image_batch(Some("png"), Some("92"));

        let prepared = prepare_image_batch(&storage, &session_id, "camera", "image", &payload)
            .await
            .expect("raw image batch should prepare")
            .expect("payload should be recognized as raw image");

        assert_eq!(
            prepared.normalized_payload_schema.fields[0].metadata["image_encoding"],
            "png"
        );
        assert!(
            !prepared.normalized_payload_schema.fields[0]
                .metadata
                .contains_key("image_quality")
        );
        let format = prepared
            .metadata_batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("format should be utf8")
            .value(0);
        assert_eq!(format, "png");
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

    fn raw_image_batch(encoding: Option<&str>, quality: Option<&str>) -> RecordBatch {
        let mut metadata = HashMap::from([
            ("image_encoding_kind".to_owned(), "raw".to_owned()),
            ("tensor_shape".to_owned(), "1x2x3".to_owned()),
        ]);
        if let Some(encoding) = encoding {
            metadata.insert("encoding".to_owned(), encoding.to_owned());
        }
        if let Some(quality) = quality {
            metadata.insert("quality".to_owned(), quality.to_owned());
        }

        let mut field = Field::new("image", DataType::LargeBinary, false);
        field.set_metadata(metadata);

        RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(LargeBinaryArray::from(vec![Some(
                [255, 0, 0, 0, 255, 0].as_slice(),
            )])) as ArrayRef],
        )
        .expect("raw image batch should build")
    }
}
