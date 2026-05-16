use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use amber_core::{NODE_ID_COLUMN, OUTPUT_ID_COLUMN, SESSION_ID_COLUMN, SchemaCatalogEntry};
use amber_node::{app::NodeRuntime, ingest::InputHandlingError};
use arrow::{array::StringArray, ipc::reader::StreamReader};
use dora_node_api::{
    ArrowData, arrow as dora_arrow,
    dora_core::config::{DataId, Input, InputMapping, NodeRunConfig, UserInputMapping},
};
use image::{ColorType, ImageEncoder, codecs::png::PngEncoder};
use tempfile::TempDir;

#[tokio::test]
async fn handle_input_persists_schema_and_writes_metadata_enriched_wal() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let config_path = write_config(
        temp_dir.path(),
        r#"
amber:
  storage:
    backend: local
    path: ./amber_data
  wal:
    rotation:
      max_duration_sec: 0
  nodes:
    - id: camera
      outputs:
        - id: image
"#,
    );
    let mut runtime = NodeRuntime::initialize_from_path(&config_path)
        .await
        .expect("startup should succeed");
    configure_inputs(&mut runtime, &[("camera_image", "camera", "image")]);

    let receipt = runtime
        .handle_input(
            DataId::from("camera_image".to_owned()),
            1_700_000_000_123_456_789,
            structured_arrow_data(),
        )
        .await
        .expect("input handling should succeed")
        .expect("selected input should produce a WAL receipt");

    let schema_fingerprint = runtime
        .stream_schema_fingerprint("camera", "image")
        .expect("stream schema fingerprint should be tracked")
        .to_owned();
    let storage = runtime.storage().clone();

    runtime.shutdown().await.expect("shutdown should succeed");

    let schema_entry = SchemaCatalogEntry::load(&storage, &schema_fingerprint)
        .await
        .expect("schema catalog entry should load");
    assert_eq!(schema_entry.schema_fingerprint, schema_fingerprint);

    let wal_bytes = storage
        .get_bytes(&receipt.path)
        .await
        .expect("published WAL segment should be readable");
    let mut reader = StreamReader::try_new(std::io::Cursor::new(wal_bytes), None)
        .expect("WAL IPC reader should open");
    let batch = reader
        .next()
        .expect("one WAL batch should exist")
        .expect("WAL batch should decode");

    assert_eq!(batch.schema().field(0).name(), SESSION_ID_COLUMN);
    assert_eq!(batch.schema().field(1).name(), NODE_ID_COLUMN);
    assert_eq!(batch.schema().field(2).name(), OUTPUT_ID_COLUMN);
}

#[tokio::test]
async fn handle_input_persists_compressed_images_as_assets_and_writes_metadata_batch() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let config_path = write_config(
        temp_dir.path(),
        r#"
amber:
  storage:
    backend: local
    path: ./amber_data
  wal:
    rotation:
      max_duration_sec: 0
  nodes:
    - id: camera
      outputs:
        - id: image
"#,
    );
    let mut runtime = NodeRuntime::initialize_from_path(&config_path)
        .await
        .expect("startup should succeed");
    configure_inputs(&mut runtime, &[("camera_image", "camera", "image")]);

    let receipt = runtime
        .handle_input(
            DataId::from("camera_image".to_owned()),
            1_700_000_000_123_456_789,
            compressed_image_arrow_data(),
        )
        .await
        .expect("image input handling should succeed")
        .expect("selected image input should produce a WAL receipt");

    let schema_fingerprint = runtime
        .stream_schema_fingerprint("camera", "image")
        .expect("stream schema fingerprint should be tracked")
        .to_owned();
    let storage = runtime.storage().clone();

    runtime.shutdown().await.expect("shutdown should succeed");

    let schema_entry = SchemaCatalogEntry::load(&storage, &schema_fingerprint)
        .await
        .expect("schema entry should load");
    assert_eq!(
        schema_entry.normalized_payload_schema.fields[0].metadata["image_format"],
        "png"
    );

    let wal_bytes = storage
        .get_bytes(&receipt.path)
        .await
        .expect("published WAL segment should be readable");
    let mut reader = StreamReader::try_new(std::io::Cursor::new(wal_bytes), None)
        .expect("WAL IPC reader should open");
    let batch = reader
        .next()
        .expect("one WAL batch should exist")
        .expect("WAL batch should decode");

    assert_eq!(batch.schema().field(5).name(), "width");
    assert_eq!(batch.schema().field(9).name(), "asset_relpath");
    let asset_relpath = batch
        .column_by_name("asset_relpath")
        .expect("asset path column should exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("asset path column should be utf8")
        .value(0)
        .to_owned();
    assert!(
        storage
            .exists(&amber_core::ObjectPath::from(asset_relpath))
            .await
            .expect("image asset should exist")
    );
}

#[tokio::test]
async fn handle_input_compresses_raw_images_with_default_jpeg_settings() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let config_path = write_config(
        temp_dir.path(),
        r#"
amber:
  storage:
    backend: local
    path: ./amber_data
  wal:
    rotation:
      max_duration_sec: 0
  nodes:
    - id: camera
      outputs:
        - id: image
"#,
    );
    let mut runtime = NodeRuntime::initialize_from_path(&config_path)
        .await
        .expect("startup should succeed");
    configure_inputs(&mut runtime, &[("camera_image", "camera", "image")]);

    let receipt = runtime
        .handle_input(
            DataId::from("camera_image".to_owned()),
            1_700_000_000_123_456_789,
            raw_image_arrow_data(None, None),
        )
        .await
        .expect("raw image input handling should succeed")
        .expect("selected image input should produce a WAL receipt");

    let schema_fingerprint = runtime
        .stream_schema_fingerprint("camera", "image")
        .expect("stream schema fingerprint should be tracked")
        .to_owned();
    let storage = runtime.storage().clone();

    runtime.shutdown().await.expect("shutdown should succeed");

    let schema_entry = SchemaCatalogEntry::load(&storage, &schema_fingerprint)
        .await
        .expect("schema entry should load");
    assert_eq!(
        schema_entry.normalized_payload_schema.fields[0].metadata["image_encoding"],
        "jpeg"
    );
    assert_eq!(
        schema_entry.normalized_payload_schema.fields[0].metadata["image_quality"],
        "85"
    );

    let wal_bytes = storage
        .get_bytes(&receipt.path)
        .await
        .expect("published WAL segment should be readable");
    let mut reader = StreamReader::try_new(std::io::Cursor::new(wal_bytes), None)
        .expect("WAL IPC reader should open");
    let batch = reader
        .next()
        .expect("one WAL batch should exist")
        .expect("WAL batch should decode");

    let format = batch
        .column_by_name("format")
        .expect("format column should exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("format should be utf8")
        .value(0);
    assert_eq!(format, "jpeg");
}

#[tokio::test]
async fn handle_input_rejects_same_session_schema_changes() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let config_path = write_config(
        temp_dir.path(),
        r#"
amber:
  storage:
    backend: local
    path: ./amber_data
  wal:
    rotation:
      max_duration_sec: 0
  nodes:
    - id: camera
      outputs:
        - id: image
"#,
    );
    let mut runtime = NodeRuntime::initialize_from_path(&config_path)
        .await
        .expect("startup should succeed");
    configure_inputs(&mut runtime, &[("camera_image", "camera", "image")]);

    runtime
        .handle_input(
            DataId::from("camera_image".to_owned()),
            1_700_000_000_123_456_789,
            structured_arrow_data(),
        )
        .await
        .expect("first input should succeed");

    let error = runtime
        .handle_input(
            DataId::from("camera_image".to_owned()),
            1_700_000_000_123_456_790,
            primitive_arrow_data(),
        )
        .await
        .expect_err("schema change should be rejected");

    match error {
        InputHandlingError::Fatal(error) => {
            assert!(
                error
                    .to_string()
                    .contains("schema fingerprint changed within session")
            );
        }
        InputHandlingError::Recoverable(error) => {
            panic!("expected fatal schema change error, got recoverable: {error}");
        }
    }
}

#[tokio::test]
async fn handle_input_samples_every_nth_frame() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let config_path = write_config(
        temp_dir.path(),
        r#"
amber:
  storage:
    backend: local
    path: ./amber_data
  wal:
    rotation:
      max_duration_sec: 0
  nodes:
    - id: camera
      outputs:
        - id: image
          every_n_frames: 5
"#,
    );
    let mut runtime = NodeRuntime::initialize_from_path(&config_path)
        .await
        .expect("startup should succeed");
    configure_inputs(&mut runtime, &[("camera_image", "camera", "image")]);

    for node_timestamp in 1..5 {
        let receipt = runtime
            .handle_input(
                DataId::from("camera_image".to_owned()),
                node_timestamp,
                structured_arrow_data(),
            )
            .await
            .expect("sampled input handling should succeed");
        assert!(receipt.is_none(), "only the fifth frame should be recorded");
    }

    let receipt = runtime
        .handle_input(
            DataId::from("camera_image".to_owned()),
            5,
            structured_arrow_data(),
        )
        .await
        .expect("fifth frame handling should succeed");
    assert!(receipt.is_some(), "the fifth frame should be recorded");
}

#[tokio::test]
async fn handle_input_counts_sampling_independently_per_node() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let config_path = write_config(
        temp_dir.path(),
        r#"
amber:
  storage:
    backend: local
    path: ./amber_data
  wal:
    rotation:
      max_duration_sec: 0
  nodes:
    - id: camera-a
      outputs:
        - id: image
          every_n_frames: 2
    - id: camera-b
      outputs:
        - id: image
          every_n_frames: 2
"#,
    );
    let mut runtime = NodeRuntime::initialize_from_path(&config_path)
        .await
        .expect("startup should succeed");
    configure_inputs(
        &mut runtime,
        &[
            ("camera_a", "camera-a", "image"),
            ("camera_b", "camera-b", "image"),
        ],
    );

    assert!(
        runtime
            .handle_input(
                DataId::from("camera_a".to_owned()),
                1,
                structured_arrow_data()
            )
            .await
            .expect("first node-a frame should succeed")
            .is_none()
    );
    assert!(
        runtime
            .handle_input(
                DataId::from("camera_b".to_owned()),
                1,
                structured_arrow_data()
            )
            .await
            .expect("first node-b frame should succeed")
            .is_none()
    );
    assert!(
        runtime
            .handle_input(
                DataId::from("camera_a".to_owned()),
                2,
                structured_arrow_data()
            )
            .await
            .expect("second node-a frame should succeed")
            .is_some()
    );
    assert!(
        runtime
            .handle_input(
                DataId::from("camera_b".to_owned()),
                2,
                structured_arrow_data()
            )
            .await
            .expect("second node-b frame should succeed")
            .is_some()
    );
}

#[tokio::test]
async fn handle_input_counts_sampling_independently_per_output() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let config_path = write_config(
        temp_dir.path(),
        r#"
amber:
  storage:
    backend: local
    path: ./amber_data
  wal:
    rotation:
      max_duration_sec: 0
  nodes:
    - id: camera
      outputs:
        - id: image
          every_n_frames: 5
        - id: depth
          every_n_frames: 3
"#,
    );
    let mut runtime = NodeRuntime::initialize_from_path(&config_path)
        .await
        .expect("startup should succeed");
    configure_inputs(
        &mut runtime,
        &[
            ("camera_image", "camera", "image"),
            ("camera_depth", "camera", "depth"),
        ],
    );

    for node_timestamp in 1..3 {
        assert!(
            runtime
                .handle_input(
                    DataId::from("camera_image".to_owned()),
                    node_timestamp,
                    structured_arrow_data(),
                )
                .await
                .expect("sampled image frame should succeed")
                .is_none()
        );
        assert!(
            runtime
                .handle_input(
                    DataId::from("camera_depth".to_owned()),
                    node_timestamp,
                    structured_arrow_data(),
                )
                .await
                .expect("sampled depth frame should succeed")
                .is_none()
        );
    }

    assert!(
        runtime
            .handle_input(
                DataId::from("camera_depth".to_owned()),
                3,
                structured_arrow_data()
            )
            .await
            .expect("third depth frame should succeed")
            .is_some()
    );
    assert!(
        runtime
            .handle_input(
                DataId::from("camera_image".to_owned()),
                3,
                structured_arrow_data()
            )
            .await
            .expect("third image frame should succeed")
            .is_none()
    );
    assert!(
        runtime
            .handle_input(
                DataId::from("camera_image".to_owned()),
                4,
                structured_arrow_data()
            )
            .await
            .expect("fourth image frame should succeed")
            .is_none()
    );
    assert!(
        runtime
            .handle_input(
                DataId::from("camera_image".to_owned()),
                5,
                structured_arrow_data()
            )
            .await
            .expect("fifth image frame should succeed")
            .is_some()
    );
}

#[tokio::test]
async fn handle_input_without_sampling_records_every_frame() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let config_path = write_config(
        temp_dir.path(),
        r#"
amber:
  storage:
    backend: local
    path: ./amber_data
  wal:
    rotation:
      max_duration_sec: 0
  nodes:
    - id: camera
      outputs:
        - id: image
"#,
    );
    let mut runtime = NodeRuntime::initialize_from_path(&config_path)
        .await
        .expect("startup should succeed");
    configure_inputs(&mut runtime, &[("camera_image", "camera", "image")]);

    for node_timestamp in 1..=3 {
        let receipt = runtime
            .handle_input(
                DataId::from("camera_image".to_owned()),
                node_timestamp,
                structured_arrow_data(),
            )
            .await
            .expect("unsampled input handling should succeed");
        assert!(
            receipt.is_some(),
            "all frames should be recorded without sampling"
        );
    }
}

fn configure_inputs(runtime: &mut NodeRuntime, mappings: &[(&str, &str, &str)]) {
    let inputs = mappings
        .iter()
        .map(|(input_id, source, output)| {
            (
                DataId::from((*input_id).to_owned()),
                Input {
                    mapping: InputMapping::User(UserInputMapping {
                        source: (*source).to_owned().into(),
                        output: (*output).to_owned().into(),
                    }),
                    queue_size: None,
                },
            )
        })
        .collect();

    runtime.configure_inputs(&NodeRunConfig {
        inputs,
        outputs: BTreeSet::new(),
    });
}

fn write_config(root: &Path, contents: &str) -> PathBuf {
    let path = root.join("amber.yaml");
    fs::write(&path, contents).expect("config file should be written");
    path
}

fn structured_arrow_data() -> ArrowData {
    let batch = dora_arrow::record_batch::RecordBatch::try_new(
        Arc::new(dora_arrow::datatypes::Schema::new(vec![
            dora_arrow::datatypes::Field::new(
                "value",
                dora_arrow::datatypes::DataType::Int32,
                false,
            ),
            dora_arrow::datatypes::Field::new("label", dora_arrow::datatypes::DataType::Utf8, true),
        ])),
        vec![
            Arc::new(dora_arrow::array::Int32Array::from(vec![1, 2])),
            Arc::new(dora_arrow::array::StringArray::from(vec![
                Some("front"),
                Some("rear"),
            ])),
        ],
    )
    .expect("payload batch should build");

    ArrowData(Arc::new(dora_arrow::array::StructArray::from(batch)))
}

fn primitive_arrow_data() -> ArrowData {
    ArrowData(Arc::new(dora_arrow::array::Int32Array::from(vec![1, 2, 3])))
}

fn compressed_image_arrow_data() -> ArrowData {
    let mut image_field = dora_arrow::datatypes::Field::new(
        "image",
        dora_arrow::datatypes::DataType::LargeBinary,
        false,
    );
    image_field.set_metadata(std::collections::HashMap::from([(
        "image_format".to_owned(),
        "png".to_owned(),
    )]));

    let batch = dora_arrow::record_batch::RecordBatch::try_new(
        Arc::new(dora_arrow::datatypes::Schema::new(vec![image_field])),
        vec![Arc::new(dora_arrow::array::LargeBinaryArray::from(vec![
            Some(tiny_png_bytes().as_slice()),
        ]))],
    )
    .expect("compressed image payload should build");

    ArrowData(Arc::new(dora_arrow::array::StructArray::from(batch)))
}

fn raw_image_arrow_data(encoding: Option<&str>, quality: Option<&str>) -> ArrowData {
    let mut metadata = std::collections::HashMap::from([
        ("image_encoding_kind".to_owned(), "raw".to_owned()),
        ("tensor_shape".to_owned(), "1x2x3".to_owned()),
    ]);
    if let Some(encoding) = encoding {
        metadata.insert("encoding".to_owned(), encoding.to_owned());
    }
    if let Some(quality) = quality {
        metadata.insert("quality".to_owned(), quality.to_owned());
    }

    let mut image_field = dora_arrow::datatypes::Field::new(
        "image",
        dora_arrow::datatypes::DataType::LargeBinary,
        false,
    );
    image_field.set_metadata(metadata);

    let batch = dora_arrow::record_batch::RecordBatch::try_new(
        Arc::new(dora_arrow::datatypes::Schema::new(vec![image_field])),
        vec![Arc::new(dora_arrow::array::LargeBinaryArray::from(vec![
            Some([255, 0, 0, 0, 255, 0].as_slice()),
        ]))],
    )
    .expect("raw image payload should build");

    ArrowData(Arc::new(dora_arrow::array::StructArray::from(batch)))
}

fn tiny_png_bytes() -> Vec<u8> {
    let mut encoded = std::io::Cursor::new(Vec::new());
    PngEncoder::new(&mut encoded)
        .write_image(&[255, 0, 0], 1, 1, ColorType::Rgb8.into())
        .expect("PNG encoding should succeed");
    encoded.into_inner()
}
