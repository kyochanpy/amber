mod support;

use amber_cli::{
    cli::CompactArgs,
    commands::{compact::run_compact, inspect::collect_inspect_rows},
};
use amber_core::{
    AMBER_TIMESTAMP_COLUMN, AmberConfig, CatalogEvent, NODE_ID_COLUMN, NODE_TIMESTAMP_COLUMN,
    OUTPUT_ID_COLUMN, ObjectPath, SESSION_ID_COLUMN, SessionManifest, SessionSourceFilter,
    SessionSourceSet, Storage, WalWriteRequest, WalWriter,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use bytes::Bytes;
use tempfile::TempDir;

use self::support::{metadata_enriched_batch_for_stream, write_config};

#[tokio::test]
async fn mvp_happy_path_flows_from_wal_to_compaction_to_inspect_rows() {
    let storage_dir = TempDir::new().expect("storage dir should exist");
    let staging_dir = TempDir::new().expect("staging dir should exist");
    let config_path = write_config(storage_dir.path()).expect("config should be written");
    let storage = Storage::new_local(storage_dir.path(), None::<&str>).expect("storage");

    let session_id = amber_core::SessionId::new();
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
            metadata_enriched_batch_for_stream(
                session_id.as_str(),
                "camera",
                "image",
                vec![1],
                vec![Some("first")],
                vec![100],
                vec![110],
            ),
        ))
        .await
        .expect("first WAL write should succeed");
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
            session_id.clone(),
            "camera",
            "image",
            "schema-v1",
            metadata_enriched_batch_for_stream(
                session_id.as_str(),
                "camera",
                "image",
                vec![2],
                vec![Some("second")],
                vec![200],
                vec![210],
            ),
        ))
        .await
        .expect("second WAL write should succeed");
    writer
        .shutdown()
        .await
        .expect("writer shutdown should publish remaining WAL");

    let manifest = SessionManifest::load(&storage, &session_id)
        .await
        .expect("session manifest should load after writer shutdown");
    assert_eq!(manifest.observed_streams.len(), 1);

    let summary = run_compact(&CompactArgs {
        config: config_path,
        cleanup: false,
    })
    .await
    .expect("compact command should succeed");
    assert_eq!(summary.compacted_segments, 2);
    assert_eq!(summary.created_parquet_files, 1);
    assert_eq!(summary.deleted_segments, 0);

    let catalog_events = CatalogEvent::list(&storage)
        .await
        .expect("catalog events should load");
    assert_eq!(
        catalog_events
            .iter()
            .filter(|event| matches!(event, CatalogEvent::WalSegmentClosed(_)))
            .count(),
        2
    );
    assert_eq!(
        catalog_events
            .iter()
            .filter(|event| matches!(event, CatalogEvent::CompactionCommitted(_)))
            .count(),
        1
    );
    assert_eq!(
        storage
            .list_prefix(&ObjectPath::from("catalog/events"))
            .await
            .expect("catalog event objects should list")
            .len(),
        3
    );

    let source_set = SessionSourceSet::resolve(&storage, &session_id, SessionSourceFilter::default())
        .await
        .expect("source set should resolve");
    assert_eq!(source_set.groups.len(), 1);
    assert!(source_set.groups[0].wal_sources.is_empty());
    assert_eq!(source_set.groups[0].parquet_sources.len(), 1);

    let parquet_path = source_set.groups[0].parquet_sources[0].path.clone();
    let bytes = storage
        .get_bytes(&parquet_path)
        .await
        .expect("parquet bytes should be readable");
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .expect("parquet builder should open")
        .build()
        .expect("parquet reader should build");
    let first_batch = reader
        .next()
        .expect("parquet should contain one batch")
        .expect("parquet batch should decode");
    for column_name in [
        SESSION_ID_COLUMN,
        NODE_ID_COLUMN,
        OUTPUT_ID_COLUMN,
        NODE_TIMESTAMP_COLUMN,
        AMBER_TIMESTAMP_COLUMN,
    ] {
        assert!(
            first_batch.column_by_name(column_name).is_some(),
            "parquet batch should include metadata column '{column_name}'"
        );
    }

    let rows = collect_inspect_rows(&storage, &session_id, &source_set)
        .await
        .expect("inspect rows should collect");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .map(|row| row.entity_path.as_str())
            .collect::<Vec<_>>(),
        vec!["camera/image", "camera/image"]
    );
    assert_eq!(
        rows.iter().map(|row| row.amber_row_index).collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        rows.iter().map(|row| row.text.as_str()).collect::<Vec<_>>(),
        vec!["value=1, label=first", "value=2, label=second"]
    );
}
