# `crates/amber-node/tests/ingest.rs`

## 役割

[`crates/amber-node/tests/ingest.rs`](/Users/sawano/oss/amber/crates/amber-node/tests/ingest.rs:1) は ingest パイプライン全体の integration test 群で、schema catalog、画像前処理、sampling、schema 変化検知を確認する。

## 各テスト

- [`handle_input_persists_schema_and_writes_metadata_enriched_wal`](/Users/sawano/oss/amber/crates/amber-node/tests/ingest.rs:19)
  - 通常 payload を ingest し、schema catalog 保存と WAL 先頭 3 列が `session_id/node_id/output_id` になることを確認する。
- [`handle_input_persists_compressed_images_as_assets_and_writes_metadata_batch`](/Users/sawano/oss/amber/crates/amber-node/tests/ingest.rs:82)
  - 圧縮 PNG が asset 化され、WAL 側には `width...asset_relpath` 列群が書かれることを確認する。
- [`handle_input_compresses_raw_images_with_default_jpeg_settings`](/Users/sawano/oss/amber/crates/amber-node/tests/ingest.rs:161)
  - raw image が既定で JPEG へ正規化され、schema catalog metadata に `image_encoding=jpeg`, `image_quality=85` が入ることを確認する。
- [`handle_input_rejects_same_session_schema_changes`](/Users/sawano/oss/amber/crates/amber-node/tests/ingest.rs:236)
  - 同一 stream に別 schema を流すと fatal error になることを確認する。
- [`handle_input_samples_every_nth_frame`](/Users/sawano/oss/amber/crates/amber-node/tests/ingest.rs:292)
  - `every_n_frames: 5` で 5 フレーム目だけ記録されることを確認する。
- [`handle_input_counts_sampling_independently_per_node`](/Users/sawano/oss/amber/crates/amber-node/tests/ingest.rs:340)
  - node ごとに sampling counter が独立していることを確認する。
- [`handle_input_counts_sampling_independently_per_output`](/Users/sawano/oss/amber/crates/amber-node/tests/ingest.rs:421)
  - 同一 node でも output ごとに counter が独立していることを確認する。
- [`handle_input_without_sampling_records_every_frame`](/Users/sawano/oss/amber/crates/amber-node/tests/ingest.rs:525)
  - sampling 未設定なら毎フレーム記録されることを確認する。

## 補助関数

- `configure_inputs`
  - Dora `NodeRunConfig` を組み立てて runtime に投入する。
- `structured_arrow_data`
  - 通常の struct payload。
- `primitive_arrow_data`
  - schema 変化検証用の単列 primitive payload。
- `compressed_image_arrow_data`
  - metadata 付き compressed PNG payload。
- `raw_image_arrow_data`
  - raw image metadata を持つ payload。
- `tiny_png_bytes`
  - 圧縮画像 fixture を生成する。
