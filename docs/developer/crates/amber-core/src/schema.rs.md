# `crates/amber-core/src/schema.rs`

## 役割

[`crates/amber-core/src/schema.rs`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:1) は Amber 標準メタデータ列、payload schema の抽出、schema fingerprint の正規化規則を定義する。WAL/Parquet/inspect/ingest の整合性はこのファイルに依存する。

## メタデータ列

- [`SESSION_ID_COLUMN`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:9)
- [`NODE_ID_COLUMN`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:10)
- [`OUTPUT_ID_COLUMN`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:11)
- [`NODE_TIMESTAMP_COLUMN`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:12)
- [`AMBER_TIMESTAMP_COLUMN`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:13)
- [`METADATA_COLUMNS`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:15)

[`metadata_fields`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:37) と [`metadata_schema`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:48) がこの順で field/schema を作る。`metadata_field_names()` は静的スライス参照を返し、`is_metadata_column` は名前判定用である。

## payload 抽出

- [`payload_fields`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:60)
  - metadata 列を除いた field 群を返す。
- [`payload_schema`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:69)
  - metadata 列を除いたうえで、schema metadata は `filter_semantic_metadata` で意味論的キーだけ残す。

## fingerprint

- [`schema_fingerprint`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:78)
  - metadata 前置き済み schema から payload だけを抜いて fingerprint を作る。
- [`schema_fingerprint_for_payload`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:82)
  - `normalized_payload_schema` を JSON 化し、`fnv1a128_hex` で 128bit hex にする。

## metadata 前置

- [`RecordBatchMetadata`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:89)
  - 1 batch 分の共通 session/node/output と、行単位 timestamp 配列を持つ。
- [`prepend_metadata_columns`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:136)
  - payload に reserved 名が混ざっていないか確認する。
  - timestamp 配列長が `num_rows()` と一致するか確認する。
  - 5 本の metadata array を前に作り、元 payload columns を後ろに連結する。
  - schema metadata は payload 側をそのまま保持する。

## 正規化

- [`normalized_payload_schema`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:198)
  - field 再帰と metadata フィルタを通じて、安定比較可能な JSON 形へ落とす。
- `normalize_field`, `normalize_data_type`
  - Arrow の全データ型を `NormalizedDataType` に写像する。
  - struct/list/union/dictionary/decimal/map/run_end_encoded まで個別処理する。
- `normalize_fields` / `normalize_union_fields` / `normalize_time_unit`
  - `NormalizedDataType` の子要素や time unit パラメータを安定表現へ落とす補助関数。
- `leaf_type` / `with_param` / `with_child` / `map_from_pairs`
  - 正規化済み型ノードを組み立てる小ヘルパ。
- `is_semantic_metadata_key`
  - allowlist 判定の 1 点集約。
- `validate_metadata_column_len`
  - `prepend_metadata_columns` で行数不一致を弾く。
- `SEMANTIC_METADATA_KEYS`
  - fingerprint に影響させる metadata key の allowlist。runtime 固有の雑多な metadata は落とす。

## エラー

- [`MetadataColumnsError`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:116)
  - reserved metadata 列名の衝突
  - metadata 配列長と payload 行数の不一致
  - metadata 前置後の `RecordBatch` 構築失敗
  を分けている。

## `Normalized*` 型

- [`NormalizedPayloadSchema`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:457)
- [`NormalizedField`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:463)
- [`NormalizedDataType`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:471)

これが schema catalog の保存形式になる。

## 依存関係

- ingest は fingerprint と metadata 前置に依存。
- writer/compactor/inspect は metadata 列名に依存。
- image は raw image schema を canonicalize した後、この正規化を使う。

## テスト

- metadata 列順
- payload 抽出
- runtime metadata を無視した fingerprint
- semantic metadata 差分を拾う fingerprint
- nested struct / union type id の差分検出
- metadata 前置の正常系と異常系
を網羅している。
