# `crates/amber-node/src/ingest.rs`

## 役割

[`crates/amber-node/src/ingest.rs`](/Users/sawano/oss/amber/crates/amber-node/src/ingest.rs:1) は 1 件の Dora input を Amber の WAL write へ変換する中核ロジックである。

## エラー分類

- [`InputHandlingError`](/Users/sawano/oss/amber/crates/amber-node/src/ingest.rs:18)
  - `Recoverable`
    - 当該 input を捨てれば続行できる失敗。
  - `Fatal`
    - runtime 全体を止めるべき失敗。

## `IngestRuntime`

- [`IngestRuntime`](/Users/sawano/oss/amber/crates/amber-node/src/ingest.rs:33)
  - `storage`
  - `session_manifest`
  - `writer`
  - `rotation_runtime`
  - `selected_inputs`
  - `frame_counters`
  - `stream_schemas`
  を参照で受け取る。

## `handle_input`

- [`handle_input`](/Users/sawano/oss/amber/crates/amber-node/src/ingest.rs:44)
  - `input_id` を文字列化し、`selected_inputs` にないものは無視して `Ok(None)`。
  - `should_record_frame` で sampling を判定し、対象外なら `Ok(None)`。
  - `dora_data_to_record_batch` で payload を Arrow batch 化する。
  - `prepare_image_batch` を呼び、画像 stream なら asset 保存 + metadata batch 化する。
  - schema fingerprint を
    - 画像変換ありなら `PreparedImageBatch.schema_fingerprint`
    - それ以外は `schema_fingerprint_for_payload`
    で決める。
  - `stream_schemas` に既存 fingerprint があり、今回と異なれば fatal error。
  - `SchemaCatalogEntry::save_if_absent` で schema catalog を更新する。
  - WAL に入れる payload を
    - 画像なら `metadata_batch`
    - それ以外は元 payload
    に決める。
  - `current_time_nanos()` で Amber ingest timestamp を取り、`RecordBatchMetadata::new(...)` と `prepend_metadata_columns` で 5 本の metadata 列を前置する。
  - `WalWriteRequest` を作る。
  - rotation runtime があれば `record_write(&request)` で active stream 集合へ登録する。
  - `writer.write(request)` で staged WAL へ追記する。
  - 成功後に `stream_schemas` へ fingerprint を確定させる。
  - sampling で捨てた frame は payload decode も schema catalog 更新も行わない。

## recoverable / fatal の分け方

- `prepare_image_batch` の `ImageError` は `is_recoverable()` を見て分類する。
- schema fingerprint 変化、schema catalog 永続化失敗、writer 不在、WAL write 失敗は fatal。
- payload decode や metadata 付与など行単位の処理失敗は recoverable として扱われる場合がある。
- asset 永続化失敗や永続化後の visibility 確認失敗は storage の整合性に関わるため fatal 側に落ちる。

## 依存関係

- payload 変換: [`dora_arrow.rs`](/Users/sawano/oss/amber/crates/amber-node/src/dora_arrow.rs:1)
- sampling と selected input 解決: [`streams.rs`](/Users/sawano/oss/amber/crates/amber-node/src/streams.rs:1)
- 画像前処理: [`amber-core/src/image.rs`](/Users/sawano/oss/amber/crates/amber-core/src/image.rs:1)
- WAL 永続化: [`amber-core/src/writer.rs`](/Users/sawano/oss/amber/crates/amber-core/src/writer.rs:1)
