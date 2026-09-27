# `crates/amber-node/src/dora_arrow.rs`

## 役割

[`crates/amber-node/src/dora_arrow.rs`](/Users/sawano/oss/amber/crates/amber-node/src/dora_arrow.rs:1) は Dora Arrow 表現を Amber 側 `arrow` crate の `RecordBatch` へ変換し、timestamp を Unix nanoseconds にそろえる橋渡し層である。

## `dora_data_to_record_batch`

- [`dora_data_to_record_batch`](/Users/sawano/oss/amber/crates/amber-node/src/dora_arrow.rs:10)
  - `ArrowData` から配列を取り出す。
  - 先頭が `StructArray` ならそれをそのまま Dora 側 `RecordBatch` に変換する。
  - ただし top-level struct に null があるケースは ingest 未対応として拒否する。
  - struct でなければ 1 列 `value` の record batch に包む。
  - その後いったん Dora Arrow IPC stream として encode し、Amber 側 `arrow::ipc::reader::StreamReader` で decode し直す。

この encode/decode 迂回は、Dora の Arrow 型と Amber 側 `arrow` crate 型を安全に橋渡しするための実装である。

## timestamp 変換

- [`metadata_timestamp_nanos`](/Users/sawano/oss/amber/crates/amber-node/src/dora_arrow.rs:56)
  - Dora metadata timestamp を `SystemTime` -> Unix nanos にする。
- [`current_time_nanos`](/Users/sawano/oss/amber/crates/amber-node/src/dora_arrow.rs:61)
  - Amber 側 ingest 時刻を現在時刻で取る。
- `system_time_to_nanos`
  - Unix epoch より前や `i64` 範囲超過をエラー化する。

## 依存関係

ingest は payload 変換と timestamp 取得の両方をこのファイルに依存する。
