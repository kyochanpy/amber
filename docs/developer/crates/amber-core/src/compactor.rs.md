# `crates/amber-core/src/compactor.rs`

## 役割

[`crates/amber-core/src/compactor.rs`](/Users/sawano/oss/amber/crates/amber-core/src/compactor.rs:1) は pending WAL segment を node/output/schema ごとにまとめて Parquet 化し、catalog に `CompactionCommittedEvent` を積む。さらに compact 済み WAL の cleanup も担当する。

## `Compactor`

- [`Compactor`](/Users/sawano/oss/amber/crates/amber-core/src/compactor.rs:22)
  - `storage`
  - `target_file_mb`
  を持つ。
- `BYTES_PER_MB`
  - `target_file_mb` を byte 換算するときの定数。

## `compact_pending`

- [`compact_pending`](/Users/sawano/oss/amber/crates/amber-core/src/compactor.rs:35)
  - `CatalogState::load` で folded catalog を取得する。
  - `Pending` 状態の WAL segment だけを拾う。
  - 各 segment を `load_segment` で実 bytes まで読み込む。
  - `(node_id, output_id, schema_fingerprint)` ごとに group 化する。
  - `partition_segments` で target size を超えない単位に区切る。
  - 各 part を `write_part` で Parquet にし、source segment id と created parquet を集める。
  - 最後に `CompactionCommittedEvent` を 1 件だけ保存して返す。
  - pending が 0 件なら `Ok(None)`。

## `cleanup_compacted`

- [`cleanup_compacted`](/Users/sawano/oss/amber/crates/amber-core/src/compactor.rs:87)
  - folded catalog から `Compacted` 状態の segment を拾う。
  - object store から物理削除する。
  - 成功したものだけ `WalSegmentDeletedEvent` を保存する。
  - 物理削除が失敗したら event は積まずにエラーを返す。

## 読込と group 化

- `load_segment`
  - published WAL bytes を Arrow IPC `StreamReader` で読む。
  - 1 segment 内で schema が変わっていたら `SchemaMismatch`。
  - batches と schema を `LoadedWalSegment` に詰める。
- `CompactionGroupKey`
  - group 単位を `(node_id, output_id, schema_fingerprint)` に固定する。

## partition と part

- `partition_segments`
  - segment id 昇順に並べる。
  - 入力側 byte_size 合計が `target_file_mb * 1024 * 1024` を超える前に part を切る。
  - `target_file_mb = 0` なら各 segment が別 part になりやすく、テストで使われる。
- `build_part`
  - part 内 segment 群の timestamp min/max を集約する。
- `CompactionPart`
  - 1 回の Parquet 出力単位。source segment 群と集約済み統計を保持する。
- `CompactionPart::batches`
  - segment ごとの record batch をフラット化して Parquet writer に渡す。

## Parquet 出力

- `write_part`
  - path を `parquet/node_id=.../output_id=.../schema_fingerprint=.../part-{uuid}.parquet` で発番。
  - `write_parquet_bytes` でメモリ上に Parquet を組む。
  - storage へ書いてから再読込し、`validate_parquet_bytes` で metadata 列と row_count を検証する。
  - その結果で `PublishedParquetFile` を返す。
- `write_parquet_bytes`
  - `ArrowWriter` へ batch を順に流し、`close()` して bytes を返す。
- `validate_parquet_bytes`
  - metadata 列が 5 本揃っているか確認する。
  - reader を最後まで回して row_count を取り直す。

## エラー

[`CompactorError`](/Users/sawano/oss/amber/crates/amber-core/src/compactor.rs:400) は catalog load、WAL read/decode、schema 不整合、Parquet write/finalize/validate、WAL delete、catalog event 保存失敗を分離する。

## 依存関係

- source 判定は [`catalog.rs`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:1)。
- metadata 列名は [`schema.rs`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:1)。
- cleanup 後の論理状態は read_set/list が catalog を再 fold して見る。

## テスト

- grouped Parquet 出力と単一 compaction event
- pending なし時の `None`
- compacted だけ削除し pending は残す cleanup
- 物理削除失敗時に delete event を積まないこと
を検証している。
