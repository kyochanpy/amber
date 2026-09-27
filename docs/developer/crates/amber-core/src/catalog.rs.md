# `crates/amber-core/src/catalog.rs`

## 役割

[`crates/amber-core/src/catalog.rs`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:1) は append-only catalog event と、その fold 結果としての `CatalogState` を定義する。Amber の物理データ面は実体ファイルだけでなく、この event log から再構成される。

## ID 型

- [`UuidV7Id`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:21)
  - event id、segment id、compaction id、parquet file id の共通実装。
  - `new()` / `parse()` / `as_str()` / `Display` / `FromStr` を持つ。
  - `CatalogEventId`, `WalSegmentId`, `CompactionId`, `ParquetFileId` は型 alias。
- [`UuidV7IdError`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:75)
  - 不正 UUID 文字列と UUIDv7 以外を分けて報告する。

## event 型

- [`CatalogEvent`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:88)
  - `WalSegmentClosed`
  - `CompactionCommitted`
  - `WalSegmentDeleted`
  を持つ。
- `event_id()`, `event_type()`, `path()`
  - 保存 path を `catalog/events/{uuid}-{type}.json` に固定する。
- `save`
  - event を JSON として保存。
- `list`
  - event ファイル群を読み、event id 昇順に並べる。

## 各 event の意味

- [`WalSegmentClosedEvent`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:151)
  - 1 つの WAL segment が publish された事実。session/node/output/schema/path/row_count/byte_size/timestamps/opened/closed を持つ。
- [`CompactionCommittedEvent`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:170)
  - どの source WAL 群からどの Parquet 群を作ったかを表す。
- [`PublishedParquetFile`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:196)
  - compact 結果 1 ファイル分の統計。
  - `new()` は path と作成時刻だけ先に入れ、件数や timestamp 統計は compactor 側が後から埋める。
- [`WalSegmentDeletedEvent`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:238)
  - compact 済み WAL を cleanup で消した事実。

## schema catalog

- [`SchemaCatalogEntry`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:257)
  - `schema_fingerprint` と `NormalizedPayloadSchema` を対応付ける。
  - `path()` は `schema_catalog/{fingerprint}.json` を返す。
  - `save_if_absent` は存在確認のうえで初回だけ保存する。
  - `load` は fingerprint から直接引く。

## `CatalogState`

- [`CatalogState`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:315)
  - `wal_segments: BTreeMap<WalSegmentId, FoldedWalSegment>`
  - `published_parquet_files: BTreeMap<ParquetFileId, PublishedParquetFile>`
- `load`
  - `CatalogEvent::list` の結果を `from_events` へ渡す。
- `from_events`
  - `WalSegmentClosed`: `Pending` 状態で segment を登録
  - `CompactionCommitted`: source WAL を `Compacted` に変更し、Parquet を登録
  - `WalSegmentDeleted`: 対象 WAL を `Deleted` に変更
  という fold を逐次適用する。

## `FoldedWalSegment`

- [`FoldedWalSegment`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:375)
  - close event を fold 後に保持する実用形。
- [`FoldedWalSegmentState`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:416)
  - `Pending`, `Compacted`, `Deleted`

## エラー

- [`CatalogError`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:423)
  - catalog event の保存・列挙・読込
  - compaction event が未知 WAL を参照した場合
  - schema catalog entry の存在確認・保存・読込
  を区別する。

## 依存関係

- writer は close 時に `WalSegmentClosedEvent` を発行する。
- compactor は `CompactionCommittedEvent` と `WalSegmentDeletedEvent` を発行する。
- read_set と CLI `list` は `CatalogState` の fold 結果に依存する。

## テスト

- event path 規約
- UUIDv7 ID の parse/validation
- schema catalog の idempotent 保存
- event list の fold 結果
- 壊れた event JSON のエラー化
を確認している。
