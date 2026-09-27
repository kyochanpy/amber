# `crates/amber-cli/src/commands/compact.rs`

## 役割

[`crates/amber-cli/src/commands/compact.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/compact.rs:1) は `amber compact` の本体で、catalog 上の pending WAL を Parquet にまとめ、必要なら compact 済み WAL を削除する。

## `CompactSummary`

- [`CompactSummary`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/compact.rs:8)
  - `created_parquet_files`
  - `compacted_segments`
  - `deleted_segments`
  を保持し、呼び出し元が文言を組み立てられるようにする。

## `run_compact`

- [`run_compact`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/compact.rs:14)
  - `load_config` で設定を読む。
  - `Storage::from_config` で storage を初期化する。
  - `Compactor::new(storage, config.compaction.target_file_mb)` を作る。
  - `compact_pending()` を呼び、Parquet 生成の有無を受け取る。
  - `args.cleanup` が真なら `cleanup_compacted()` を呼び、削除件数を数える。
  - `CompactionCommittedEvent` の中身から生成ファイル数と source WAL 数をサマリ化して返す。

## 依存関係

- compaction 実装本体は [`amber-core/src/compactor.rs`](/Users/sawano/oss/amber/crates/amber-core/src/compactor.rs:1)。
- catalog 状態の反映は `amber-core` 側に全面依存。

## テスト

- [`compact_command_compacts_closed_segments_without_cleanup`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/compact.rs:64)
  - rotate 済み segment だけが compact され、WAL オブジェクト自体は残ることを確認する。
- [`compact_command_can_cleanup_compacted_segments`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/compact.rs:164)
  - `--cleanup` 時に compact 済み WAL が物理削除され、`WalSegmentDeleted` イベントが残ることを確認する。
