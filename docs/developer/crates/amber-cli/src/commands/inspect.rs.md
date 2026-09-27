# `crates/amber-cli/src/commands/inspect.rs`

## 役割

[`crates/amber-cli/src/commands/inspect.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:1) は session に紐づく WAL/Parquet を読み、Amber のメタデータ列を使って `rerun` 表示やテスト用の行収集を行う。

## 主なデータ型

- [`InspectSelection`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:28)
  - 対象 session と、必要なら node/output の絞り込みを表す。
- [`InspectRow`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:35)
  - `collect_inspect_rows` が返すテスト向け DTO。entity path、グループ内 row index、timestamp、payload 文字列を持つ。

## `run_inspect`

- [`run_inspect`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:43)
  - MVP では `--rerun` 必須とし、未指定を拒否する。
  - `--output` 単独指定も拒否する。
  - `load_storage` で storage を開く。
  - `resolve_inspect_selection` で session/node/output の実選択を確定する。
  - `SessionSourceSet::resolve` を使って、対象 session の pending WAL と関連 Parquet を統合した読み取り集合を作る。
  - node が指定されていれば `validate_selected_output` で output の曖昧さを排除する。
  - group が 0 件なら inspect 対象なしとして失敗する。
  - `build_rerun_recording` で rerun に接続し、`for_each_inspect_batch` 経由で各 batch を `log_batch_to_rerun` へ流す。
  - 最後に `flush_blocking()` で送信を完了する。

## 選択解決

- [`resolve_inspect_selection`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:95)
  - `--session`、位置引数 `selector`、既定値 `latest` の順で session selector を決める。
- [`latest_session_id`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:118)
  - manifest 一覧を開始時刻降順でソートし、先頭の session id を返す。

## 入力検証

- [`validate_selected_output`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:133)
  - 指定 node が存在しない場合、該当 output がない場合、node に複数 output があるのに `--output` 未指定な場合を個別に弾く。

## rerun 起動

- [`build_rerun_recording`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:181)
  - blueprint ファイルがあれば存在確認後に `--blueprint` 引数を渡す。
  - `RecordingStreamBuilder::new("amber.inspect")` で recording id を `amber-inspect-{session_id}` に固定する。

## 読み取りループ

- [`collect_inspect_rows`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:202)
  - `for_each_inspect_batch` の visitor 版。テストから使う。
- [`for_each_inspect_batch`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:216)
  - group ごとに `amber_row_index` を 0 から開始する。
- [`for_each_group_inspect_batch`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:240)
  - Parquet source は `ParquetRecordBatchReaderBuilder` で読み、各 batch に `filter_batch_to_session` をかける。
  - WAL source は Arrow IPC `StreamReader` で読み、そのまま visitor に渡す。
  - Parquet 側だけ session filter を掛けるのは、Parquet が複数 session の元 WAL を混載しうるからで、WAL は 1 session 専用だから不要。
- [`filter_batch_to_session`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:303)
  - `session_id` 列を `StringArray` として読み、Boolean mask で対象 session の行だけ残す。

## 行の可視化とテキスト化

- [`log_batch_to_rerun`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:319)
  - entity path は `node_id/output_id`。
  - `amber_row`, `node_time`, `amber_time` の 3 軸で時間情報をセットし、payload を `TextLog` で記録する。
- [`append_batch_inspect_rows`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:347)
  - `InspectRow` に変換する visitor 実装。
- [`render_row_text`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:372)
  - metadata 列を飛ばし、payload 列だけを `name=value` へ落とす。
  - payload 列が 0 本なら `"<empty payload>"`。
- [`typed_column`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:398)
  - 名前と downcast をまとめた小ヘルパ。

## 依存関係

- source 解決は [`amber-core/src/read_set.rs`](/Users/sawano/oss/amber/crates/amber-core/src/read_set.rs:1)。
- metadata 列名は [`amber-core/src/schema.rs`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:1)。

## テスト

- [`inspect_rejects_output_without_node`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:419)
  - `--output` 単独指定を拒否する。
- [`inspect_latest_requires_output_when_node_has_multiple_outputs`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:440)
  - node 配下に複数 output がある場合、`--output` 必須であることを確認する。
- [`inspect_filters_parquet_rows_to_selected_session`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:506)
  - 同じ Parquet に別 session 起源の行があっても、session filter が機能することを確認する。
- [`inspect_row_indices_restart_for_each_group`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:628)
  - `amber_row_index` が session 全体通番ではなく group ごとに 0 から打ち直されることを確認する。
