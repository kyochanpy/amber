# `crates/amber-cli/src/commands/mod.rs`

## 役割

[`crates/amber-cli/src/commands/mod.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/mod.rs:1) はサブコマンド実行のルータである。

## モジュール公開

- `pub mod compact;`
- `pub mod inspect;`
- `pub mod list;`

## `run`

- [`run`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/mod.rs:10)
  - `Cli.command` を `match` する。
  - `Compact`
    - [`compact::run_compact`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/compact.rs:14) を呼ぶ。
    - 返ってきた `CompactSummary` に応じて、人間向けメッセージを分岐して出す。
    - 新規 compaction がなくても `--cleanup` で既存 compacted WAL を削除できた場合は、その削除件数だけを表示する。
    - `args.cleanup` かつ compaction が発生した場合は cleanup 結果も別途表示する。
  - `Inspect`
    - [`inspect::run_inspect`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:43) に丸投げする。
  - `List`
    - [`list::run_list`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/list.rs:24) で情報を集め、[`render_session_list`](/Users/sawano/oss/amber/crates/amber-cli/src/output.rs:6) で整形して表示する。

## 依存関係

このファイルは CLI 表示文言を持つが、ストレージや WAL の処理自体は各サブモジュールに依存している。
