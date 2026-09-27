# `crates/amber-cli/src/cli.rs`

## 役割

[`crates/amber-cli/src/cli.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/cli.rs:1) は `amber` コマンドの引数仕様を定義する。

## `Cli`

- [`Cli`](/Users/sawano/oss/amber/crates/amber-cli/src/cli.rs:7)
  - `command: Command` だけを持つ最上位 parser。
  - `#[command(name = "amber", version, about = "Amber command-line tools")]` でヘルプ表示とバージョン表記を固定する。

## `Command`

- [`Command`](/Users/sawano/oss/amber/crates/amber-cli/src/cli.rs:13)
  - `Compact(CompactArgs)`
  - `Inspect(InspectArgs)`
  - `List(ListArgs)`
  - 実行側は [`commands::run`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/mod.rs:10) でこの enum に `match` する。

## 各引数型

- [`CompactArgs`](/Users/sawano/oss/amber/crates/amber-cli/src/cli.rs:20)
  - `--config` は既定値 `amber.yaml`。
  - `--cleanup` は compaction 後に compact 済み WAL を物理削除するフラグ。
- [`ListArgs`](/Users/sawano/oss/amber/crates/amber-cli/src/cli.rs:28)
  - `--config` は既定値 `amber.yaml`。
  - `--data-dir` はローカルストレージ時だけ有効。
  - `--latest` は 1 件だけ返す。
  - `--limit` は件数上限。
  - `--tag` は manifest の `tags` フィルタ。
- [`InspectArgs`](/Users/sawano/oss/amber/crates/amber-cli/src/cli.rs:42)
  - 位置引数 `selector` と `--session` は同じ目的で、`--session` には `conflicts_with = "selector"` が付いている。
  - `--config` は既定値 `amber.yaml`。
  - `--output` は `run_inspect` 側で `--node` なしを拒否する。
  - `--rerun` も clap では必須化せず、`run_inspect` 側で未指定を拒否する。
  - `--blueprint` は rerun 起動引数に横流しする。

## 依存関係

このファイルは純粋な宣言層で、各値の意味付けは `compact.rs` / `list.rs` / `inspect.rs` で行う。
