# `crates/amber-cli/Cargo.toml`

## 役割

[`crates/amber-cli/Cargo.toml`](/Users/sawano/oss/amber/crates/amber-cli/Cargo.toml:1) は CLI クレートの依存境界を定義する。`amber-core` を直接利用しつつ、`clap` で引数を解釈し、`arrow`/`parquet`/`datafusion`/`rerun` で検査系コマンドを実装する構成になっている。

## 依存の意味

- `amber-core`: セッション、カタログ、ストレージ、WAL、compaction の本体 API を CLI から呼び出す。
- `clap`: [`src/cli.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/cli.rs:1) の `Parser`/`Args`/`Subcommand` 導出に使う。
- `arrow` / `parquet`: [`src/commands/inspect.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:1) で WAL/Parquet を直接読む。
- `datafusion`: 現状コード上では表に出ていないが、将来の問い合わせ機能を見据えた依存として workspace に揃えている。
- `rerun`: `inspect --rerun` で可視化ストリームを起動する。
- `tracing` / `tracing-subscriber`: 現状 CLI 本体では薄いが、ライブラリ側のログ整備に合わせるため入っている。
- `tempfile`(dev): テストの一時ストレージを作る。

## 読み方

この manifest 自体に処理分岐はないが、依存関係から CLI の責務が分かる。実処理は [`src/main.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/main.rs:1) から [`src/commands/mod.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/mod.rs:1) に流れ、そこから `compact` / `list` / `inspect` に分岐する。
