# `crates/amber-cli/src/main.rs`

## 役割

[`crates/amber-cli/src/main.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/main.rs:1) は CLI バイナリの最小エントリポイントである。

## 処理

- [`main`](/Users/sawano/oss/amber/crates/amber-cli/src/main.rs:6)
- `#[tokio::main]`
  - 非同期コマンドをそのまま呼べるように Tokio ランタイムを立ち上げる。
- `Cli::parse()`
  - [`src/cli.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/cli.rs:1) の `clap::Parser` 実装で引数をデコードする。
- `run(cli).await`
  - 実際の処理分岐は [`src/commands/mod.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/mod.rs:1) に渡す。

## 依存関係

このファイルはエラー加工を一切せず、戻り値 `anyhow::Result<()>` をそのまま返す。終了コード化やエラー表示は `main` を呼ぶ Rust 実行環境に委ね、CLI の振る舞い自体は全て下流モジュールに依存する。
