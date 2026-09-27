# `crates/amber-cli/src/lib.rs`

## 役割

[`crates/amber-cli/src/lib.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/lib.rs:1) は CLI クレートの公開モジュール面だけを束ねる。

## 公開境界

- `pub mod cli;`
  - 引数構造体とサブコマンド enum を公開する。
- `pub mod commands;`
  - 実コマンド本体を公開する。
- `pub mod config;`
  - CLI 用の設定ロードと `--data-dir` 上書きを公開する。
- `pub mod output;`
  - `list` 出力の整形ロジックを公開する。
- `#[cfg(test)] pub(crate) mod test_support;`
  - テスト専用ユーティリティ。公開 API に含めない。

## 依存関係

このファイル自体は制御を持たない。`main.rs` はここから `cli::Cli` と `commands::run` を読み込むだけで、以降の処理は各モジュールに委譲される。
