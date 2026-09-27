# `crates/amber-node/src/main.rs`

## 役割

[`crates/amber-node/src/main.rs`](/Users/sawano/oss/amber/crates/amber-node/src/main.rs:1) は `amber-node` バイナリの起動点で、tracing 初期化と top-level error 処理だけを担当する。

## 処理

- [`main`](/Users/sawano/oss/amber/crates/amber-node/src/main.rs:5)
- `init_tracing()`
  - `tracing_subscriber::fmt()` を `Level::INFO` / target なしで初期化する。
  - `try_init()` を使うので、既に初期化済みでも panic しない。
- `run().await`
  - 実際の node runtime は [`src/app.rs`](/Users/sawano/oss/amber/crates/amber-node/src/app.rs:1)。
- エラー時
  - tracing に `amber-node startup failed` という固定メッセージを出す。
  - `eprintln!("{error:#}")` で人間向け整形を stderr へ出す。
  - 終了コード 1 でプロセスを落とす。
