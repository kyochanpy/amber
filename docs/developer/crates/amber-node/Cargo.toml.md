# `crates/amber-node/Cargo.toml`

## 役割

[`crates/amber-node/Cargo.toml`](/Users/sawano/oss/amber/crates/amber-node/Cargo.toml:1) は Dora node として動く Amber 取り込みプロセスの依存を定義する。

## 依存の意味

- `amber-core`: session/storage/schema/image/writer を使う。
- `dora-node-api`: Dora runtime と event/input 表現を受け取る。
- `arrow`: Dora Arrow を Amber Arrow へ橋渡しした後の扱いに使う。
- `tracing` / `tracing-subscriber`: 起動、入力処理、停止時のログ出力。
- `image`(dev): 画像入力の integration test で PNG を作る。

## 位置付け

この crate は「Dora の input event を Amber の WAL と catalog に変える実行時アダプタ」であり、永続化の本体は `amber-core` に依存する。
