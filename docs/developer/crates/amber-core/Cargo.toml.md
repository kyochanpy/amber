# `crates/amber-core/Cargo.toml`

## 役割

[`crates/amber-core/Cargo.toml`](/Users/sawano/oss/amber/crates/amber-core/Cargo.toml:1) は Amber の保存・整形・圧縮・設定解釈の中心ライブラリ依存を定義する。

## 依存の意味

- `arrow` / `parquet`: WAL は Arrow IPC、長期保存は Parquet で扱う。
- `object_store`: storage backend 抽象化に使う。現状 backend は local のみ有効。
- `image`: 画像 payload の検出、再エンコード、メタデータ化に使う。
- `serde` / `serde_json` / `serde_yaml`: config、catalog event、manifest、schema catalog を読む。
- `uuid`: session、catalog event、segment、parquet file の UUIDv7 生成に使う。
- `tokio` / `futures`: 非同期 storage と background writer に使う。

## 位置付け

`amber-node` と `amber-cli` の両方がこのクレートに依存する。したがってこの manifest は実装の共通境界でもある。
