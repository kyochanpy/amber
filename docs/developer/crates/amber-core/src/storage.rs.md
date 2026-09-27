# `crates/amber-core/src/storage.rs`

## 役割

[`crates/amber-core/src/storage.rs`](/Users/sawano/oss/amber/crates/amber-core/src/storage.rs:1) は `object_store` を Amber 用に薄く包み、prefix 付きの JSON/bytes/read/list/delete API とパス規約を提供する。

## `Storage`

- [`Storage`](/Users/sawano/oss/amber/crates/amber-core/src/storage.rs:20)
  - `store: Arc<dyn ObjectStore>`
  - `prefix: ObjectPath`
  を持つ。
  - `object_store()` と `prefix()` は下流モジュールが backend 本体や prefix を参照するときに使う。

## 生成処理

- `new`
  - 既存 object store と prefix をそのまま受ける。
- `from_config`
  - `StorageConfig` を見て backend を構築する。現状 `LocalFileSystem::new_with_prefix` のみ。
- `new_local`
  - テストや node 起動用の直接コンストラクタ。
- `ObjectPath`
  - `object_store::path::Path` の type alias。crate 全体でこの別名を使う。
- `prefix_from_config`
  - `Option<&str>` を object_store の path へ正規化し、不正文字列は `InvalidPrefix` にする内部 helper。

## 基本 I/O

- `put_json` / `get_json`
  - `serde_json` と `put_bytes` / `get_bytes` を組み合わせる。
- `put_bytes`
  - `qualify_path` で prefix を付けてから object store に書く。
- `get_bytes`
  - 読み出し後に `Bytes` を `Vec<u8>` 化する。
- `delete`
  - prefix 付き path を削除する。
- `list_prefix`
  - object store から列挙し、返却 path は `strip_prefix` で prefix を外す。
- `exists`
  - `head` を使って存在確認し、NotFound だけを `false` に変換する。
  - それ以外の head error は `HeadObject` として呼び出し側へ返す。

## path 合成

- `qualify_path`
  - `prefix` と相対 path を結合する。
- `strip_prefix`
  - object store から返る絶対側 path を prefix 相対へ戻す。

## `StorageError`

[`StorageError`](/Users/sawano/oss/amber/crates/amber-core/src/storage.rs:200) は backend 未対応、prefix 不正、各 I/O、JSON serialize/deserialize、prefix mismatch を区別する。

## `paths` モジュール

[`paths`](/Users/sawano/oss/amber/crates/amber-core/src/storage.rs:274) は Amber 内部の保存規約を集中管理する。

- `session_root` / `session_manifest`
  - `sessions/session_id=.../manifest.json`
- `session_asset_root` / `session_asset`
  - セッションに紐づく派生 asset 保存先
- `session_wal_root` / `wal_stream_root` / `wal_segment`
  - WAL の stream 別配置
- `global_parquet_root` / `parquet_root` / `parquet_file`
  - compact 済み Parquet のグローバル配置
- `catalog_events_prefix` / `catalog_event`
  - append-only event log
- `schema_catalog_dir` / `schema_file`
  - schema fingerprint ごとの正規化 schema 保存先
- `escape_component` / `unescape_component`
  - `joint/states` のような `/` を `%2F` 化して path segment として安全に扱う。
  - `unescape_component` は不完全な `%` escape、非 hex、UTF-8 不正を別エラーとして返す。

## 依存関係

catalog、session、writer、compactor、image の全ファイルがこのパス規約に依存する。Amber のディレクトリ構造の実体はこのファイルで決まる。

## テスト

- local backend の JSON/bytes I/O
- prefix 付き list/delete
- config からの prefix 構築
- path helper の escape/配置規約
- 不正 percent encoding の拒否
を確認している。
