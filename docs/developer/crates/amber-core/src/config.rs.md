# `crates/amber-core/src/config.rs`

## 役割

[`crates/amber-core/src/config.rs`](/Users/sawano/oss/amber/crates/amber-core/src/config.rs:1) は `amber.yaml` の正規形を定義し、既定値補完と backend 妥当性検査までを行う。

## 定数

- `DEFAULT_LOCAL_STORAGE_PATH = "./amber_data"`
- `DEFAULT_WAL_ROTATION_MAX_SIZE_MB = 256`
- `DEFAULT_WAL_ROTATION_MAX_DURATION_SEC = 300`
- `DEFAULT_COMPACTION_TARGET_FILE_MB = 256`

これらは YAML 未指定時の補完値で、`default_storage_backend` / `default_local_storage_path` / `default_wal_rotation_max_size_mb` / `default_wal_rotation_max_duration_sec` / `default_compaction_target_file_mb` と `Default` 実装から参照される。

## 主な設定型

- [`AmberConfig`](/Users/sawano/oss/amber/crates/amber-core/src/config.rs:16)
  - `storage`, `wal`, `compaction`, `nodes` を束ねるトップレベル。
- [`StorageBackend`](/Users/sawano/oss/amber/crates/amber-core/src/config.rs:29)
  - `Local` と `S3` を列挙する。
  - `S3` は設定形状として deserialize できるが、`is_supported()` は `Local` のみ真で、`as_str()` / `Display` はエラーメッセージ用の文字列表現を返す。
- [`StorageConfig`](/Users/sawano/oss/amber/crates/amber-core/src/config.rs:55)
  - path/bucket/prefix/endpoint/access_key/secret_key を持つ。
  - `resolved_local_path()` は未指定時も `./amber_data` を返す。
- [`WalConfig`](/Users/sawano/oss/amber/crates/amber-core/src/config.rs:117)
  - 現状は `rotation` だけ。
- [`WalRotationConfig`](/Users/sawano/oss/amber/crates/amber-core/src/config.rs:124)
  - `max_size_mb` と `max_duration_sec` を持つ。
  - `DEFAULT_MAX_SIZE_MB` / `DEFAULT_MAX_DURATION_SEC` で既定値も公開する。
  - node 実装は現状 duration ベース rotation だけ使い、size ベースは未対応として弾く。
- [`CompactionConfig`](/Users/sawano/oss/amber/crates/amber-core/src/config.rs:147)
  - target file size を持つ。
- [`NodeConfig`](/Users/sawano/oss/amber/crates/amber-core/src/config.rs:162) / [`OutputConfig`](/Users/sawano/oss/amber/crates/amber-core/src/config.rs:170)
  - `amber-node` が記録対象 stream と sampling 間隔を選ぶために使う。

## 主な処理

- `StorageConfig::resolve_path_relative_to`
  - local backend の `path` が相対なら、config ファイルのディレクトリ基準へ直す。`path` 未指定時も既定値 `./amber_data` を config ファイルのディレクトリ配下へ解決する。
- `StorageConfig::ensure_supported`
  - まだ未実装の backend をここで明示的に落とす。
- `AmberConfig::from_file`
  - ファイル読込
  - YAML parse (`AmberFile { amber: AmberConfig }` 形式)
  - storage path の相対解決
  - storage backend サポート判定
  の順で正規化する。
  - `AmberFile` はこのトップレベル `amber:` ラッパを表す内部専用型である。

## エラー

- [`ConfigError`](/Users/sawano/oss/amber/crates/amber-core/src/config.rs:177)
  - `ReadConfigFile`
  - `ParseConfigFile`
  - `UnsupportedStorageBackend`
  で責務を分ける。CLI / node 側はこれに文脈だけ上積みする。

## テスト

- 既定値補完、未知キー拒否、未対応 backend 拒否、相対 path 解決、IO/parse エラーへのパス文脈付与を個別に検証している。
