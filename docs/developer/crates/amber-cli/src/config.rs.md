# `crates/amber-cli/src/config.rs`

## 役割

[`crates/amber-cli/src/config.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/config.rs:1) は CLI で設定を読む薄いアダプタで、`amber-core::AmberConfig` と `amber-core::Storage` にユーザー向け文脈を付ける。

## `load_config`

- [`load_config`](/Users/sawano/oss/amber/crates/amber-cli/src/config.rs:6)
  - `AmberConfig::from_file(path)` を呼ぶ。
  - 失敗時は `failed to load amber config from '...'` を付与する。
  - 実際の YAML 解析、既定値補完、相対パス解決は [`amber-core/src/config.rs`](/Users/sawano/oss/amber/crates/amber-core/src/config.rs:1) に依存する。

## `load_storage`

- [`load_storage`](/Users/sawano/oss/amber/crates/amber-cli/src/config.rs:11)
  - まず `load_config` で構成を取得する。
  - `--data-dir` が指定された場合だけ `config.storage.path` を上書きする。
  - ただし `StorageBackend::Local` 以外では `--data-dir` を拒否する。
  - 最後に `Storage::from_config(&config.storage)` で実ストレージを作る。

## 依存関係

- 設定構文の正しさは `amber-core::AmberConfig` に依存。
- ストレージ backend の実装は `amber-core::Storage` に依存。
- `list` と `inspect` はこの関数経由で同じ上書き規則を共有する。

## テスト

- [`list_command_supports_local_data_dir_override`](/Users/sawano/oss/amber/crates/amber-cli/src/config.rs:44)
  - 設定ファイルに書かれたパスではなく `--data-dir` の実ディレクトリを参照することを確認する。
  - `create_manifest` で別ディレクトリに manifest を置き、`run_list` がそれを拾えるかを見る。
