# `crates/amber-node/tests/startup.rs`

## 役割

[`crates/amber-node/tests/startup.rs`](/Users/sawano/oss/amber/crates/amber-node/tests/startup.rs:1) は `NodeRuntime` 起動シーケンスの integration test 群である。

## 各テスト

- [`startup_initializes_storage_manifest_writer_and_rotation_runtime`](/Users/sawano/oss/amber/crates/amber-node/tests/startup.rs:13)
  - path 初期化、open manifest 作成、staging root 生成、writer 可用、rotation runtime 可用を一括で確認する。
- [`startup_reads_config_path_from_amber_config_env`](/Users/sawano/oss/amber/crates/amber-node/tests/startup.rs:56)
  - `AMBER_CONFIG` を読んで起動できること、`max_duration_sec: 0` で rotation runtime が無効化されることを確認する。
- [`startup_reports_missing_amber_config_env`](/Users/sawano/oss/amber/crates/amber-node/tests/startup.rs:89)
  - 環境変数未設定時に明示エラーになることを確認する。

## 補助

- `ENV_LOCK`
  - 並列テスト中の環境変数競合を防ぐ。
- `write_config`
  - 一時 YAML の書き出し。
