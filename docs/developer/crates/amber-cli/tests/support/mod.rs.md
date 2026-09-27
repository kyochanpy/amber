# `crates/amber-cli/tests/support/mod.rs`

## 役割

[`crates/amber-cli/tests/support/mod.rs`](/Users/sawano/oss/amber/crates/amber-cli/tests/support/mod.rs:1) は integration test 側の fixture を提供する。中身は `src/test_support.rs` とほぼ同じだが、別クレート境界のため再定義している。

## 各関数

- [`write_config`](/Users/sawano/oss/amber/crates/amber-cli/tests/support/mod.rs:14)
  - 一時 storage root を指す `amber.yaml` を書く。
- [`metadata_enriched_batch_for_stream`](/Users/sawano/oss/amber/crates/amber-cli/tests/support/mod.rs:27)
  - 任意 session/node/output 用の payload batch を作り、`prepend_metadata_columns` で Amber 標準メタデータ列を付ける。

## 依存関係

integration test は `amber_cli` を外部クレートとして扱うため、`pub(crate)` な `src/test_support.rs` を直接使えない。そのため同等 fixture をここで持っている。
