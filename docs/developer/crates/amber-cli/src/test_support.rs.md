# `crates/amber-cli/src/test_support.rs`

## 役割

[`crates/amber-cli/src/test_support.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/test_support.rs:1) は CLI 単体テストの共通 fixture を提供する。

## `CreatedManifest`

- [`CreatedManifest`](/Users/sawano/oss/amber/crates/amber-cli/src/test_support.rs:18)
  - 実質的には `SessionManifest` を包むだけの薄い返却型。
  - テスト側が manifest 本体を読めるようにしている。

## `create_manifest`

- [`create_manifest`](/Users/sawano/oss/amber/crates/amber-cli/src/test_support.rs:22)
  - 新しい `SessionId` を作り `SessionManifest::create` を呼ぶ。
  - `stream_count` の回数だけ `ClosedWalStreamUpdate` を生成し、manifest に観測済み stream を埋める。
  - `tags` を設定する。
  - `status` に応じて
    - `Open`: そのまま
    - `Closed`: `close(...)`
    - `Interrupted`: `ended_at` と `updated_at` と `status` を手動で上書き
    する。
  - 最後に `save` して返す。

## `write_config`

- [`write_config`](/Users/sawano/oss/amber/crates/amber-cli/src/test_support.rs:62)
  - 最小 `amber.yaml` を一時ディレクトリに書き出す。
  - compaction target は 1MB にして、テストで小さなデータでも動くようにする。

## `metadata_enriched_batch*`

- [`metadata_enriched_batch`](/Users/sawano/oss/amber/crates/amber-cli/src/test_support.rs:75)
  - 固定 stream (`session-1`, `node-a`, `output-x`) を使う簡易版。
- [`metadata_enriched_batch_for_stream`](/Users/sawano/oss/amber/crates/amber-cli/src/test_support.rs:92)
  - `value:Int32` と `label:Utf8` を持つ payload を作り、`prepend_metadata_columns` で Amber 標準メタデータ列を前置する。

## 依存関係

このファイルは CLI テストが `amber-core` の manifest/WAL ルールを再利用できるようにする補助層である。
