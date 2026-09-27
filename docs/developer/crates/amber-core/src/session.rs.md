# `crates/amber-core/src/session.rs`

## 役割

[`crates/amber-core/src/session.rs`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:1) は session manifest の ID 規則、永続形式、閉塞済み stream 集約ルールを持つ。

## `SessionId`

- [`SessionId`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:18)
  - 内部は UUIDv7 文字列。
  - `new()` は `Uuid::now_v7()` を使う。
  - `as_str()` と `Display`/`AsRef<str>` で path 生成やログ出力に使いやすくしている。
  - `parse()` / `FromStr` は UUID と version を検証し、v7 以外を拒否する。
  - v7 を使うことで文字列ソートが時系列順と一致しやすい。
- [`SessionIdError`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:72)
  - UUID parse 失敗と UUIDv7 以外を分けて報告する。

## バージョン定数

- `MANIFEST_VERSION = 1`
- `AMBER_VERSION = env!("CARGO_PKG_VERSION")`

manifest には両方が保存され、後方互換判定と生成元バージョン記録に使われる。

## `SessionManifest`

- [`SessionManifest`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:84)
  - `manifest_version`
  - `session_id`
  - `started_at` / `ended_at` / `updated_at`
  - `status`
  - `config_snapshot`
  - `amber_version`
  - `observed_streams`
  - `tags`
  - `notes`
  を持つ。

## 補助型

- [`SessionStatus`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:100)
  - `Open`, `Closed`, `Interrupted`
- [`ObservedStreamSummary`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:107)
  - stream 単位の観測要約。
- [`ClosedWalStreamUpdate`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:118)
  - writer が segment close 時に manifest へ反映する増分情報。
  - `with_row_count()` / `with_byte_size()` で集計値を後付けできる。

## 主な処理

- `SessionManifest::new`
  - manifest 初期値を作る。status は `Open`。
- `path`
  - [`paths::session_manifest`](/Users/sawano/oss/amber/crates/amber-core/src/storage.rs:329) に従う。
  - 同等の free function `manifest_path(session_id)` も公開されている。
- `observe_closed_wal_stream`
  - 同じ `(node_id, output_id)` の既存 summary があれば `apply(update)`、なければ新規追加。
  - 適用後は node/output 順に sort し、`updated_at` を更新する。
- `close`
  - `ended_at` と `updated_at` を設定し、status を `Closed` にする。
- `save` / `create` / `load` / `close_and_save`
  - storage への永続化 API。
  - `load` は path に保存されていた `session_id` が期待値と異なる場合を明示的に落とす。
- `ObservedStreamSummary::apply`
  - `first_seen_at` と `last_seen_at` を最小/最大に寄せる。
  - 新しい schema fingerprint があれば重複なく追加し sort する。
  - `row_count` と `byte_size` は `accumulate_optional` で加算する。

## エラー

- [`SessionManifestError`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:310)
  - 保存、読込、session id 不一致を分けている。

## 依存関係

- manifest パスは `storage::paths` に依存。
- writer の `publish_closed_segments` が `ClosedWalStreamUpdate` を生成してこの manifest を更新する。
- CLI `list` と node `shutdown` はこのファイルの永続形式に直接依存する。

## テスト

- UUIDv7 の時系列性
- 非 v7 UUID の拒否
- 複数 close update の merge
- create/load/close の往復
- エラーメッセージへの session 文脈付与
を確認している。
