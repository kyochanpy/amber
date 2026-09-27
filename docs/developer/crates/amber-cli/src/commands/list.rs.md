# `crates/amber-cli/src/commands/list.rs`

## 役割

[`crates/amber-cli/src/commands/list.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/list.rs:1) は session manifest 一覧と物理状態を結合し、`amber list` の表示元データを作る。

## データ型

- [`SessionListEntry`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/list.rs:12)
  - `manifest` に論理状態、`has_pending_wal` と `has_committed_parquet` に物理状態を持つ。
- [`SessionPhysicalState`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/list.rs:19)
  - catalog から導く集約結果だけを持つ。

## `run_list`

- [`run_list`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/list.rs:24)
  - `load_storage` で storage を作る。
  - `list_session_manifests` で manifest 群を取る。
  - `started_at` 降順、同時刻なら `session_id` 降順で並べ替える。
  - `--tag` があれば manifest の `tags` でフィルタする。
  - `load_session_physical_states` で catalog から pending/parquet 状態を引く。
  - manifest と physical state を結合して `SessionListEntry` 群を作る。
  - `--latest` または `--limit` を適用する。

## `list_session_manifests`

- [`list_session_manifests`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/list.rs:63)
  - `sessions/` 以下を列挙し、`manifest.json` だけを対象にする。
  - 壊れた manifest は `eprintln!` で warning を出し、一覧全体は止めない。
  - この設計により、一部破損があっても他セッションは表示できる。

## `load_session_physical_states`

- [`load_session_physical_states`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/list.rs:91)
  - `CatalogState::load` で folded catalog を取得する。
  - WAL segment の `state` ごとに
    - `Pending` -> `has_pending_wal = true`
    - `Compacted | Deleted` -> `has_committed_parquet = true`
    と集約する。
  - deleted 済みでも Parquet が存在した履歴として扱う。

## 依存関係

- manifest 列挙は [`amber-core/src/session.rs`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:1) の保存形式に依存。
- physical state 集約は [`amber-core/src/catalog.rs`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:1) の folding ルールに依存。

## テスト

- [`list_command_filters_and_summarizes_sessions`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/list.rs:128)
  - 並び順、tag フィルタ、latest、limit、physical state 集約、整形表示まで一通り検証する。
