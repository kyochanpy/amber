# `crates/amber-core/src/read_set.rs`

## 役割

[`crates/amber-core/src/read_set.rs`](/Users/sawano/oss/amber/crates/amber-core/src/read_set.rs:1) は「ある session を読むために、どの pending WAL とどの Parquet を見るべきか」を解決する。`inspect` の入口であり、compaction 後も session 単位の見え方を維持する要の層である。

## データ型

- [`SessionSourceFilter`](/Users/sawano/oss/amber/crates/amber-core/src/read_set.rs:11)
  - node/output の任意フィルタ。
- [`WalSource`](/Users/sawano/oss/amber/crates/amber-core/src/read_set.rs:29)
  - 未 compaction の session 専用 WAL source。
- [`ParquetSource`](/Users/sawano/oss/amber/crates/amber-core/src/read_set.rs:38)
  - compact 後 Parquet source。物理ファイルは複数 session を含みうるので `session_id_filter` を持つ。
- [`SessionSourceGroup`](/Users/sawano/oss/amber/crates/amber-core/src/read_set.rs:47)
  - `(node_id, output_id, schema_fingerprint)` 単位の source 束。
- [`SessionSourceSet`](/Users/sawano/oss/amber/crates/amber-core/src/read_set.rs:56)
  - session 全体の解決結果。
  - `session_root` と `manifest` も保持するので、呼び出し側は別途 manifest 再読込なしで論理情報を参照できる。

## `SessionSourceSet::resolve`

- [`resolve`](/Users/sawano/oss/amber/crates/amber-core/src/read_set.rs:64)
  - manifest を load する。
  - catalog events 一覧を読む。
  - `CatalogState::from_events` で fold する。
  - pending WAL について
    - 指定 session のものだけ
    - `Pending` 状態だけ
    - filter に一致するものだけ
    を group へ入れる。
  - compaction event について
    - 作成 Parquet ごとに group key を作る。
    - source WAL 群の中に「要求 session と一致し、node/output/schema も一致する segment」があるかを確認する。
    - 同一 path の重複登録は `seen_parquet_sources` で防ぐ。
  - 各 group 内の source path を sort する。
  - `validate_schema_consistency` で、同じ `(node_id, output_id)` に複数 fingerprint が混ざっていないか確認する。

## 補助処理

- `SessionSourceFilter::matches`
  - `Option` フィルタの両方をまとめて評価する。
- `SourceGroupKey`
  - group の map key。
- `segment_matches_parquet`
  - compact event が対象 session に関係する Parquet かどうかを判定する。
- `validate_schema_consistency`
  - stream ごとに schema fingerprint 集合を作り、複数あるなら `SchemaConflict` を返す。

## エラー

- [`SessionSourceError`](/Users/sawano/oss/amber/crates/amber-core/src/read_set.rs:254)
  - manifest load
  - catalog event list
  - catalog fold
  - 同一 stream の schema conflict
  を区別する。

## 依存関係

- manifest 読込は [`session.rs`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:1)。
- physical state は [`catalog.rs`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:1)。
- 実際の読み取りは CLI [`inspect.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/inspect.rs:1) がこの結果を辿る。

## テスト

- pending WAL と compact 済み Parquet の統合
- cleanup 後も Parquet source を引けること
- 同一 stream 内 schema 衝突の拒否
を確認している。
