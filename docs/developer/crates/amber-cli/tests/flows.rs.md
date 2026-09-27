# `crates/amber-cli/tests/flows.rs`

## 役割

[`crates/amber-cli/tests/flows.rs`](/Users/sawano/oss/amber/crates/amber-cli/tests/flows.rs:1) は CLI 周辺の MVP ハッピーパスを end-to-end で検証する integration test である。

## `mvp_happy_path_flows_from_wal_to_compaction_to_inspect_rows`

- [`mvp_happy_path_flows_from_wal_to_compaction_to_inspect_rows`](/Users/sawano/oss/amber/crates/amber-cli/tests/flows.rs:19)
  - manifest を作成する。
  - `WalWriter` で同一 stream に 2 回書き、1 回 rotate して 2 segment を作る。
  - shutdown 後、manifest に observed stream が反映されたことを確認する。
  - `run_compact` を呼んで 2 segment が 1 parquet へまとまることを確認する。
  - catalog event の種類と件数を確認する。
  - `SessionSourceSet::resolve` により、pending WAL なし・Parquet 1 件の読み取り集合になることを確認する。
  - 実 Parquet を開き、Amber メタデータ列が残っていることを検証する。
  - 最後に `collect_inspect_rows` で payload 文字列が `"value=1, label=first"` / `"value=2, label=second"` になることまで確認する。

## 意味

この 1 本で `writer -> catalog -> compactor -> read_set -> inspect` の主要な接続面が崩れていないかを見る。
