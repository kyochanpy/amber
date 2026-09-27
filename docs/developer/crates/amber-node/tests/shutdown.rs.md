# `crates/amber-node/tests/shutdown.rs`

## 役割

[`crates/amber-node/tests/shutdown.rs`](/Users/sawano/oss/amber/crates/amber-node/tests/shutdown.rs:1) は graceful shutdown の integration test である。

## `shutdown_closes_session_and_cleans_up_staging_state`

- [`shutdown_closes_session_and_cleans_up_staging_state`](/Users/sawano/oss/amber/crates/amber-node/tests/shutdown.rs:17)
  - `camera/image` を記録対象に設定する。
  - 1 input を `handle_input` して staged WAL receipt を得る。
  - `shutdown()` 後に
    - manifest が `Closed`
    - `ended_at` あり
    - observed stream 1 件
    - staging root が削除済み
    - catalog state に WAL segment が 1 件
    - published WAL を読み出せる
    ことを確認する。

## 補助

- `structured_arrow_data`
  - Dora 側 `StructArray` 形式の payload を作る。
- `write_config`
  - integration test 用 YAML 出力。
