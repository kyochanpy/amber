# `crates/amber-cli/src/output.rs`

## 役割

[`crates/amber-cli/src/output.rs`](/Users/sawano/oss/amber/crates/amber-cli/src/output.rs:1) は `list` コマンド専用の文字列表現を作る。

## `render_session_list`

- [`render_session_list`](/Users/sawano/oss/amber/crates/amber-cli/src/output.rs:6)
  - 空なら `"No sessions found.\n"` を返す。
  - 非空なら固定幅テーブルのヘッダを作る。
  - 各 [`SessionListEntry`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/list.rs:12) から
    - session id
    - 開始時刻
    - 終了時刻または `-`
    - status
    - stream 数
    - pending WAL の有無
    - Parquet の有無
    を整形して 1 行ずつ連結する。

## 補助関数

- [`format_timestamp`](/Users/sawano/oss/amber/crates/amber-cli/src/output.rs:36)
  - UTC の RFC3339 秒精度で整形する。
- [`format_status`](/Users/sawano/oss/amber/crates/amber-cli/src/output.rs:40)
  - `SessionStatus` を `"open" | "closed" | "interrupted"` に写像する。
- [`yes_no`](/Users/sawano/oss/amber/crates/amber-cli/src/output.rs:48)
  - 真偽値を `"yes"` / `"no"` に固定する。

## 依存関係

出力内容の意味は [`amber-core::SessionManifest`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:84) と [`SessionListEntry`](/Users/sawano/oss/amber/crates/amber-cli/src/commands/list.rs:12) の構造に依存する。物理状態の集約は `list.rs` 側で済ませ、このファイルでは `has_pending_wal` / `has_committed_parquet` を表示文字列へ変換するだけにしている。
