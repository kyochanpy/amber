# `crates/amber-core/src/writer.rs`

## 役割

[`crates/amber-core/src/writer.rs`](/Users/sawano/oss/amber/crates/amber-core/src/writer.rs:1) は WAL writer 本体である。入力 batch を stream 単位の staged Arrow IPC segment に追記し、`rotate` または `shutdown` 時に object store へ publish し、catalog event と session manifest を更新する。

## 公開 API

- [`WalWriter`](/Users/sawano/oss/amber/crates/amber-core/src/writer.rs:27)
  - background task を所有するフロント API。
  - `spawn_local_with_capacity` で command queue 容量も調整でき、既定値は `DEFAULT_WRITER_QUEUE_CAPACITY = 64`。
- [`WalWriterHandle`](/Users/sawano/oss/amber/crates/amber-core/src/writer.rs:34)
  - rotation runtime から使う軽量 handle。
- [`WriteCommand`](/Users/sawano/oss/amber/crates/amber-core/src/writer.rs:148)
  - task へ送る内部コマンド列挙。`Write` / `Flush` / `Rotate` / `Shutdown` を表す。
- [`WalWriteRequest`](/Users/sawano/oss/amber/crates/amber-core/src/writer.rs:164)
  - session/node/output/schema_fingerprint/batch を持つ。
  - `new()` で組み立てられる。
- [`WalWriteReceipt`](/Users/sawano/oss/amber/crates/amber-core/src/writer.rs:191)
  - 現在の segment id/path/累積 row_count を返す。
- [`WalRotateRequest`](/Users/sawano/oss/amber/crates/amber-core/src/writer.rs:198)
  - `new()` で対象 stream を組み立てる。
- [`WalRotateReceipt`](/Users/sawano/oss/amber/crates/amber-core/src/writer.rs:219)
  - `rotated` と、実際に rotate した場合の `segment_id` / `path` を返す。

## 実行モデル

- `spawn_local` / `spawn_local_with_capacity`
  - `mpsc` queue と writer task を作る。
- `write`, `flush`, `rotate`, `shutdown`
  - すべて `send_command` で task に命令を送り、`CommandResult` を待つ。
- `shutdown`
  - 二重呼び出しを許容し、`await_join` を通じて task の終了を保証する。
- `CommandEnvelope`
  - command 本体と `oneshot` 応答チャネルを束ねる transport 単位。

## task 内部

- `run_writer_task`
  - `HashMap<WalStreamKey, OpenWalSegment>` を持ち、command を順番に処理する。
- `WalStreamKey`
  - open segment を `(session_id, node_id, output_id)` 単位で束ねる。schema fingerprint は segment 内一貫性検査に使い、key には含めない。
- `handle_command`
  - `Write` -> `handle_write`
  - `Flush` -> `flush_segments`
  - `Rotate` -> `handle_rotate`
  - `Shutdown` -> `shutdown_segments`

## 書き込み

- `handle_write`
  - `(session_id,node_id,output_id)` ごとに `OpenWalSegment` を引く。
  - 未作成なら `OpenWalSegment::new` で stage ファイルを作る。
  - 既存 segment と `schema_fingerprint` が違えば `SchemaFingerprintChanged`。
  - fingerprint は同じでも Arrow schema 実体が違えば `SchemaMismatch`。
  - 問題なければ `append` し、累積 row_count を含む receipt を返す。

## flush / rotate / shutdown

- `flush_segments`
  - 各 open segment に対し `flush_durable` を呼び、ローカル staged ファイルを `sync_data()` まで行う。ただし catalog へはまだ見せない。
- `handle_rotate`
  - 対象 stream の open segment だけを閉じる。
  - `close(storage)` で publish 用の `ClosedWalSegment` を作り、`publish_closed_segments` で catalog/manifest へ反映する。
  - 対象 stream が open でなければ no-op receipt を返す。
- `shutdown_segments`
  - 全 open segment を drain して close/publish する。

## publish

- `publish_closed_segments`
  - 各 closed segment について `WalSegmentClosedEvent` を保存する。
  - session ごとに `ClosedWalStreamUpdate` をまとめ、manifest を load -> update -> save する。
  - 最後に staged file を削除する。

## `OpenWalSegment`

- `new`
  - path を `paths::wal_segment` 規約で決める。
  - staging root 配下に同じ相対構造でローカルファイルを作る。
  - `StreamWriter::try_new_buffered` を開く。
- `append`
  - batch を Arrow IPC stream へ追記し、row_count と timestamp stats を更新する。
- `flush_durable`
  - buffered writer flush 後に `sync_data()` を blocking task で実行する。
- `close`
  - Arrow stream を finalize し、staged bytes を object store へ publish する。
  - 再読込して publish 成功を検証する。
  - timestamp stats から close event と manifest update を生成する。
  - row が 0 件なら `first_seen_at=open_at`, `last_seen_at=closed_at` を使う。

## metadata 依存

- `TimestampStats`
  - node/amber timestamp の min/max を segment 単位で蓄積する。
- `TimestampStats::update`
  - `metadata_bounds(batch, NODE_TIMESTAMP_COLUMN)` と `metadata_bounds(batch, AMBER_TIMESTAMP_COLUMN)` を使う。
- `metadata_bounds`
  - Int64 metadata 列の min/max を手で走査する。
  - metadata 列欠落や型不一致は writer error にする。

## エラー

[`WalWriterError`](/Users/sawano/oss/amber/crates/amber-core/src/writer.rs:226) は task 不在、schema 変化、ファイル作成/追記/flush/sync/finalize/publish/manifest 更新失敗まで細かく分ける。`amber-node` は fatal 扱いで停止させる。

## 依存関係

- metadata 列規約: [`schema.rs`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:1)
- catalog 更新: [`catalog.rs`](/Users/sawano/oss/amber/crates/amber-core/src/catalog.rs:1)
- manifest 更新: [`session.rs`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:1)

## テスト

- path 規約
- 同一 stream 追記
- fingerprint 変化拒否
- schema 不一致拒否
- flush が durability だけを与えること
- shutdown による publish/catalog/manifest 更新
- 複数 stream 一括 close
- rotate の対象 stream 限定動作
- missing stream rotate の no-op
- segment id の UUIDv7 性
を確認している。
