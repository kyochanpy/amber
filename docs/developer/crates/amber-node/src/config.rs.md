# `crates/amber-node/src/config.rs`

## 役割

[`crates/amber-node/src/config.rs`](/Users/sawano/oss/amber/crates/amber-node/src/config.rs:1) は node 起動に必要な設定・storage・session・staging root を準備する。

## 定数

- `AMBER_CONFIG_ENV = "AMBER_CONFIG"`
- `STAGING_ROOT_DIR = "_staging"`

## 各関数

- [`amber_config_path_from_env`](/Users/sawano/oss/amber/crates/amber-node/src/config.rs:13)
  - 環境変数 `AMBER_CONFIG` を `PathBuf` として取り出し、未設定なら明示エラーにする。
- [`load_config`](/Users/sawano/oss/amber/crates/amber-node/src/config.rs:19)
  - `AmberConfig::from_file` に node 向け文脈を付ける。
- [`initialize_storage`](/Users/sawano/oss/amber/crates/amber-node/src/config.rs:24)
  - local backend なら root directory を先に `create_dir_all` する。
  - その後 `Storage::from_config` を呼ぶ。
- [`start_session`](/Users/sawano/oss/amber/crates/amber-node/src/config.rs:45)
  - 新しい `SessionId` と `Utc::now()` を使って open manifest を作る。
- [`prepare_staging_root`](/Users/sawano/oss/amber/crates/amber-node/src/config.rs:57)
  - local backend の storage root 配下に `_staging/session_id=...` を作る。
  - 現状 backend が local 以外だと起動を拒否する。

## 依存関係

- 設定解釈は [`amber-core/src/config.rs`](/Users/sawano/oss/amber/crates/amber-core/src/config.rs:1)。
- manifest 作成は [`amber-core/src/session.rs`](/Users/sawano/oss/amber/crates/amber-core/src/session.rs:1)。
- `app.rs` の `NodeRuntime::initialize_from_path` がこのファイルを順に呼び出す。
