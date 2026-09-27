# `crates/amber-node/src/lib.rs`

## 役割

[`crates/amber-node/src/lib.rs`](/Users/sawano/oss/amber/crates/amber-node/src/lib.rs:1) は node クレートの公開面をまとめる。

## モジュール

- `app`
- `config`
- `dora_arrow`
- `ingest`
- `rotation`
- `streams`

## 再 export

- `pub use app::run;`
  - バイナリ [`src/main.rs`](/Users/sawano/oss/amber/crates/amber-node/src/main.rs:1) は crate root から `run()` だけを使う。
