# `crates/amber-core/src/lib.rs`

## 役割

[`crates/amber-core/src/lib.rs`](/Users/sawano/oss/amber/crates/amber-core/src/lib.rs:1) は core クレートの crate root で、内部モジュールを公開し、上位クレートが使う型を再 export する。

## モジュール

- `catalog`
- `compactor`
- `config`
- `image`
- `read_set`
- `schema`
- `session`
- `storage`
- `writer`

## 再 export の意味

上位クレートは個別モジュールを深追いせず、`amber_core::SessionManifest` や `amber_core::WalWriter` のように crate root 経由で使える。`amber-cli` と `amber-node` の import が短く保たれる一方で、実装は各ファイルに分離されている。
