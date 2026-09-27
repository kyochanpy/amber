# Developer Docs

このディレクトリには、`crates/amber-cli`、`crates/amber-core`、`crates/amber-node` 配下の各ファイルに 1:1 対応する解説を配置している。

## 配置規則

- ソース: `crates/amber-core/src/writer.rs`
- 対応ドキュメント: `docs/developer/crates/amber-core/src/writer.rs.md`

## 読み始める順序

1. [`docs/developer/crates/amber-core/src/storage.rs.md`](/Users/sawano/oss/amber/docs/developer/crates/amber-core/src/storage.rs.md:1)
2. [`docs/developer/crates/amber-core/src/session.rs.md`](/Users/sawano/oss/amber/docs/developer/crates/amber-core/src/session.rs.md:1)
3. [`docs/developer/crates/amber-core/src/catalog.rs.md`](/Users/sawano/oss/amber/docs/developer/crates/amber-core/src/catalog.rs.md:1)
4. [`docs/developer/crates/amber-core/src/schema.rs.md`](/Users/sawano/oss/amber/docs/developer/crates/amber-core/src/schema.rs.md:1)
5. [`docs/developer/crates/amber-core/src/writer.rs.md`](/Users/sawano/oss/amber/docs/developer/crates/amber-core/src/writer.rs.md:1)
6. [`docs/developer/crates/amber-core/src/compactor.rs.md`](/Users/sawano/oss/amber/docs/developer/crates/amber-core/src/compactor.rs.md:1)
7. [`docs/developer/crates/amber-core/src/read_set.rs.md`](/Users/sawano/oss/amber/docs/developer/crates/amber-core/src/read_set.rs.md:1)
8. [`docs/developer/crates/amber-node/src/app.rs.md`](/Users/sawano/oss/amber/docs/developer/crates/amber-node/src/app.rs.md:1)
9. [`docs/developer/crates/amber-node/src/ingest.rs.md`](/Users/sawano/oss/amber/docs/developer/crates/amber-node/src/ingest.rs.md:1)
10. [`docs/developer/crates/amber-cli/src/commands/inspect.rs.md`](/Users/sawano/oss/amber/docs/developer/crates/amber-cli/src/commands/inspect.rs.md:1)

上の順は、実行時のデータフローに沿っている。個別ファイルを読む場合は、対応する `.md` を同じ相対位置から辿ればよい。
