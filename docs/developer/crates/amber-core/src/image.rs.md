# `crates/amber-core/src/image.rs`

## 役割

[`crates/amber-core/src/image.rs`](/Users/sawano/oss/amber/crates/amber-core/src/image.rs:1) は画像 payload を検出し、バイナリ asset を `sessions/.../assets/...` に切り出したうえで、WAL に書く metadata batch へ変換する。非画像 payload は素通しで `None` を返す。

## 基本方針

- 圧縮済み画像
  - payload schema はそのまま schema catalog に登録する。
  - 画像本体は asset として保存し、WAL には `width/height/channels/format/asset_relpath/byte_size` を書く。
- raw 画像
  - metadata から shape/encoding/quality を読み、JPEG または PNG に再エンコードする。
  - raw schema は `canonicalize_raw_image_schema` で正規化して fingerprint を安定化する。
  - 既定の raw 出力は JPEG、既定 quality は 85。

## 公開型

- [`PreparedImageBatch`](/Users/sawano/oss/amber/crates/amber-core/src/image.rs:27)
  - `schema_fingerprint`
  - `normalized_payload_schema`
  - `metadata_batch`
- [`ImageError`](/Users/sawano/oss/amber/crates/amber-core/src/image.rs:34)
  - 画像認識、decode、encode、shape/quality 解釈、asset 永続化、metadata batch 生成の失敗を表す。
  - `is_recoverable()` で ingest 側が recoverable/fatal を分ける。asset 永続化や検証に関する storage error は recoverable に含めず、ingest 側で fatal 扱いになる。

## メインフロー

- [`prepare_image_batch`](/Users/sawano/oss/amber/crates/amber-core/src/image.rs:133)
  - まず `prepare_compressed_image_batch` を試す。
  - 失敗でなく「非該当」なら `prepare_raw_image_batch` を試す。

## 圧縮画像経路

- [`prepare_compressed_image_batch`](/Users/sawano/oss/amber/crates/amber-core/src/image.rs:150)
  - `compressed_image_column` で「単一 `LargeBinary` 列かつ format metadata あり」を確認する。
  - 各行を decode して width/height/channels を抽出する。
  - 元の圧縮 bytes をそのまま asset として保存する。
  - schema fingerprint と schema catalog は「派生 metadata batch」ではなく「入力 payload schema」の意味論で計算する。

## raw 画像経路

- [`prepare_raw_image_batch`](/Users/sawano/oss/amber/crates/amber-core/src/image.rs:194)
  - `raw_image_column` で raw 画像っぽい単一 `LargeBinary` 列を検出する。
  - `raw_image_dimensions` で `tensor_shape` または `width/height/channels` を読む。
  - `raw_image_encoding` / `raw_image_quality` で出力形式を決める。
  - 行ごとに byte 長を検証し、`encode_raw_image` で JPEG/PNG 化する。
  - `canonicalize_raw_image_schema` で metadata を正規化し、その schema から fingerprint を計算する。
  - PNG 指定時は quality 値を妥当性確認だけ行い、 canonical schema からは `image_quality` を落とす。

## asset 永続化

- [`persist_image_rows`](/Users/sawano/oss/amber/crates/amber-core/src/image.rs:271)
  - 行ごとに `asset-{uuidv7}-{row}.ext` を発番する。
  - storage に `put_bytes` したあと `exists` で再確認する。
  - WAL に書く metadata batch を 6 列構成で作る。

## 補助処理

- `compressed_image_column`
  - 圧縮画像の検出。metadata key は `image_format` / `image_encoding` / `format` / `mime_type` の揺れを吸収する。
- `raw_image_column`
  - raw 画像の検出。`image_encoding_kind=raw` / `encoding_kind=raw` だけでなく shape/encoding/quality metadata の存在でも候補に入る。
- `parse_image_format`, `looks_like_raw_image`
  - metadata キーの揺れを吸収する。
- `parse_tensor_shape`
  - `"HxWxC"` または `"H,W,C"` 形式を解釈。
- `encode_raw_image`
  - `image` crate encoder で再エンコード。
- `color_type_for_encoding`
  - JPEG は 1ch/3ch、PNG は 1/2/3/4ch を許可する。
- `canonicalize_raw_image_schema`
  - raw 画像 metadata を正規化し、不要な quality を落とす。

## 内部定数

- `IMAGE_FORMAT_KEYS` / `IMAGE_ENCODING_KEYS` / `IMAGE_QUALITY_KEYS` / `RAW_IMAGE_KIND_KEYS`
  - metadata key の別名集合。
- `DEFAULT_RAW_ENCODING = jpeg`
- `DEFAULT_JPEG_QUALITY = 85`

## 内部補助型

- `EncodedImageFormat`
  - `jpeg` / `png` の内部表現。`name()` / `extension()` / `as_image_format()` を持つ。
- `RawImageDimensions`
  - `width` / `height` / `channels` と `pixel_bytes()` を持つ。
- `ImageMetadataRow`
  - asset 保存前の 1 行分中間表現。

## 依存関係

- ingest はこの戻り値が `Some` のとき payload を metadata batch に差し替える。
- schema fingerprint 計算は [`schema.rs`](/Users/sawano/oss/amber/crates/amber-core/src/schema.rs:1) に依存。
- asset path 規約は [`storage.rs`](/Users/sawano/oss/amber/crates/amber-core/src/storage.rs:1) に依存。

## テスト

- 圧縮 PNG の asset 化
- 非画像 payload の素通し
- raw 画像の既定 JPEG 化
- raw 画像の PNG 化
を確認している。
