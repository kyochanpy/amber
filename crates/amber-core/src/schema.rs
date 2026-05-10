use std::collections::{BTreeMap, HashMap};

use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit, UnionFields, UnionMode};
use serde::Serialize;

pub const SESSION_ID_COLUMN: &str = "session_id";
pub const NODE_ID_COLUMN: &str = "node_id";
pub const OUTPUT_ID_COLUMN: &str = "output_id";
pub const NODE_TIMESTAMP_COLUMN: &str = "node_timestamp";
pub const AMBER_TIMESTAMP_COLUMN: &str = "amber_timestamp";

pub const METADATA_COLUMNS: [&str; 5] = [
    SESSION_ID_COLUMN,
    NODE_ID_COLUMN,
    OUTPUT_ID_COLUMN,
    NODE_TIMESTAMP_COLUMN,
    AMBER_TIMESTAMP_COLUMN,
];

const SEMANTIC_METADATA_KEYS: &[&str] = &[
    "ARROW:extension:name",
    "ARROW:extension:metadata",
    "coordinate_frame",
    "encoding_kind",
    "image_encoding",
    "image_format",
    "image_encoding_kind",
    "semantic_type",
    "tensor_shape",
    "unit",
];

pub fn metadata_fields() -> Vec<Field> {
    vec![
        Field::new(SESSION_ID_COLUMN, DataType::Utf8, false),
        Field::new(NODE_ID_COLUMN, DataType::Utf8, false),
        Field::new(OUTPUT_ID_COLUMN, DataType::Utf8, false),
        // Timestamps are nanoseconds since Unix epoch for MVP.
        Field::new(NODE_TIMESTAMP_COLUMN, DataType::Int64, false),
        Field::new(AMBER_TIMESTAMP_COLUMN, DataType::Int64, false),
    ]
}

pub fn metadata_schema() -> Schema {
    Schema::new(metadata_fields())
}

pub fn metadata_field_names() -> &'static [&'static str] {
    &METADATA_COLUMNS
}

pub fn is_metadata_column(name: &str) -> bool {
    METADATA_COLUMNS.contains(&name)
}

pub fn payload_fields(schema: &Schema) -> Vec<Field> {
    schema
        .fields
        .iter()
        .filter(|field| !is_metadata_column(field.name()))
        .map(|field| field.as_ref().clone())
        .collect()
}

pub fn payload_schema(schema: &Schema) -> Schema {
    Schema::new_with_metadata(
        payload_fields(schema),
        filter_semantic_metadata(&schema.metadata).into_iter().collect(),
    )
}

pub fn schema_fingerprint(schema: &Schema) -> String {
    schema_fingerprint_for_payload(&payload_schema(schema))
}

pub fn schema_fingerprint_for_payload(schema: &Schema) -> String {
    let normalized = normalize_schema(schema);
    let bytes = serde_json::to_vec(&normalized).expect("normalized schema should serialize");
    fnv1a128_hex(&bytes)
}

fn normalize_schema(schema: &Schema) -> NormalizedSchema {
    NormalizedSchema {
        fields: schema
            .fields
            .iter()
            .map(|field| normalize_field(field.as_ref()))
            .collect(),
        metadata: filter_semantic_metadata(&schema.metadata),
    }
}

fn normalize_field(field: &Field) -> NormalizedField {
    NormalizedField {
        name: field.name().to_owned(),
        nullable: field.is_nullable(),
        data_type: normalize_data_type(field.data_type()),
        metadata: filter_semantic_metadata(field.metadata()),
    }
}

fn normalize_data_type(data_type: &DataType) -> NormalizedDataType {
    match data_type {
        DataType::Null => leaf_type("null"),
        DataType::Boolean => leaf_type("bool"),
        DataType::Int8 => leaf_type("int8"),
        DataType::Int16 => leaf_type("int16"),
        DataType::Int32 => leaf_type("int32"),
        DataType::Int64 => leaf_type("int64"),
        DataType::UInt8 => leaf_type("uint8"),
        DataType::UInt16 => leaf_type("uint16"),
        DataType::UInt32 => leaf_type("uint32"),
        DataType::UInt64 => leaf_type("uint64"),
        DataType::Float16 => leaf_type("float16"),
        DataType::Float32 => leaf_type("float32"),
        DataType::Float64 => leaf_type("float64"),
        DataType::Timestamp(unit, timezone) => NormalizedDataType {
            kind: "timestamp",
            params: map_from_pairs([
                ("time_unit", normalize_time_unit(*unit)),
                (
                    "timezone",
                    timezone
                        .as_ref()
                        .map_or_else(|| "null".to_owned(), |value| value.to_string()),
                ),
            ]),
            children: Vec::new(),
            union_mode: None,
            union_type_ids: Vec::new(),
        },
        DataType::Date32 => leaf_type("date32"),
        DataType::Date64 => leaf_type("date64"),
        DataType::Time32(unit) => with_param("time32", "time_unit", normalize_time_unit(*unit)),
        DataType::Time64(unit) => with_param("time64", "time_unit", normalize_time_unit(*unit)),
        DataType::Duration(unit) => with_param("duration", "time_unit", normalize_time_unit(*unit)),
        DataType::Interval(unit) => with_param(
            "interval",
            "interval_unit",
            format!("{unit:?}").to_lowercase(),
        ),
        DataType::Binary => leaf_type("binary"),
        DataType::FixedSizeBinary(size) => {
            with_param("fixed_size_binary", "size", size.to_string())
        }
        DataType::LargeBinary => leaf_type("large_binary"),
        DataType::BinaryView => leaf_type("binary_view"),
        DataType::Utf8 => leaf_type("utf8"),
        DataType::LargeUtf8 => leaf_type("large_utf8"),
        DataType::Utf8View => leaf_type("utf8_view"),
        DataType::List(field) => with_child("list", field.as_ref()),
        DataType::ListView(field) => with_child("list_view", field.as_ref()),
        DataType::FixedSizeList(field, size) => {
            let mut normalized = with_child("fixed_size_list", field.as_ref());
            normalized
                .params
                .insert("size".to_owned(), size.to_string());
            normalized
        }
        DataType::LargeList(field) => with_child("large_list", field.as_ref()),
        DataType::LargeListView(field) => with_child("large_list_view", field.as_ref()),
        DataType::Struct(fields) => NormalizedDataType {
            kind: "struct",
            params: BTreeMap::new(),
            children: normalize_fields(fields),
            union_mode: None,
            union_type_ids: Vec::new(),
        },
        DataType::Union(fields, mode) => NormalizedDataType {
            kind: "union",
            params: BTreeMap::new(),
            children: normalize_union_fields(fields),
            union_mode: Some(match mode {
                UnionMode::Sparse => "sparse".to_owned(),
                UnionMode::Dense => "dense".to_owned(),
            }),
            union_type_ids: fields.iter().map(|(type_id, _)| type_id).collect(),
        },
        DataType::Dictionary(key, value) => NormalizedDataType {
            kind: "dictionary",
            params: BTreeMap::new(),
            children: vec![
                NormalizedField {
                    name: "key".to_owned(),
                    nullable: false,
                    data_type: normalize_data_type(key.as_ref()),
                    metadata: BTreeMap::new(),
                },
                NormalizedField {
                    name: "value".to_owned(),
                    nullable: false,
                    data_type: normalize_data_type(value.as_ref()),
                    metadata: BTreeMap::new(),
                },
            ],
            union_mode: None,
            union_type_ids: Vec::new(),
        },
        DataType::Decimal128(precision, scale) => NormalizedDataType {
            kind: "decimal128",
            params: map_from_pairs([
                ("precision", precision.to_string()),
                ("scale", scale.to_string()),
            ]),
            children: Vec::new(),
            union_mode: None,
            union_type_ids: Vec::new(),
        },
        DataType::Decimal256(precision, scale) => NormalizedDataType {
            kind: "decimal256",
            params: map_from_pairs([
                ("precision", precision.to_string()),
                ("scale", scale.to_string()),
            ]),
            children: Vec::new(),
            union_mode: None,
            union_type_ids: Vec::new(),
        },
        DataType::Map(field, keys_sorted) => NormalizedDataType {
            kind: "map",
            params: map_from_pairs([("keys_sorted", keys_sorted.to_string())]),
            children: vec![normalize_field(field.as_ref())],
            union_mode: None,
            union_type_ids: Vec::new(),
        },
        DataType::RunEndEncoded(run_ends, values) => NormalizedDataType {
            kind: "run_end_encoded",
            params: BTreeMap::new(),
            children: vec![
                normalize_field(run_ends.as_ref()),
                normalize_field(values.as_ref()),
            ],
            union_mode: None,
            union_type_ids: Vec::new(),
        },
    }
}

fn normalize_fields(fields: &Fields) -> Vec<NormalizedField> {
    fields
        .iter()
        .map(|field| normalize_field(field.as_ref()))
        .collect()
}

fn normalize_union_fields(fields: &UnionFields) -> Vec<NormalizedField> {
    fields
        .iter()
        .map(|(_, field)| normalize_field(field.as_ref()))
        .collect()
}

fn normalize_time_unit(unit: TimeUnit) -> String {
    match unit {
        TimeUnit::Second => "second".to_owned(),
        TimeUnit::Millisecond => "millisecond".to_owned(),
        TimeUnit::Microsecond => "microsecond".to_owned(),
        TimeUnit::Nanosecond => "nanosecond".to_owned(),
    }
}

fn filter_semantic_metadata(metadata: &HashMap<String, String>) -> BTreeMap<String, String> {
    metadata
        .iter()
        .filter(|(key, _)| is_semantic_metadata_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn is_semantic_metadata_key(key: &str) -> bool {
    SEMANTIC_METADATA_KEYS.contains(&key)
}

fn leaf_type(kind: &'static str) -> NormalizedDataType {
    NormalizedDataType {
        kind,
        params: BTreeMap::new(),
        children: Vec::new(),
        union_mode: None,
        union_type_ids: Vec::new(),
    }
}

fn with_param(kind: &'static str, key: &'static str, value: String) -> NormalizedDataType {
    NormalizedDataType {
        kind,
        params: map_from_pairs([(key, value)]),
        children: Vec::new(),
        union_mode: None,
        union_type_ids: Vec::new(),
    }
}

fn with_child(kind: &'static str, field: &Field) -> NormalizedDataType {
    NormalizedDataType {
        kind,
        params: BTreeMap::new(),
        children: vec![normalize_field(field)],
        union_mode: None,
        union_type_ids: Vec::new(),
    }
}

fn map_from_pairs<const N: usize>(pairs: [(&'static str, String); N]) -> BTreeMap<String, String> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

fn fnv1a128_hex(bytes: &[u8]) -> String {
    const OFFSET_BASIS: u128 = 144066263297769815596495629667062367629;
    const PRIME: u128 = 309485009821345068724781371;

    let mut hash = OFFSET_BASIS;
    for byte in bytes {
        hash ^= u128::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }

    format!("{hash:032x}")
}

#[derive(Debug, Serialize)]
struct NormalizedSchema {
    fields: Vec<NormalizedField>,
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct NormalizedField {
    name: String,
    nullable: bool,
    data_type: NormalizedDataType,
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct NormalizedDataType {
    kind: &'static str,
    params: BTreeMap<String, String>,
    children: Vec<NormalizedField>,
    union_mode: Option<String>,
    union_type_ids: Vec<i8>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema, UnionFields};

    use super::*;

    #[test]
    fn metadata_schema_has_expected_field_order_and_types() {
        let schema = metadata_schema();
        let fields = schema.fields.iter().collect::<Vec<_>>();

        assert_eq!(fields[0].name(), SESSION_ID_COLUMN);
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(fields[1].name(), NODE_ID_COLUMN);
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
        assert_eq!(fields[2].name(), OUTPUT_ID_COLUMN);
        assert_eq!(fields[2].data_type(), &DataType::Utf8);
        assert_eq!(fields[3].name(), NODE_TIMESTAMP_COLUMN);
        assert_eq!(fields[3].data_type(), &DataType::Int64);
        assert_eq!(fields[4].name(), AMBER_TIMESTAMP_COLUMN);
        assert_eq!(fields[4].data_type(), &DataType::Int64);
    }

    #[test]
    fn payload_schema_excludes_amber_metadata_columns() {
        let schema = Schema::new(vec![
            Field::new(SESSION_ID_COLUMN, DataType::Utf8, false),
            Field::new("payload", DataType::Int32, true),
            Field::new(AMBER_TIMESTAMP_COLUMN, DataType::Int64, false),
        ]);

        let payload = payload_schema(&schema);
        let names = payload
            .fields
            .iter()
            .map(|field| field.name().to_owned())
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["payload".to_owned()]);
    }

    #[test]
    fn fingerprint_ignores_amber_and_runtime_metadata() {
        let mut field_with_runtime = Field::new("payload", DataType::Int32, true);
        field_with_runtime.set_metadata(HashMap::from([
            ("debug_label".to_owned(), "camera front".to_owned()),
            ("source_host".to_owned(), "robot-a".to_owned()),
        ]));

        let mut schema_with_runtime = Schema::new(vec![
            Field::new(SESSION_ID_COLUMN, DataType::Utf8, false),
            field_with_runtime,
            Field::new(AMBER_TIMESTAMP_COLUMN, DataType::Int64, false),
        ]);
        schema_with_runtime
            .metadata
            .insert("process_id".to_owned(), "1234".to_owned());

        let schema_without_runtime =
            Schema::new(vec![Field::new("payload", DataType::Int32, true)]);

        assert_eq!(
            schema_fingerprint(&schema_with_runtime),
            schema_fingerprint(&schema_without_runtime)
        );
    }

    #[test]
    fn fingerprint_changes_when_semantic_metadata_changes() {
        let mut image_field_png = Field::new("image", DataType::Binary, false);
        image_field_png.set_metadata(HashMap::from([
            ("image_format".to_owned(), "png".to_owned()),
            ("debug_label".to_owned(), "rgb".to_owned()),
        ]));

        let mut image_field_jpeg = Field::new("image", DataType::Binary, false);
        image_field_jpeg.set_metadata(HashMap::from([
            ("image_format".to_owned(), "jpeg".to_owned()),
            ("debug_label".to_owned(), "rgb".to_owned()),
        ]));

        let png_schema = Schema::new(vec![image_field_png]);
        let jpeg_schema = Schema::new(vec![image_field_jpeg]);

        assert_ne!(
            schema_fingerprint_for_payload(&png_schema),
            schema_fingerprint_for_payload(&jpeg_schema)
        );
    }

    #[test]
    fn fingerprint_captures_nested_structure_and_field_order() {
        let struct_a = Field::new(
            "payload",
            DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Int32, false),
                Field::new("y", DataType::Int32, false),
            ])),
            false,
        );
        let struct_b = Field::new(
            "payload",
            DataType::Struct(Fields::from(vec![
                Field::new("y", DataType::Int32, false),
                Field::new("x", DataType::Int32, false),
            ])),
            false,
        );

        assert_ne!(
            schema_fingerprint_for_payload(&Schema::new(vec![struct_a])),
            schema_fingerprint_for_payload(&Schema::new(vec![struct_b]))
        );
    }

    #[test]
    fn fingerprint_captures_union_type_ids() {
        let union_a = Field::new(
            "payload",
            DataType::Union(
                UnionFields::new(
                    vec![1, 3],
                    vec![
                        Arc::new(Field::new("x", DataType::Int32, false)),
                        Arc::new(Field::new("y", DataType::Utf8, true)),
                    ],
                ),
                UnionMode::Dense,
            ),
            false,
        );
        let union_b = Field::new(
            "payload",
            DataType::Union(
                UnionFields::new(
                    vec![1, 4],
                    vec![
                        Arc::new(Field::new("x", DataType::Int32, false)),
                        Arc::new(Field::new("y", DataType::Utf8, true)),
                    ],
                ),
                UnionMode::Dense,
            ),
            false,
        );

        assert_ne!(
            schema_fingerprint_for_payload(&Schema::new(vec![union_a])),
            schema_fingerprint_for_payload(&Schema::new(vec![union_b]))
        );
    }
}
