/*!
Structs that model the Iceberg spec.  These structs will
serialise and deserialise the JSON.

## Table metadata example

```rust
use iceberg_rs::model::table::TableMetadataV2;

let data = r#"
    {
        "format-version" : 2,
        "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
        "location": "s3://b/wh/data.db/table",
        "last-sequence-number" : 1,
        "last-updated-ms": 1515100955770,
        "last-column-id": 1,
        "schemas": [
            {
                "schema-id" : 1,
                "type" : "struct",
                "fields" :[
                    {
                        "id": 1,
                        "name": "struct_name",
                        "required": true,
                        "field_type": "fixed[1]"
                    }
                ]
            }
        ],
        "current-schema-id" : 1,
        "partition-specs": [
            {
                "spec-id": 1,
                "fields": [
                    {
                        "source-id": 4,
                        "field-id": 1000,
                        "name": "ts_day",
                        "transform": "day"
                    }
                ]
            }
        ],
        "default-spec-id": 1,
        "last-partition-id": 1,
        "properties": {
            "commit.retry.num-retries": "1"
        },
        "metadata-log": [
            {
                "metadata-file": "s3://bucket/.../v1.json",
                "timestamp-ms": 1515100
            }
        ],
        "sort-orders": [],
        "default-sort-order-id": 0
    }
"#;
 let metadata = serde_json::from_str::<TableMetadataV2>(&data).unwrap();
```

*/
pub mod manifest_list;
pub mod decimal;
pub mod manifest;
pub mod partition;
pub mod schema;
pub mod snapshot;
pub mod sort;
pub mod table;
pub mod types;
