use crate::{partition::PartitionSpec, schema};
use serde::{de::Error, Deserialize, Deserializer, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq)]
#[repr(u8)]
enum TableMetadataFormatVersion {
    V1 = 1,
    V2 = 2,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", remote = "Self")]
struct TableMetadataV2 {
    /// Integer Version for the format.
    format_version: TableMetadataFormatVersion,
    /// A UUID that identifies the table
    table_uuid: Uuid,
    /// Location tables base location
    location: String,
    /// The tables highest sequence number
    last_sequence_number: i64,
    /// Timestamp in milliseconds from the unix epoch when the table was last updated.
    last_updated_ms: i64,
    /// An integer; the highest assigned column ID for the table.
    last_column_id: i32,
    //A list of schemas, stored as objects with schema-id.
    schemas: Vec<schema::Schema>,
    //ID of the table’s current schema.
    current_schema_id: i32,
    /// A list of partition specs, stored as full partition spec objects.
    partition_specs: Vec<PartitionSpec>,
    /// ID of the “current” spec that writers should use by default.
    default_spec_id: i32,
    /// An integer; the highest assigned partition field ID across all partition specs for the table.
    last_partition_id: i32,
}

impl<'de> Deserialize<'de> for TableMetadataV2 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let this = Self::deserialize(deserializer)?;

        if !matches!(this.format_version, TableMetadataFormatVersion::V2) {
            return Err(D::Error::custom("format-version should be 2"));
        }

        Ok(this)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::TableMetadataV2;

    #[test]
    fn test_deserialize_table_data_v2() -> Result<()> {
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
                "last-partition-id": 1
            }
        "#;
        let metadata = serde_json::from_str::<TableMetadataV2>(&data)?;
        assert!(matches!(
            metadata.format_version,
            crate::table::TableMetadataFormatVersion::V2
        ));
        Ok(())
    }

    #[test]
    fn test_invalid_table_uuid() -> Result<()> {
        let data = r#"
            {
                "format-version" : 2,
                "table-uuid": "xxxx"
            }
        "#;
        assert!(serde_json::from_str::<TableMetadataV2>(&data).is_err());
        Ok(())
    }
    #[test]
    fn test_deserialize_table_data_v2_invalid_format_version() -> Result<()> {
        let data = r#"
            {
                "format-version" : 1
            }
        "#;
        assert!(serde_json::from_str::<TableMetadataV2>(&data).is_err());
        Ok(())
    }
}
