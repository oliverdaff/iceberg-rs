/*!
Defines the [table metadata](https://iceberg.apache.org/spec/#table-metadata).
The main struct here is [TableMetadataV2] which defines the data for a table.
*/
use std::collections::HashMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::snapshot::SnapshotV1;
use crate::model::{
    partition::PartitionSpec,
    schema,
    snapshot::{Reference, SnapshotV2},
    sort,
};

#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// generic table metadata
pub enum TableMetadata {
    /// version 1 of the table metadata
    V1(TableMetadataV1),
    /// version 2 of the table metadata
    V2(TableMetadataV2),
}

impl Serialize for TableMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct TypedTableMetadata {
            format_version: usize,
            #[serde(flatten)]
            meta: TableMetadata,
        }
        let meta = TypedTableMetadata {
            format_version: self.format_version(),
            meta: self.clone(),
        };

        meta.serialize(serializer)
    }
}

impl Deserialize for TableMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'_>,
    {
        use serde_json::Value;
        let v = Value::deserialize(deserializer)?;
        match v
            .get("format-version")
            .and_then(Value::as_u64)
            .ok_or(anyhow!("expected integer field: \"format-version\""))?
        {
            1 => TableMetadataV1::deserialize(v)
                .map(TableMetadata::V1)
                .map_err(|e| anyhow!("parse error: {}", e)),
            2 => TableMetadataV2::deserialize(v)
                .map(TableMetadata::V2)
                .map_err(|e| anyhow!("parse error: {}", e)),
            v => anyhow!("unsupported format version: {}", e),
        }
    }

    fn deserialize_in_place<D>(deserializer: D, place: &mut Self) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Default implementation just delegates to `deserialize` impl.
        *place = try!(Deserialize::deserialize(deserializer));
        Ok(())
    }
}

impl TableMetadata {
    /// the format version this table metadata satisfies
    pub fn format_version(&self) -> usize {
        match self {
            TableMetadata::V1(_) => 1,
            TableMetadata::V2(_) => 2,
        }
    }
    /// convert to the latest table spec
    pub fn to_latest(self) -> TableMetadataV2 {
        match self {
            TableMetadata::V1(v1) => v1.into(),
            TableMetadata::V2(v2) => v2,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "format-version")]
/// Fields for the version 2 of the table metadata.
pub struct TableMetadataV1 {
    /// Integer Version for the format.
    /// A UUID that identifies the table
    ///
    /// Although it's optional in version 1, but for the convenience of
    /// conversion, we make it required.
    pub table_uuid: Uuid,
    /// schema information
    pub schema: schema::SchemaV1,
    /// Location tables base location
    pub location: String,
    /// Timestamp in milliseconds from the unix epoch when the table was last updated.
    pub last_updated_ms: i64,
    /// An integer; the highest assigned column ID for the table.
    pub last_column_id: i32,
    /// A list of schemas, stored as objects with schema-id.
    pub schemas: Option<Vec<schema::SchemaV1>>,
    /// ID of the table’s current schema.
    pub current_schema_id: Option<i32>,
    /// single record of partition spec
    pub partition_spec: PartitionSpec,
    /// A list of partition specs, stored as full partition spec objects.
    pub partition_specs: Option<Vec<PartitionSpec>>,
    /// ID of the “current” spec that writers should use by default.
    pub default_spec_id: Option<i32>,
    /// An integer; the highest assigned partition field ID across all partition specs for the table.
    pub last_partition_id: Option<i32>,
    ///A string to string map of table properties. This is used to control settings that
    /// affect reading and writing and is not intended to be used for arbitrary metadata.
    /// For example, commit.retry.num-retries is used to control the number of commit retries.
    pub properties: Option<HashMap<String, String>>,
    /// long ID of the current table snapshot; must be the same as the current
    /// ID of the main branch in refs.
    pub current_snapshot_id: Option<i64>,
    ///A list of valid snapshots. Valid snapshots are snapshots for which all
    /// data files exist in the file system. A data file must not be deleted
    /// from the file system until the last snapshot in which it was listed is
    /// garbage collected.
    pub snapshots: Option<Vec<SnapshotV1>>,
    /// A list (optional) of timestamp and snapshot ID pairs that encodes changes
    /// to the current snapshot for the table. Each time the current-snapshot-id
    /// is changed, a new entry should be added with the last-updated-ms
    /// and the new current-snapshot-id. When snapshots are expired from
    /// the list of valid snapshots, all entries before a snapshot that has
    /// expired should be removed.
    pub snapshot_log: Option<Vec<SnapshotLog>>,

    /// A list (optional) of timestamp and metadata file location pairs
    /// that encodes changes to the previous metadata files for the table.
    /// Each time a new metadata file is created, a new entry of the
    /// previous metadata file location should be added to the list.
    /// Tables can be configured to remove oldest metadata log entries and
    /// keep a fixed-size log of the most recent entries after a commit.
    pub metadata_log: Option<Vec<MetadataLog>>,

    /// A list of sort orders, stored as full sort order objects.
    pub sort_orders: Option<Vec<sort::SortOrder>>,
    /// Default sort order id of the table. Note that this could be used by
    /// writers, but is not used when reading because reads use the specs
    /// stored in manifest files.
    pub default_sort_order_id: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "format-version")]
/// Fields for the version 2 of the table metadata.
pub struct TableMetadataV2 {
    /// Integer Version for the format.
    /// A UUID that identifies the table
    pub table_uuid: Uuid,
    /// Location tables base location
    pub location: String,
    /// The tables highest sequence number
    pub last_sequence_number: i64,
    /// Timestamp in milliseconds from the unix epoch when the table was last updated.
    pub last_updated_ms: i64,
    /// An integer; the highest assigned column ID for the table.
    pub last_column_id: i32,
    /// A list of schemas, stored as objects with schema-id.
    pub schemas: Vec<schema::SchemaV2>,
    /// ID of the table’s current schema.
    pub current_schema_id: i32,
    /// A list of partition specs, stored as full partition spec objects.
    pub partition_specs: Vec<PartitionSpec>,
    /// ID of the “current” spec that writers should use by default.
    pub default_spec_id: i32,
    /// An integer; the highest assigned partition field ID across all partition specs for the table.
    pub last_partition_id: i32,
    ///A string to string map of table properties. This is used to control settings that
    /// affect reading and writing and is not intended to be used for arbitrary metadata.
    /// For example, commit.retry.num-retries is used to control the number of commit retries.
    pub properties: Option<HashMap<String, String>>,
    /// long ID of the current table snapshot; must be the same as the current
    /// ID of the main branch in refs.
    pub current_snapshot_id: Option<i64>,
    ///A list of valid snapshots. Valid snapshots are snapshots for which all
    /// data files exist in the file system. A data file must not be deleted
    /// from the file system until the last snapshot in which it was listed is
    /// garbage collected.
    pub snapshots: Option<Vec<SnapshotV2>>,
    /// A list (optional) of timestamp and snapshot ID pairs that encodes changes
    /// to the current snapshot for the table. Each time the current-snapshot-id
    /// is changed, a new entry should be added with the last-updated-ms
    /// and the new current-snapshot-id. When snapshots are expired from
    /// the list of valid snapshots, all entries before a snapshot that has
    /// expired should be removed.
    pub snapshot_log: Option<Vec<SnapshotLog>>,

    /// A list (optional) of timestamp and metadata file location pairs
    /// that encodes changes to the previous metadata files for the table.
    /// Each time a new metadata file is created, a new entry of the
    /// previous metadata file location should be added to the list.
    /// Tables can be configured to remove oldest metadata log entries and
    /// keep a fixed-size log of the most recent entries after a commit.
    pub metadata_log: Option<Vec<MetadataLog>>,

    /// A list of sort orders, stored as full sort order objects.
    pub sort_orders: Vec<sort::SortOrder>,
    /// Default sort order id of the table. Note that this could be used by
    /// writers, but is not used when reading because reads use the specs
    /// stored in manifest files.
    pub default_sort_order_id: i64,
    ///A map of snapshot references. The map keys are the unique snapshot reference
    /// names in the table, and the map values are snapshot reference objects.
    /// There is always a main branch reference pointing to the current-snapshot-id
    /// even if the refs map is null.
    pub refs: Option<HashMap<String, Reference>>,
}

impl From<TableMetadataV1> for TableMetadataV2 {
    fn from(value: TableMetadataV1) -> Self {
        let (current_schema_id, schemas) = if let (Some(current_schema_id), Some(schemas)) =
            (value.current_schema_id, value.schemas)
        {
            (
                current_schema_id,
                schemas.into_iter().map(|v1| v1.into()).collect(),
            )
        } else {
            (0, vec![value.schema.into()])
        };

        let (default_spec_id, partition_specs) =
            if let (Some(default_spec_id), Some(partition_specs)) =
                (value.default_spec_id, value.partition_specs)
            {
                (default_spec_id, partition_specs)
            } else {
                (0, vec![value.partition_spec])
            };

        let (default_sort_order_id, sort_orders) =
            if let (Some(default_sort_order_id), Some(sort_orders)) =
                (value.default_sort_order_id, value.sort_orders)
            {
                (default_sort_order_id, sort_orders)
            } else {
                (0, vec![])
            };

        Self {
            table_uuid: value.table_uuid,
            location: value.location,
            last_sequence_number: 0,
            last_updated_ms: value.last_updated_ms,
            last_column_id: value.last_column_id,
            last_partition_id: value.last_partition_id.unwrap_or(0),
            schemas,
            current_schema_id,
            partition_specs,
            default_spec_id,
            properties: value.properties,
            current_snapshot_id: value.current_snapshot_id,
            snapshots: value
                .snapshots
                .map(|snapshots| snapshots.into_iter().map(|s1| s1.into()).collect()),
            snapshot_log: value.snapshot_log,
            metadata_log: value.metadata_log,
            sort_orders,
            default_sort_order_id,
            refs: None,
        }
    }
}

impl From<&TableMetadataV1> for TableMetadataV2 {
    fn from(value: &TableMetadataV1) -> Self {
        let (current_schema_id, schemas) = if let (Some(current_schema_id), Some(schemas)) =
            (value.current_schema_id, &value.schemas)
        {
            (
                current_schema_id,
                schemas.iter().map(|v1| v1.into()).collect(),
            )
        } else {
            (0, vec![value.schema.clone().into()])
        };
        let (default_spec_id, partition_specs) =
            if let (Some(default_spec_id), Some(partition_specs)) =
                (value.default_spec_id, &value.partition_specs)
            {
                (default_spec_id, partition_specs.clone())
            } else {
                (0, vec![value.partition_spec.clone()])
            };
        let (default_sort_order_id, sort_orders) =
            if let (Some(default_sort_order_id), Some(sort_orders)) =
                (value.default_sort_order_id, &value.sort_orders)
            {
                (default_sort_order_id, sort_orders.clone())
            } else {
                (0, vec![])
            };
        Self {
            table_uuid: value.table_uuid,
            location: value.location.clone(),
            last_sequence_number: 0,
            last_updated_ms: value.last_updated_ms,
            last_column_id: value.last_column_id,
            last_partition_id: value.last_partition_id.unwrap_or(0),
            schemas,
            current_schema_id,
            partition_specs,
            default_spec_id,
            properties: value.properties.clone(),
            current_snapshot_id: value.current_snapshot_id,
            snapshots: value
                .snapshots
                .as_ref()
                .map(|snapshots| snapshots.iter().map(|s1| s1.into()).collect()),
            snapshot_log: value.snapshot_log.clone(),
            metadata_log: value.metadata_log.clone(),
            sort_orders,
            default_sort_order_id,
            refs: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Encodes changes to the previous metadata files for the table
pub struct MetadataLog {
    /// The file for the log.
    pub metadata_file: String,
    /// Time new metadata was created
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// A log of when each snapshot was made.
pub struct SnapshotLog {
    /// Id of the snapshot.
    pub snapshot_id: i64,
    /// Last updated timestamp
    pub timestamp_ms: i64,
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::model::table::TableMetadata;

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
        let metadata = serde_json::from_str::<TableMetadata>(data)?;
        //test serialise deserialise works.
        let metadata_two: TableMetadata = serde_json::from_str(&serde_json::to_string(&metadata)?)?;
        assert_eq!(metadata, metadata_two);

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
        assert!(serde_json::from_str::<TableMetadata>(data).is_err());
        Ok(())
    }

    #[test]
    fn test_read_compatible_v1() -> Result<()> {
        let data = r#"
{
  "format-version" : 1,
  "table-uuid" : "bf530b84-8e0a-4949-b2c4-b50f02a1334f",
  "location" : "s3://testbucket/iceberg_data/iceberg_ctl/iceberg_db/iceberg_tbl",
  "last-updated-ms" : 1672980637554,
  "last-column-id" : 2,
  "schema" : {
    "type" : "struct",
    "schema-id" : 0,
    "fields" : [ {
      "id" : 1,
      "name" : "id",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 2,
      "name" : "data",
      "required" : false,
      "type" : "string"
    } ]
  },
  "current-schema-id" : 0,
  "schemas" : [ {
    "type" : "struct",
    "schema-id" : 0,
    "fields" : [ {
      "id" : 1,
      "name" : "id",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 2,
      "name" : "data",
      "required" : false,
      "type" : "string"
    } ]
  } ],
  "partition-spec" : [ ],
  "default-spec-id" : 0,
  "partition-specs" : [ {
    "spec-id" : 0,
    "fields" : [ ]
  } ],
  "last-partition-id" : 999,
  "default-sort-order-id" : 0,
  "sort-orders" : [ {
    "order-id" : 0,
    "fields" : [ ]
  } ],
  "properties" : {
    "owner" : "root"
  },
  "current-snapshot-id" : -1,
  "refs" : { },
  "snapshots" : [ ],
  "statistics" : [ ],
  "snapshot-log" : [ ],
  "metadata-log" : [ ]
}
    "#;
        let table_meta = serde_json::from_str::<TableMetadata>(data)?;
        Ok(())
    }
}
