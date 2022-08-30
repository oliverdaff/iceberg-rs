/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::collections::HashMap;

use anyhow::Error;
use uuid::Uuid;

use crate::model::{
    partition::PartitionSpec,
    schema,
    snapshot::{Reference, SnapshotV2},
    sort,
    table::{MetadataLog, SnapshotLog, TableMetadataV2},
};

///Iceberg table
pub struct Table {
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
    pub schemas: Vec<Schema>,
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
    pub snapshots: Option<Vec<Snapshot>>,
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

///General snapshot, can be V1 or V2
pub enum Snapshot {
    ///Snapshot V2
    V2(SnapshotV2),
}

///General schema, can be V1 or V2
pub enum Schema {
    ///Schema V2
    V2(schema::SchemaV2),
}

impl TryFrom<TableMetadataV2> for Table {
    type Error = Error;

    fn try_from(value: TableMetadataV2) -> Result<Self, Self::Error> {
        let snapshots = value
            .snapshots
            .map(|x| x.into_iter().map(|y| Snapshot::V2(y)).collect::<Vec<_>>());
        let schemas = value
            .schemas
            .into_iter()
            .map(|x| Schema::V2(x))
            .collect::<Vec<_>>();
        Ok(Table {
            table_uuid: value.table_uuid,
            location: value.location,
            last_sequence_number: value.last_sequence_number,
            last_updated_ms: value.last_updated_ms,
            last_column_id: value.last_column_id,
            schemas: schemas,
            current_schema_id: value.current_schema_id,
            partition_specs: value.partition_specs,
            default_spec_id: value.default_spec_id,
            last_partition_id: value.last_partition_id,
            properties: value.properties,
            current_snapshot_id: value.current_snapshot_id,
            snapshots: snapshots,
            snapshot_log: value.snapshot_log,
            metadata_log: value.metadata_log,
            sort_orders: value.sort_orders,
            default_sort_order_id: value.default_sort_order_id,
            refs: value.refs,
        })
    }
}
