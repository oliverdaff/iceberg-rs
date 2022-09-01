/*!
Defining the [TableBuilder] trait for creating catalog tables and starting create/replace transactions
*/

use std::time::SystemTime;

use uuid::Uuid;

use crate::error::{IcebergError, Result};
use crate::model::partition::{PartitionField, Transform};
use crate::model::sort::{NullOrder, SortDirection, SortField, SortOrder};
use crate::model::{partition::PartitionSpec, schema::SchemaV2, table::TableMetadataV2};

///Builder pattern to create a table
pub struct TableBuilder {
    metadata: TableMetadataV2,
}

impl TableBuilder {
    /// Creates a new [TableBuilder] with some default metadata entries already set.
    pub fn new(location: String, schema: SchemaV2) -> Result<Self> {
        let partition_spec = PartitionSpec {
            spec_id: 1,
            fields: vec![PartitionField {
                name: "default".to_string(),
                field_id: 1,
                source_id: 1,
                transform: Transform::Void,
            }],
        };
        let sort_order = SortOrder {
            order_id: 1,
            fields: vec![SortField {
                source_id: 1,
                transform: Transform::Void,
                direction: SortDirection::Descending,
                null_order: NullOrder::Last,
            }],
        };
        let metadata = TableMetadataV2 {
            table_uuid: Uuid::new_v4(),
            location: location,
            last_sequence_number: 0,
            last_updated_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|err| IcebergError::Message(err.to_string()))?
                .as_millis() as i64,
            last_column_id: schema.struct_fields.fields.len() as i32,
            schemas: vec![schema],
            current_schema_id: 1,
            partition_specs: vec![partition_spec],
            default_spec_id: 1,
            last_partition_id: 1,
            properties: None,
            current_snapshot_id: None,
            snapshots: None,
            snapshot_log: None,
            metadata_log: None,
            sort_orders: vec![sort_order],
            default_sort_order_id: 0,
            refs: None,
        };
        Ok(TableBuilder { metadata: metadata })
    }
    /// Sets a partition spec for the table.
    pub fn with_partition_spec(mut self, partition_spec: PartitionSpec) -> Self {
        self.metadata.partition_specs.push(partition_spec);
        self
    }
}
