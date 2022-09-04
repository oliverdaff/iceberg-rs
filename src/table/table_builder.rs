/*!
Defining the [TableBuilder] struct for creating catalog tables and starting create/replace transactions
*/

use std::sync::Arc;
use std::time::SystemTime;

use object_store::path::Path;
use uuid::Uuid;

use crate::catalog::table_identifier::TableIdentifier;
use crate::error::{IcebergError, Result};
use crate::model::partition::{PartitionField, Transform};
use crate::model::sort::{NullOrder, SortDirection, SortField, SortOrder};
use crate::model::{partition::PartitionSpec, schema::SchemaV2, table::TableMetadataV2};
use crate::table::Table;

use super::Catalog;

///Builder pattern to create a table
pub struct TableBuilder {
    identifier: TableIdentifier,
    catalog: Arc<dyn Catalog>,
    metadata: TableMetadataV2,
}

impl TableBuilder {
    /// Creates a new [TableBuilder] with some default metadata entries already set.
    pub fn new(
        identifier: TableIdentifier,
        location: String,
        schema: SchemaV2,
        catalog: Arc<dyn Catalog>,
    ) -> Result<Self> {
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
            location,
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
        Ok(TableBuilder {
            identifier,
            metadata,
            catalog,
        })
    }
    /// Building a table writes the metadata file and commits the table to the catalog, TODO !!!!
    pub async fn commit(self) -> Result<Table> {
        let object_store = self.catalog.object_store();
        let location = &self.metadata.location;
        let uuid = Uuid::new_v4();
        let version = &self.metadata.last_sequence_number;
        let metadata_json = serde_json::to_string(&self.metadata)
            .map_err(|err| IcebergError::Message(err.to_string()))?;
        let path: Path = (location.to_string()
            + "/metadata/"
            + &version.to_string()
            + "-"
            + &uuid.to_string()
            + ".metadata.json")
            .into();
        object_store
            .put(&path, metadata_json.into())
            .await
            .map_err(|err| IcebergError::Message(err.to_string()))?;
        self.catalog
            .clone()
            .register_table(&self.identifier, path.as_ref())
            .await?;
        Ok(Table::new(
            self.identifier,
            self.catalog,
            self.metadata,
            path.as_ref(),
        ))
    }
    /// Sets a partition spec for the table.
    pub fn with_partition_spec(mut self, partition_spec: PartitionSpec) -> Self {
        self.metadata.partition_specs.push(partition_spec);
        self
    }
}
