/*!
Defining the [TableBuilder] trait for creating catalog tables and starting create/replace transactions
*/

use crate::model::{partition::PartitionSpec, table::TableMetadataV2};

///Builder pattern to create a table
pub struct TableBuilder {
    metadata: TableMetadataV2,
}

impl TableBuilder {
    /// Sets a partition spec for the table.
    pub fn with_partition_spec(mut self, partition_spec: PartitionSpec) -> Self {
        self.metadata.partition_specs.push(partition_spec);
        self
    }
}
