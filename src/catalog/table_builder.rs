/*!
Defining the [TableBuilder] trait for creating catalog tables and starting create/replace transactions
*/

use crate::model::partition::PartitionSpec;

///Builder pattern to create a table
pub trait TableBuilder {
    ///Sets partition spec for the table.
    fn with_partition_spec(self, spec: PartitionSpec) -> Box<dyn TableBuilder>;
}
