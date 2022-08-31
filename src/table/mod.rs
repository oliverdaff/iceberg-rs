/*!
Defining the [Table] struct that represents an iceberg table.
*/

use crate::model::table::TableMetadataV2;

mod operation;
pub(crate) mod transaction;

///Iceberg table
pub struct Table {
    metadata: TableMetadataV2,
}

impl From<TableMetadataV2> for Table {
    fn from(value: TableMetadataV2) -> Self {
        Table { metadata: value }
    }
}
