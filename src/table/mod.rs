/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::sync::Arc;

use crate::{catalog::Catalog, model::table::TableMetadataV2};

use self::transaction::Transaction;

mod operation;
pub mod table_builder;
pub(crate) mod transaction;

///Iceberg table
pub struct Table {
    catalog: Arc<dyn Catalog>,
    metadata: TableMetadataV2,
    metadata_location: String,
}

impl Table {
    /// Create a new Table
    pub fn new(
        catalog: Arc<dyn Catalog>,
        metadata: TableMetadataV2,
        metadata_location: &str,
    ) -> Self {
        Table {
            catalog: catalog,
            metadata: metadata,
            metadata_location: metadata_location.to_string(),
        }
    }
    /// Get the catalog associated to the table
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }
    /// Get the metadata of the table
    pub fn metadata(&self) -> &TableMetadataV2 {
        &self.metadata
    }
    /// Get the location of the current metadata file
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }
    /// Create a new transaction for this table
    pub fn new_transaction(&mut self) -> Transaction {
        Transaction::new(self)
    }
}
