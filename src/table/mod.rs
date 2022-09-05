/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::sync::Arc;

use object_store::ObjectStore;

use crate::{
    catalog::{table_identifier::TableIdentifier, Catalog},
    model::table::TableMetadataV2,
};

use self::transaction::Transaction;

mod operation;
pub mod table_builder;
pub mod transaction;

/// Tables can be either one of following types:
/// - FileSystem(https://iceberg.apache.org/spec/#file-system-tables)
/// - Metastore(https://iceberg.apache.org/spec/#metastore-tables)
enum TableType {
    FileSystem(Arc<dyn ObjectStore>),
    Metastore(TableIdentifier, Arc<dyn Catalog>),
}

///Iceberg table
pub struct Table {
    table_type: TableType,
    metadata: TableMetadataV2,
    metadata_location: String,
}

impl Table {
    /// Create a new Table
    pub fn new_metastore_table(
        identifier: TableIdentifier,
        catalog: Arc<dyn Catalog>,
        metadata: TableMetadataV2,
        metadata_location: &str,
    ) -> Self {
        Table {
            table_type: TableType::Metastore(identifier, catalog),
            metadata,
            metadata_location: metadata_location.to_string(),
        }
    }
    /// Get the identifier of the table
    pub fn identifier(&self) -> Option<&TableIdentifier> {
        match &self.table_type {
            TableType::FileSystem(_) => None,
            TableType::Metastore(identifier, _) => Some(identifier),
        }
    }
    /// Get the catalog associated to the table, returns None if the table is a filesystem table
    pub fn catalog(&self) -> Option<&Arc<dyn Catalog>> {
        match &self.table_type {
            TableType::FileSystem(_) => None,
            TableType::Metastore(_, catalog) => Some(catalog),
        }
    }
    /// Get the object_store associated to the table, returns None if the table is a metastore table
    pub fn object_store(&self) -> Option<&Arc<dyn ObjectStore>> {
        match &self.table_type {
            TableType::FileSystem(object_store) => Some(object_store),
            TableType::Metastore(_, _) => None,
        }
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
