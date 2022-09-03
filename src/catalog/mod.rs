/*!
Defines traits to communicate with an iceberg catalog.
*/

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;

pub mod namespace;
pub mod table_builder;
pub mod table_identifier;

use crate::{model::schema::SchemaV2, table::Table};
use object_store::ObjectStore;
use table_identifier::TableIdentifier;

use self::namespace::Namespace;
use self::table_builder::TableBuilder;

/// Trait to create, replace and drop tables in an iceberg catalog.
#[async_trait::async_trait]
pub trait Catalog: Send + Sync {
    /// Lists all tables in the given namespace.
    async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<TableIdentifier>>;
    /// Create a table from an identifier and a schema
    async fn create_table(
        self: Arc<Self>,
        identifier: &TableIdentifier,
        schema: &SchemaV2,
    ) -> Result<Table>;
    /// Check if a table exists
    async fn table_exists(&self, identifier: &TableIdentifier) -> bool;
    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: &TableIdentifier) -> Result<()>;
    /// Load a table.
    async fn load_table(self: Arc<Self>, identifier: &TableIdentifier) -> Result<Table>;
    /// Invalidate cached table metadata from current catalog.
    async fn invalidate_table(&self, identifier: &TableIdentifier) -> Result<()>;
    /// Register a table with the catalog if it doesn't exist.
    async fn register_table(
        self: Arc<Self>,
        identifier: &TableIdentifier,
        metadata_file_location: &str,
    ) -> Result<Table>;
    /// Update a table by atomically changing the pointer to the metadata file
    async fn update_table(
        self: Arc<Self>,
        identifier: &TableIdentifier,
        metadata_file_location: &str,
        previous_metadata_file_location: &str,
    ) -> Result<Table>;
    /// Instantiate a builder to either create a table or start a create/replace transaction.
    async fn build_table(
        self: Arc<Self>,
        identifier: &TableIdentifier,
        schema: &SchemaV2,
    ) -> Result<TableBuilder>;
    /// Initialize a catalog given a custom name and a map of catalog properties.
    /// A custom Catalog implementation must have a no-arg constructor. A compute engine like Spark
    /// or Flink will first initialize the catalog without any arguments, and then call this method to
    /// complete catalog initialization with properties passed into the engine.
    async fn initialize(self: Arc<Self>, properties: &HashMap<String, String>) -> Result<()>;
    /// Return the associated object store to the catalog
    fn object_store(&self) -> Arc<dyn ObjectStore>;
}
