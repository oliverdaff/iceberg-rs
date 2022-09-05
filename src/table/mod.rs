/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::sync::Arc;

use anyhow::{anyhow, Result};
use futures::StreamExt;
use object_store::{path::Path, ObjectStore};

use crate::{
    catalog::{table_identifier::TableIdentifier, Catalog},
    model::table::TableMetadataV2,
    transaction::Transaction,
};

pub mod table_builder;

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
    /// Load a filesystem table from an objectstore
    pub async fn load_file_system_table(
        location: &str,
        object_store: &Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let path: Path = (location.to_string() + "/metadata/").into();
        let files = object_store
            .list(Some(&path))
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        let version = files
            .fold(Ok::<i64, anyhow::Error>(0), |acc, x| async move {
                match (acc, x) {
                    (Ok(acc), Ok(object_meta)) => {
                        let name = object_meta
                            .location
                            .parts()
                            .last()
                            .ok_or(anyhow!("Metadata location path is empty."))?;
                        if name.as_ref().ends_with(".metadata.json") {
                            let version: i64 = name
                                .as_ref()
                                .trim_start_matches("v")
                                .trim_end_matches(".metadata.json")
                                .parse()?;
                            if version > acc {
                                Ok(version)
                            } else {
                                Ok(acc)
                            }
                        } else {
                            Ok(acc)
                        }
                    }
                    (Err(err), _) => Err(anyhow!(err.to_string())),
                    (_, Err(err)) => Err(anyhow!(err.to_string())),
                }
            })
            .await?;
        let metadata_location = path.to_string() + "v" + &version.to_string() + ".metadata.json";
        let bytes = &object_store
            .get(&metadata_location.clone().into())
            .await
            .map_err(|err| anyhow!(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        let metadata: TableMetadataV2 = serde_json::from_str(
            std::str::from_utf8(bytes).map_err(|err| anyhow!(err.to_string()))?,
        )
        .map_err(|err| anyhow!(err.to_string()))?;
        Ok(Table {
            metadata,
            table_type: TableType::FileSystem(Arc::clone(object_store)),
            metadata_location,
        })
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
