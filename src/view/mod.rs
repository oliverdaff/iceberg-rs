/*!
 * Defines the [View] struct that represents an iceberg view.
*/

use anyhow::{anyhow, Result};
use futures::StreamExt;

use std::sync::Arc;

use object_store::{path::Path, ObjectStore};

use crate::{
    catalog::{identifier::Identifier, Catalog},
    model::{schema::SchemaStruct, view_metadata::ViewMetadata},
    table::TableType,
};

use self::transaction::Transaction as ViewTransaction;

pub mod transaction;

/// An iceberg view
pub struct View {
    /// Type of the View, either filesystem or metastore.
    table_type: TableType,
    /// Metadata for the iceberg view according to the iceberg view spec
    metadata: ViewMetadata,
    /// Path to the current metadata location
    metadata_location: String,
}

/// Public interface of the table.
impl View {
    /// Create a new metastore view
    pub async fn new_metastore_view(
        identifier: Identifier,
        catalog: Arc<dyn Catalog>,
        metadata: ViewMetadata,
        metadata_location: &str,
    ) -> Result<Self> {
        Ok(View {
            table_type: TableType::Metastore(identifier, catalog),
            metadata,
            metadata_location: metadata_location.to_string(),
        })
    }
    /// Load a filesystem view from an objectstore
    pub async fn load_file_system_view(
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
                            .ok_or_else(|| anyhow!("Metadata location path is empty."))?;
                        if name.as_ref().ends_with(".metadata.json") {
                            let version: i64 = name
                                .as_ref()
                                .trim_start_matches('v')
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
        let metadata_location = path.to_string() + "/v" + &version.to_string() + ".metadata.json";
        let bytes = &object_store
            .get(&metadata_location.clone().into())
            .await
            .map_err(|err| anyhow!(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| anyhow!(err.to_string()))?;
        let metadata: ViewMetadata = serde_json::from_str(
            std::str::from_utf8(bytes).map_err(|err| anyhow!(err.to_string()))?,
        )
        .map_err(|err| anyhow!(err.to_string()))?;
        Ok(View {
            metadata,
            table_type: TableType::FileSystem(Arc::clone(object_store)),
            metadata_location,
        })
    }
    /// Get the table identifier in the catalog. Returns None of it is a filesystem table.
    pub fn identifier(&self) -> Option<&Identifier> {
        match &self.table_type {
            TableType::FileSystem(_) => None,
            TableType::Metastore(identifier, _) => Some(identifier),
        }
    }
    /// Get the catalog associated to the table. Returns None if the table is a filesystem table
    pub fn catalog(&self) -> Option<&Arc<dyn Catalog>> {
        match &self.table_type {
            TableType::FileSystem(_) => None,
            TableType::Metastore(_, catalog) => Some(catalog),
        }
    }
    /// Get the object_store associated to the table
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        match &self.table_type {
            TableType::FileSystem(object_store) => Arc::clone(object_store),
            TableType::Metastore(_, catalog) => catalog.object_store(),
        }
    }
    /// Get the metadata of the table
    pub fn schema(&self) -> Option<&SchemaStruct> {
        self.metadata.current_schema()
    }
    /// Get the metadata of the table
    pub fn metadata(&self) -> &ViewMetadata {
        &self.metadata
    }
    /// Get the location of the current metadata file
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }
    /// Create a new transaction for this table
    pub fn new_transaction(&mut self) -> ViewTransaction {
        ViewTransaction::new(self)
    }
}

/// Private interface of the view.
impl View {
    /// Increment the version number of the view. Is typically used when commiting a new view transaction.
    pub(crate) fn increment_version_number(&mut self) {
        match &mut self.metadata {
            ViewMetadata::V1(metadata) => {
                metadata.current_version_id += 1;
            }
        }
    }
}
