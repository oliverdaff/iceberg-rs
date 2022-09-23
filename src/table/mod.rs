/*!
Defining the [Table] struct that represents an iceberg table.
*/

use std::{collections::HashMap, io::Cursor, sync::Arc, time::SystemTime};

use anyhow::{anyhow, Result};
use apache_avro::types::Value;
use futures::StreamExt;
use object_store::{path::Path, ObjectStore};

use crate::{
    catalog::{table_identifier::TableIdentifier, Catalog},
    model::{
        manifest_list::ManifestFile,
        schema::SchemaV2,
        snapshot::{SnapshotV2, Summary},
        table::TableMetadataV2,
    },
    transaction::Transaction,
};

pub mod files;
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
    manifests: Vec<ManifestFile>,
}

/// Public interface of the table.
impl Table {
    /// Create a new metastore Table
    pub async fn new_metastore_table(
        identifier: TableIdentifier,
        catalog: Arc<dyn Catalog>,
        metadata: TableMetadataV2,
        metadata_location: &str,
    ) -> Result<Self> {
        let manifests = get_manifests(&metadata, catalog.object_store())
            .await?
            .collect::<Result<Vec<_>>>()?;
        Ok(Table {
            table_type: TableType::Metastore(identifier, catalog),
            metadata,
            metadata_location: metadata_location.to_string(),
            manifests,
        })
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
        let metadata: TableMetadataV2 = serde_json::from_str(
            std::str::from_utf8(bytes).map_err(|err| anyhow!(err.to_string()))?,
        )
        .map_err(|err| anyhow!(err.to_string()))?;
        let manifests = get_manifests(&metadata, Arc::clone(object_store))
            .await?
            .collect::<Result<Vec<_>>>()?;
        Ok(Table {
            metadata,
            table_type: TableType::FileSystem(Arc::clone(object_store)),
            metadata_location,
            manifests,
        })
    }
    /// Get the table identifier in the catalog. Returns None of it is a filesystem table.
    pub fn identifier(&self) -> Option<&TableIdentifier> {
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
    pub fn schema(&self) -> &SchemaV2 {
        &self
            .metadata
            .schemas
            .iter()
            .filter(|schema| schema.schema_id == self.metadata.current_schema_id)
            .next()
            .unwrap()
    }
    /// Get the metadata of the table
    pub fn metadata(&self) -> &TableMetadataV2 {
        &self.metadata
    }
    /// Get the location of the current metadata file
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }
    /// Get the location of the current metadata file
    pub fn manifests(&self) -> &[ManifestFile] {
        &self.manifests
    }
    /// Create a new transaction for this table
    pub fn new_transaction(&mut self) -> Transaction {
        Transaction::new(self)
    }
}

/// Private interface of the table.
impl Table {
    pub(crate) fn increment_sequence_number(&mut self) {
        self.metadata.last_sequence_number += 1;
    }

    pub(crate) fn new_snapshot(&mut self) {
        let mut bytes: [u8; 8] = [0u8; 8];
        getrandom::getrandom(&mut bytes).unwrap();
        let snapshot_id = i64::from_le_bytes(bytes);
        let snapshot = SnapshotV2 {
            snapshot_id: snapshot_id,
            parent_snapshot_id: self.metadata().current_snapshot_id,
            sequence_number: self.metadata().last_sequence_number + 1,
            timestamp_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            manifest_list: self.metadata().location.to_string()
                + "/metadata/snap-"
                + &snapshot_id.to_string()
                + &uuid::Uuid::new_v4().to_string()
                + ".avro",
            summary: Summary {
                operation: None,
                other: HashMap::new(),
            },
            schema_id: Some(self.metadata().current_schema_id as i64),
        };
        if let Some(snapshots) = &mut self.metadata.snapshots {
            snapshots.push(snapshot);
            self.metadata.current_snapshot_id = Some(snapshots.len() as i64)
        } else {
            self.metadata.snapshots = Some(vec![snapshot]);
            self.metadata.current_snapshot_id = Some(0i64)
        }
    }
}

pub(crate) async fn get_manifests(
    metadata: &TableMetadataV2,
    object_store: Arc<dyn ObjectStore>,
) -> Result<impl Iterator<Item = Result<ManifestFile>>> {
    let snapshot = if let Some(snapshots) = &metadata.snapshots {
        Ok(snapshots.last().unwrap())
    } else {
        Err(anyhow!("No snapshots in this table."))
    }?;
    let path: Path = snapshot.manifest_list.clone().into();
    {
        let bytes: Cursor<Vec<u8>> = Cursor::new(
            object_store
                .get(&path)
                .await
                .map_err(anyhow::Error::msg)?
                .bytes()
                .await?
                .into(),
        );
        let reader = apache_avro::Reader::new(bytes)?;
        let map = reader.map(
            avro_value_to_manifest_file
                as fn(Result<Value, apache_avro::Error>) -> Result<ManifestFile, anyhow::Error>,
        );
        Ok(map)
    }
}

fn avro_value_to_manifest_file(
    entry: Result<Value, apache_avro::Error>,
) -> Result<ManifestFile, anyhow::Error> {
    entry
        .and_then(|value| apache_avro::from_value(&value))
        .map_err(anyhow::Error::msg)
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use object_store::{memory::InMemory, ObjectStore};

    use crate::{
        model::schema::{AllType, PrimitiveType, SchemaV2, Struct, StructField},
        table::table_builder::TableBuilder,
    };

    #[tokio::test]
    async fn test_increment_sequence_number() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = SchemaV2 {
            schema_id: 1,
            identifier_field_ids: Some(vec![1, 2]),
            name_mapping: None,
            struct_fields: Struct {
                fields: vec![
                    StructField {
                        id: 1,
                        name: "one".to_string(),
                        required: false,
                        field_type: AllType::Primitive(PrimitiveType::String),
                        doc: None,
                    },
                    StructField {
                        id: 2,
                        name: "two".to_string(),
                        required: false,
                        field_type: AllType::Primitive(PrimitiveType::String),
                        doc: None,
                    },
                ],
            },
        };
        let mut table =
            TableBuilder::new_filesystem_table("test/table1", schema, Arc::clone(&object_store))
                .unwrap()
                .commit()
                .await
                .unwrap();

        let metadata_location = table.metadata_location();
        assert_eq!(metadata_location, "test/table1/metadata/v0.metadata.json");

        let transaction = table.new_transaction();
        transaction.commit().await.unwrap();
        let metadata_location = table.metadata_location();
        assert_eq!(metadata_location, "test/table1/metadata/v1.metadata.json");
    }
}
