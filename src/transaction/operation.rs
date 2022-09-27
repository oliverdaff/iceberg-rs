/*!
 * Defines the different [Operation]s on a [Table].
*/

use anyhow::Result;
use object_store::path::Path;

use crate::{
    model::{
        manifest::{DataFile, FileFormat, ManifestEntry, PartitionValues, Status},
        manifest_list::{Content, FieldSummary, ManifestFile},
        schema::SchemaV2,
    },
    table::Table,
};

///Table operations
pub enum Operation {
    /// Update schema
    UpdateSchema(SchemaV2),
    /// Update spec
    UpdateSpec(i32),
    // /// Update table properties
    // UpdateProperties,
    // /// Replace the sort order
    // ReplaceSortOrder,
    // /// Update the table location
    // UpdateLocation,
    // /// Append new files to the table
    // NewAppend,
    /// Quickly append new files to the table
    NewFastAppend(Vec<String>),
    // /// Replace files in the table and commit
    // NewRewrite,
    // /// Replace manifests files and commit
    // RewriteManifests,
    // /// Replace files in the table by a filter expression
    // NewOverwrite,
    // /// Remove or replace rows in existing data files
    // NewRowDelta,
    // /// Delete files in the table and commit
    // NewDelete,
    // /// Expire snapshots in the table
    // ExpireSnapshots,
    // /// Manage snapshots in the table
    // ManageSnapshots,
    // /// Read and write table data and metadata files
    // IO,
}

impl Operation {
    pub async fn execute(self, table: &mut Table) -> Result<()> {
        match self {
            Operation::NewFastAppend(paths) => {
                let object_store = table.object_store();
                let table_metadata = table.metadata();
                let manifest_list_location: Path = table_metadata
                    .snapshots
                    .as_ref()
                    .map(|snapshots| snapshots.last().unwrap().manifest_list.clone())
                    .unwrap()
                    .into();
                let manifest_schema = apache_avro::Schema::parse_str(&ManifestEntry::schema(
                    &PartitionValues::schema(
                        &table_metadata.default_spec(),
                        table_metadata.current_schema(),
                    )?,
                ))?;
                let mut manifest_writer = apache_avro::Writer::new(&manifest_schema, Vec::new());
                for path in paths {
                    let manifest_entry = ManifestEntry {
                        status: Status::Added,
                        snapshot_id: table_metadata.current_snapshot_id,
                        sequence_number: table_metadata
                            .snapshots
                            .as_ref()
                            .map(|snapshots| snapshots.last().unwrap().sequence_number),
                        data_file: DataFile {
                            content: None,
                            file_path: path,
                            file_format: FileFormat::Parquet,
                            partition: PartitionValues::from_iter(
                                table_metadata
                                    .default_spec()
                                    .fields
                                    .iter()
                                    .map(|field| (field.name.to_owned(), None)),
                            ),
                            record_count: 4,
                            file_size_in_bytes: 1200,
                            block_size_in_bytes: None,
                            file_ordinal: None,
                            sort_columns: None,
                            column_sizes: None,
                            value_counts: None,
                            null_value_counts: None,
                            nan_value_counts: None,
                            distinct_counts: None,
                            lower_bounds: None,
                            upper_bounds: None,
                            key_metadata: None,
                            split_offsets: None,
                            equality_ids: None,
                            sort_order_id: None,
                        },
                    };
                    manifest_writer.append_ser(manifest_entry)?;
                }
                let manifest_bytes = manifest_writer.into_inner()?;
                let manifest_location: Path = (manifest_list_location
                    .to_string()
                    .trim_end_matches(".avro")
                    .to_owned()
                    + "-m0.avro")
                    .into();
                object_store
                    .put(&manifest_location, manifest_bytes.into())
                    .await?;
                let manifest_list_schema = apache_avro::Schema::parse_str(&ManifestFile::schema())?;
                let mut manifest_list_writer =
                    apache_avro::Writer::new(&manifest_list_schema, Vec::new());
                match &table_metadata.snapshots {
                    Some(snapshots) => {
                        if snapshots.len() > 1 {
                            let old_manifest_location: Path =
                                snapshots[snapshots.len() - 2].manifest_list.clone().into();
                            let bytes: Vec<u8> = object_store
                                .get(&old_manifest_location)
                                .await?
                                .bytes()
                                .await?
                                .into();
                            let reader = apache_avro::Reader::new(&*bytes)?;
                            manifest_list_writer.extend(reader.filter_map(Result::ok))?;
                        }
                    }
                    None => (),
                };
                let manifest_file = ManifestFile {
                    manifest_path: manifest_location.to_string(),
                    manifest_length: 1200,
                    partition_spec_id: 0,
                    content: Some(Content::Data),
                    sequence_number: Some(566),
                    min_sequence_number: Some(0),
                    added_snapshot_id: 39487483032,
                    added_files_count: Some(1),
                    existing_files_count: Some(2),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(1000),
                    existing_rows_count: Some(8000),
                    deleted_rows_count: Some(0),
                    partitions: Some(vec![FieldSummary {
                        contains_null: true,
                        contains_nan: Some(false),
                        lower_bound: None,
                        upper_bound: None,
                    }]),
                    key_metadata: None,
                };
                manifest_list_writer.append_ser(manifest_file)?;
                let manifest_list_bytes = manifest_list_writer.into_inner()?;
                let manifest_list_location: Path = table_metadata
                    .snapshots
                    .as_ref()
                    .map(|snapshots| snapshots.last().unwrap().manifest_list.clone())
                    .unwrap()
                    .into();
                object_store
                    .put(&manifest_list_location, manifest_list_bytes.into())
                    .await?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
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
    async fn test_append_files() {
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
            TableBuilder::new_filesystem_table("test/append", schema, Arc::clone(&object_store))
                .unwrap()
                .commit()
                .await
                .unwrap();

        let metadata_location = table.metadata_location();
        assert_eq!(metadata_location, "test/append/metadata/v0.metadata.json");

        let transaction = table.new_transaction();
        transaction
            .fast_append(vec![
                "test/append/data/file1.parquet".to_string(),
                "test/append/data/file2.parquet".to_string(),
            ])
            .commit()
            .await
            .unwrap();
        let metadata_location = table.metadata_location();
        assert_eq!(metadata_location, "test/append/metadata/v1.metadata.json");
    }
}
