/*!
Manifest files
*/
use std::collections::HashMap;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

/// Details of a manifest file
pub struct Manifest {
    /// The manifest metadata
    pub metadata: Metadata,
    /// The manifest entry
    pub entry: ManifestEntry,
}

/// Lists data files or delete files, along with each file’s
/// partition data tuple, metrics, and tracking information.
/// Should this be called metadata?
pub struct Metadata {
    /// JSON representation of the table schema at the time the manifest was written
    /// Should this be Typed?
    pub schema: String,
    /// ID of the schema used to write the manifest as a string
    /// Should this be typed into a
    pub schema_id: Option<String>,
    /// JSON fields representation of the partition spec used to write the manifest
    pub partition_spec: Option<String>,
    /// ID of the partition spec used to write the manifest as a string
    pub partition_spec_id: Option<String>,
    /// Table format version number of the manifest as a string
    pub format_version: Option<String>,
    /// Type of content files tracked by the manifest: “data” or “deletes”
    pub content: Option<String>,
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone)]
#[repr(u8)]
/// Used to track additions and deletions
pub enum Status {
    /// Existing files
    Existing = 0,
    /// Added files
    Added = 1,
    /// Deleted files
    Deleted = 2,
}

/// Entry in manifest.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct ManifestEntry {
    /// Used to track additions and deletions
    pub status: Status,
    /// Snapshot id where the file was added, or deleted if status is 2.
    /// Inherited when null.
    pub snapshot_id: Option<i64>,
    /// Sequence number when the file was added. Inherited when null.
    pub sequence_number: Option<i64>,
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone)]
#[repr(u8)]
/// Type of content stored by the data file.
pub enum Content {
    /// Data.
    Data = 0,
    /// Deletes at position.
    PositionDeletes = 1,
    /// Delete by equality.
    EqualityDeletes = 2,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// Name of file format
pub enum FileFormat {
    /// Avro file
    Avro,
    /// Orc file
    Orc,
    /// Parquet file
    Parquet,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// DataFile found in Manifest.
pub struct DataFile {
    ///Type of content in data file.
    pub content: Option<Content>,
    /// Full URI for the file with a FS scheme.
    pub file_path: String,

    // TODO: Partition Data
    /// Number of records in this file
    pub record_count: i64,
    /// Total file size in bytes
    pub file_size_in_bytes: i64,
    /// Block size
    pub block_size_in_bytes: Option<i64>,
    /// File ordinal
    pub file_ordinal: Option<i32>,
    /// Columns to sort
    pub sort_columns: Option<Vec<i32>>,
    /// Map from column id to total size on disk
    pub column_sizes: Option<HashMap<i32, i64>>,
    /// Map from column id to number of null values
    pub null_value_counts: Option<HashMap<i32, i64>>,
    /// Map from column id to number of NaN values
    pub nan_value_counts: Option<HashMap<i32, i64>>,
    /// Map from column id to number of distinct values in the column.
    pub distinct_counts: Option<HashMap<i32, i64>>,
    /// Map from column id to lower bound in the column
    pub lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// Map from column id to upper bound in the column
    pub upper_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// Implementation specific key metadata for encryption
    pub key_metadata: Option<Vec<u8>>,
    /// Split offsets for the data file.
    pub split_offsets: Option<Vec<i64>>,
    /// Field ids used to determine row equality in equality delete files.
    pub equality_ids: Option<Vec<i32>>,
    /// ID representing sort order for this file
    pub sort_order_id: Option<i32>,
}

/// Read a manifest
pub fn read_manifest<R: std::io::Read>(r: R) -> Result<Manifest> {
    let mut reader = apache_avro::Reader::new(r)?;

    let metadata = read_metadata(&reader)?;
    let entry = read_manifest_entry(&mut reader)?;
    Ok(Manifest { metadata, entry })
}

/// Read metadata from the avro reader
fn read_metadata<R: std::io::Read>(reader: &apache_avro::Reader<R>) -> Result<Metadata> {
    let read_string = |key: &str| {
        reader
            .user_metadata()
            .get(key)
            .map(|id| String::from_utf8(id.to_vec()).map_err(anyhow::Error::from))
            .transpose()
    };

    let schema = read_string("schema")?.context("Metadata must have table schema")?;
    let schema_id = read_string("schema-id")?;
    let partition_spec = read_string("partition-spec")?;
    let partition_spec_id = read_string("partition-spec-id")?;
    let format_version = read_string("format-version")?;
    let content = read_string("content")?;
    Ok(Metadata {
        schema,
        schema_id,
        partition_spec,
        partition_spec_id,
        format_version,
        content,
    })
}

fn read_manifest_entry<R: std::io::Read>(
    reader: &mut apache_avro::Reader<R>,
) -> Result<ManifestEntry> {
    let record = reader
        .into_iter()
        .next()
        .context("Manifest Entry Expected")??;
    if let apache_avro::types::Value::Record(values) = record {
        let values: HashMap<String, apache_avro::types::Value> =
            HashMap::from_iter(values.into_iter());
        let status = values.get("status").context("status not found")?;
        let snapshot_id = values
            .get("snapshot_id")
            .map(apache_avro::from_value)
            .transpose()?;
        let sequence_number = values
            .get("sequence_number")
            .map(apache_avro::from_value)
            .transpose()?;
        Ok(ManifestEntry {
            status: apache_avro::from_value(status)?,
            snapshot_id: snapshot_id.flatten(),
            sequence_number: sequence_number.flatten(),
        })
    } else {
        anyhow::bail!("Avro record expected")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{self, types::Value};
    use proptest::prelude::*;

    fn status_strategy() -> impl Strategy<Value = Status> {
        prop_oneof![
            Just(Status::Existing),
            Just(Status::Added),
            Just(Status::Deleted),
        ]
    }

    prop_compose! {
        fn arb_manifest_entry()(status in status_strategy(),
            snapshot_id in prop::option::of(any::<i64>()),
            sequence_number in prop::option::of(any::<i64>())
        )  -> ManifestEntry{
            ManifestEntry{
                status,
                snapshot_id,
                sequence_number
            }
        }
    }

    proptest! {
            #[test]
            fn test_manifest_entry(a in arb_manifest_entry()) {
                let raw_schema = r#"
            {
                "type": "record",
                "name": "manifest_entry",
                "fields": [
                    {"name": "status", "type": "int"},
                    {"name": "snapshot_id", "type": ["null", "long"], "default":  "null"},
                    {"name": "sequence_number", "type": ["null", "long"], "default":  "null"}
                ]
            } 
            "#;
                let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();
                let mut writer = apache_avro::Writer::new(&schema, Vec::new());
                writer.append_ser(&a).unwrap();

                let encoded = writer.into_inner().unwrap();

                let reader = apache_avro::Reader::new(&encoded[..]).unwrap();
                for value in reader {
                    let entry = apache_avro::from_value::<ManifestEntry>(&value.unwrap()).unwrap();
                    assert_eq!(a, entry)
                }

            }
            #[test]
            fn test_read_manifest(a in arb_manifest_entry()) {
            let raw_schema = r#"
            {
                "type": "record",
                "name": "manifest_entry",
                "fields": [
                    {"name": "status", "type": "int"},
                    {"name": "snapshot_id", "type": ["null", "long"], "default":  "null"},
                    {"name": "sequence_number", "type": ["null", "long"], "default":  "null"}
                ]
            } 
            "#;
            let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();
            // TODO: make this a correct partition spec
            let partition_spec = r#"{"spec-id": "0"}"#;
            let partition_spec_id = "2";
            // TODO: make this a correct schema
            let table_schema = r#"{"schema": "0"}"#;
            let table_schema_id = "1";
            let format_version = "1";
            let content = "data";

            let meta: std::collections::HashMap<String, apache_avro::types::Value> =
                std::collections::HashMap::from_iter(vec![
                    ("schema".to_string(), Value::Bytes(table_schema.into())),
                    ("schema-id".to_string(), Value::Bytes(table_schema_id.into())),
                    ("partition-spec".to_string(), Value::Bytes(partition_spec.into())),
                    ("partition-spec-id".to_string(), Value::Bytes(partition_spec_id.into())),
                    ("format-version".to_string(), Value::Bytes(format_version.into())),
                    ("content".to_string(), Value::Bytes(content.into()))
                    ],
                );
            let mut writer = apache_avro::Writer::builder()
            .schema(&schema)
            .writer(vec![])
            .user_metadata(meta)
            .build();
            writer.append_ser(&a).unwrap();

            let encoded = writer.into_inner().unwrap();
            let reader = apache_avro::Reader::new(&encoded[..]).unwrap();
            let metadata = read_metadata(&reader).unwrap();
            assert_eq!(metadata.schema, table_schema.to_string());
            assert_eq!(metadata.schema_id, Some(table_schema_id.to_string()));
            assert_eq!(metadata.partition_spec, Some(partition_spec.to_string()));
            assert_eq!(metadata.partition_spec_id, Some(partition_spec_id.to_string()));
            assert_eq!(metadata.format_version, Some(format_version.to_string()));
            assert_eq!(metadata.content, Some(content.to_string()));
        }
    #[test]
    fn test_read_manifest_entry(a in arb_manifest_entry()) {
            let raw_schema = r#"
            {
                "type": "record",
                "name": "manifest_entry",
                "fields": [
                    {"name": "status", "type": "int"},
                    {"name": "snapshot_id", "type": ["null", "long"], "default":  "null"},
                    {"name": "sequence_number", "type": ["null", "long"], "default":  "null"}
                ]
            }
            "#;
            let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();
           let mut writer = apache_avro::Writer::builder()
            .schema(&schema)
            .writer(vec![])
            .build();
            writer.append_ser(&a).unwrap();

            let encoded = writer.into_inner().unwrap();
            let mut reader = apache_avro::Reader::new(&encoded[..]).unwrap();
            let metadata_entry = read_manifest_entry(&mut reader).unwrap();
            assert_eq!(a.status, metadata_entry.status);
            assert_eq!(a.snapshot_id, metadata_entry.snapshot_id);
            assert_eq!(a.sequence_number, metadata_entry.sequence_number);
    }

    }
}
