/*!
Manifest files
*/
use std::{collections::HashMap, io::Read};

use anyhow::{anyhow, Context, Result};
use apache_avro::{
    types::{Record, Value},
    Reader, Schema,
};
use serde::{de::DeserializeOwned, ser::SerializeSeq, Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_repr::{Deserialize_repr, Serialize_repr};

use super::partition::PartitionField;

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
    /// File path, partition tuple, metrics, …
    pub data_file: DataFile,
}

impl ManifestEntry {
    /// Get schema of manifest entry.
    pub fn schema(partition_schema: &str) -> String {
        let datafile_schema = DataFile::schema(partition_schema);
        r#"{
            "type": "record",
            "name": "manifest_entry",
            "fields": [
                {
                    "name": "status",
                    "type": "int",
                    "field_id": 0
                },
                {
                    "name": "snapshot_id",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field_id": 1
                },
                {
                    "name": "sequence_number",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field_id": 3
                },
                {
                    "name": "data_file",
                    "type": "#
            .to_owned()
            + &datafile_schema
            + r#",
                    "field_id": 2
                }
            ]
        }"#
    }
    /// Turn manifest entry into a record.
    pub fn into_record<'schema>(
        self,
        schema: &'schema Schema,
        partition_spec_name: &str,
    ) -> Result<Record<'schema>> {
        let mut record =
            Record::new(schema).ok_or_else(|| anyhow!("Failed to create Record with schema."))?;
        let value = apache_avro::to_value(self)?;
        if let Value::Record(mut my_manifest_entry) = value {
            my_manifest_entry
                .iter_mut()
                .filter(|x| x.0 == "data_file")
                .for_each(|(_, data_file)| {
                    if let Value::Record(ref mut my_data_file) = data_file {
                        my_data_file
                            .iter_mut()
                            .filter(|y| y.0 == "partition")
                            .for_each(|(_, partition)| {
                                if let Value::Record(ref mut my_record) = partition {
                                    my_record
                                        .iter_mut()
                                        .filter(|z| z.0 == "partition_spec_name")
                                        .for_each(|(name, _)| {
                                            *name = partition_spec_name.to_string();
                                        })
                                }
                            })
                    }
                });
            for (key, value) in my_manifest_entry {
                record.put(&key, value)
            }
            Ok(record)
        } else {
            Err(anyhow!("ManifestEntry is not a record."))
        }
    }
}

impl TryFrom<Value> for ManifestEntry {
    type Error = anyhow::Error;

    fn try_from(mut value: Value) -> Result<Self, Self::Error> {
        if let Value::Record(ref mut my_manifest_entry) = value {
            my_manifest_entry
                .iter_mut()
                .filter(|x| x.0 == "data_file")
                .for_each(|(_, data_file)| {
                    if let Value::Record(ref mut my_data_file) = data_file {
                        my_data_file
                            .iter_mut()
                            .filter(|y| y.0 == "partition")
                            .for_each(|(_, partition)| {
                                if let Value::Record(ref mut my_record) = partition {
                                    if my_record.len() == 1 {
                                        if let Some((name, _)) = my_record.get_mut(0) {
                                            *name = "partition_spec_name".to_string();
                                        }
                                    }
                                }
                            })
                    }
                });
            apache_avro::from_value::<ManifestEntry>(&value).map_err(anyhow::Error::msg)
        } else {
            Err(anyhow!("ManifestEntry is not a record."))
        }
    }
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

#[derive(Debug, PartialEq, Eq, Clone)]
#[repr(u8)]
/// Name of file format
pub enum FileFormat {
    /// Avro file
    Avro = 0,
    /// Orc file
    Orc = 1,
    /// Parquet file
    Parquet = 2,
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixed types.
impl Serialize for FileFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use FileFormat::*;
        match self {
            Avro => serializer.serialize_str("AVRO"),
            Orc => serializer.serialize_str("ORC"),
            Parquet => serializer.serialize_str("PARQUET"),
        }
    }
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixed types.
impl<'de> Deserialize<'de> for FileFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "AVRO" {
            Ok(FileFormat::Avro)
        } else if s == "ORC" {
            Ok(FileFormat::Orc)
        } else if s == "PARQUET" {
            Ok(FileFormat::Parquet)
        } else {
            Err(serde::de::Error::custom("Invalid data file format."))
        }
    }
}

/// The partition struct stores the tuple of partition values for each file.
/// Its type is derived from the partition fields of the partition spec used to write the manifest file.
/// In v2, the partition struct’s field ids must match the ids from the partition spec.
/// The schema of the partition tuples is different for different partition specs.
/// To achieve a constant scheme for the rust struct, this create uses aliasing.
/// But this means that the schema for reading and writing are different.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct PartitionStruct {
    pub(crate) partition_spec_name: Option<i64>,
}

impl PartitionStruct {
    /// Get the schema for reading the PartitionStruct based on the PartitionSpec. It is deduced from the writers schema.
    pub fn read_schema<R: Read>(reader: R) -> Result<String> {
        let reader = Reader::new(reader)?;
        let metadata = reader.user_metadata();
        let partition_specs: Vec<PartitionField> = serde_json::from_slice(
            &metadata
                .get("partition-spec")
                .ok_or_else(|| anyhow!("No partition spec in metadata."))?
                .to_vec(),
        )?;
        let partition_spec_id: i32 = serde_json::from_slice(
            &metadata
                .get("partition-spec-id")
                .ok_or_else(|| anyhow!("No partition spec in metadata."))?
                .to_vec(),
        )?;
        Ok(
            r#"{"type": "record","name": "r102","fields": [{"name": ""#.to_owned()
                + &partition_specs[partition_spec_id as usize].name
                + r#"", "type":  ["null","long"], "aliases": ["partition_spec_name"], "default": null}]}"#,
        )
    }
    /// Get the schema for writing the PartitionStruct
    pub fn write_schema(partition_spec_name: &str) -> String {
        r#"{"type": "record","name": "r102","fields": [{"name": ""#.to_owned()
            + partition_spec_name
            + r#"", "type":  ["null","long"], "aliases": ["partition_spec_name"], "default": null}]}"#
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct KeyValue<T: Serialize + Clone> {
    key: i32,
    value: T,
}

/// Utility struct to convert . Derefences to a Hashmap.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AvroMap<T: Serialize + Clone>(HashMap<i32, T>);

impl<T: Serialize + Clone> core::ops::Deref for AvroMap<T> {
    type Target = HashMap<i32, T>;

    fn deref(self: &'_ AvroMap<T>) -> &'_ Self::Target {
        &self.0
    }
}

impl<T: Serialize + Clone> Serialize for AvroMap<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let entries = self
            .0
            .iter()
            .map(|(key, value)| KeyValue {
                key: *key,
                value: (*value).clone(),
            })
            .collect::<Vec<KeyValue<T>>>();
        let mut seq = serializer.serialize_seq(Some(entries.len()))?;
        for element in entries {
            seq.serialize_element(&element)?;
        }
        seq.end()
    }
}

impl<'de, T: Serialize + DeserializeOwned + Clone> Deserialize<'de> for AvroMap<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let vec: Vec<KeyValue<T>> = Vec::deserialize(deserializer)?;
        Ok(AvroMap(HashMap::from_iter(
            vec.into_iter().map(|x| (x.key, x.value)),
        )))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// DataFile found in Manifest.
pub struct DataFile {
    ///Type of content in data file.
    pub content: Option<Content>,
    /// Full URI for the file with a FS scheme.
    pub file_path: String,
    /// String file format name, avro, orc or parquet
    pub file_format: FileFormat,
    /// Partition data tuple, schema based on the partition spec output using partition field ids for the struct field ids
    pub partition: PartitionStruct,
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
    pub column_sizes: Option<AvroMap<i64>>,
    /// Map from column id to number of values in the column (including null and NaN values)
    pub value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of null values
    pub null_value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of NaN values
    pub nan_value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of distinct values in the column.
    pub distinct_counts: Option<AvroMap<i64>>,
    /// Map from column id to lower bound in the column
    pub lower_bounds: Option<AvroMap<ByteBuf>>,
    /// Map from column id to upper bound in the column
    pub upper_bounds: Option<AvroMap<ByteBuf>>,
    /// Implementation specific key metadata for encryption
    pub key_metadata: Option<ByteBuf>,
    /// Split offsets for the data file.
    pub split_offsets: Option<Vec<i64>>,
    /// Field ids used to determine row equality in equality delete files.
    pub equality_ids: Option<Vec<i32>>,
    /// ID representing sort order for this file
    pub sort_order_id: Option<i32>,
}

impl DataFile {
    /// Get schema
    pub fn schema(partition_schema: &str) -> String {
        r#"{
            "type": "record",
            "name": "r2",
            "fields": [
                {
                    "name": "content",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field_id": 134
                },
                {
                    "name": "file_path",
                    "type": "string",
                    "field_id": 100
                },
                {
                    "name": "file_format",
                    "type": "string",
                    "field_id": 101
                },
                {
                    "name": "partition",
                    "type": "#
            .to_owned()
            + partition_schema
            + r#",
                    "field_id": 102
                },
                {
                    "name": "record_count",
                    "type": "long",
                    "field_id": 103
                },
                {
                    "name": "file_size_in_bytes",
                    "type": "long",
                    "field_id": 104
                },
                {
                    "name": "block_size_in_bytes",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field_id": 105
                },
                {
                    "name": "file_ordinal",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field_id": 106
                },
                {
                    "name": "sort_columns",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": "int",
                            "element-id": 112
                        }
                    ],
                    "default": null,
                    "field_id": 107
                },
                {
                    "name": "column_sizes",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k117_v118",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 117
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 118
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 108
                },
                {
                    "name": "value_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k119_v120",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 119
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 120
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 109
                },
                {
                    "name": "null_value_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k121_v122",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 121
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 122
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 110
                },
                {
                    "name": "nan_value_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k138_v139",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 138
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 139
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 137
                },
                {
                    "name": "distinct_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k123_v124",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 123
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 124
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 111
                },
                {
                    "name": "lower_bounds",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k126_v127",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 126
                                    },
                                    {
                                        "name": "value",
                                        "type": "bytes",
                                        "field-id": 127
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 125
                },
                {
                    "name": "upper_bounds",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k129_v130",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 129
                                    },
                                    {
                                        "name": "value",
                                        "type": "bytes",
                                        "field-id": 130
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 128
                },
                {
                    "name": "key_metadata",
                    "type": [
                        "null",
                        "bytes"
                    ],
                    "default": null,
                    "field_id": 131
                },
                {
                    "name": "split_offsets",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": "long",
                            "element-id": 133
                        }
                    ],
                    "default": null,
                    "field_id": 132
                },
                {
                    "name": "equality_ids",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": "int",
                            "element-id": 136
                        }
                    ],
                    "default": null,
                    "field_id": 135
                },
                {
                    "name": "sort_order_id",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field_id": 140
                }
            ]
        }"#
    }
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
    record.try_into()
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
                sequence_number,
                data_file: DataFile {
                    content: None,
                    file_path: "/".to_string(),
                    file_format: FileFormat::Parquet,
                    partition: PartitionStruct {
                        partition_spec_name: Some(0)
                    },
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
                }
            }
        }
    }

    proptest! {
            #[test]
            fn test_manifest_entry(a in arb_manifest_entry()) {
                let partition_schema = PartitionStruct::write_schema("date");

                let raw_schema = ManifestEntry::schema(&partition_schema);

                let schema = apache_avro::Schema::parse_str(&raw_schema).unwrap();

                // TODO: make this a correct partition spec
                let partition_spec = r#"[{
                    "source-id": 4,
                    "field-id": 1000,
                    "name": "date",
                    "transform": "day"
                  }]"#;
            let partition_spec_id = "0";
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
            writer.append(a.clone().into_record(&schema,"date").unwrap()).unwrap();

                let encoded = writer.into_inner().unwrap();

                let reader = apache_avro::Reader::new( &encoded[..]).unwrap();

                for value in reader {
                    let entry = ManifestEntry::try_from(value.unwrap()).unwrap();
                    assert_eq!(a, entry)
                }

            }
            #[test]
            fn test_read_manifest(a in arb_manifest_entry()) {
            let partition_schema = PartitionStruct::write_schema("date");

            let raw_schema = ManifestEntry::schema(&partition_schema);

            let schema = apache_avro::Schema::parse_str(&raw_schema).unwrap();

            // TODO: make this a correct partition spec
            let partition_spec = r#"[{
                "source-id": 4,
                "field-id": 1000,
                "name": "date",
                "transform": "day"
              }]"#;
            let partition_spec_id = "0";
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
            writer.append(a.clone().into_record(&schema,"date").unwrap()).unwrap();

            let encoded = writer.into_inner().unwrap();

            let reader = apache_avro::Reader::new( &encoded[..]).unwrap();
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

            let partition_schema = PartitionStruct::write_schema("date");

            let raw_schema = ManifestEntry::schema(&partition_schema);

            let schema = apache_avro::Schema::parse_str(&raw_schema).unwrap();

            // TODO: make this a correct partition spec
            let partition_spec = r#"[{
                "source-id": 4,
                "field-id": 1000,
                "name": "date",
                "transform": "day"
              }]"#;
            let partition_spec_id = "0";
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
            writer.append(a.clone().into_record(&schema,"date").unwrap()).unwrap();

            let encoded = writer.into_inner().unwrap();

            let mut reader = apache_avro::Reader::new( &encoded[..]).unwrap();
            let metadata_entry = read_manifest_entry(&mut reader).unwrap();
            assert_eq!(a.status, metadata_entry.status);
            assert_eq!(a.snapshot_id, metadata_entry.snapshot_id);
            assert_eq!(a.sequence_number, metadata_entry.sequence_number);
            assert_eq!(a.data_file.partition, metadata_entry.data_file.partition);
    }

    }
}
