/*!
 * A Struct for the view metadata   
*/

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model::schema::SchemaStruct;

/// Metadata of an iceberg view
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ViewMetadata {
    /// Version 1 of the table metadata
    V1(ViewMetadataV1),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "format-version")]
/// Fields for the version 1 of the view metadata.
pub struct ViewMetadataV1 {
    /// The view’s base location. This is used to determine where to store manifest files and view metadata files.
    location: String,
    ///	Current version of the view. Set to ‘1’ when the view is first created.
    current_version_id: i64,
    /// An array of structs describing the last known versions of the view. Controlled by the table property: “version.history.num-entries”. See section Versions.
    versions: Vec<Version>,
    /// A list of timestamp and version ID pairs that encodes changes to the current version for the view.
    /// Each time the current-version-id is changed, a new entry should be added with the last-updated-ms and the new current-version-id.
    version_log: Vec<VersionLogStruct>,
    /// A string to string map of view properties. This is used for metadata such as “comment” and for settings that affect view maintenance.
    /// This is not intended to be used for arbitrary metadata.
    properties: Option<HashMap<String, String>>,
    ///	A list of schemas, the same as the ‘schemas’ field from Iceberg table spec.
    schemas: Option<Vec<SchemaStruct>>,
    ///	ID of the current schema of the view
    current_schema_id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "format-version")]
/// Fields for the version 2 of the view metadata.
pub struct Version {
    /// Monotonically increasing id indicating the version of the view. Starts with 1.
    version_id: i64,
    ///	Timestamp expressed in ms since epoch at which the version of the view was created.
    timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "format-version")]
/// Fields for the version 2 of the view metadata.
pub struct VersionLogStruct {
    ///	The timestamp when the referenced version was made the current version
    timestamp_ms: i64,
    /// Version id of the view
    version_id: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "format-version")]
/// Fields for the version 2 of the view metadata.
pub struct Summary {
    /// A string value indicating the view operation that caused this metadata to be created. Allowed values are “create” and “replace”.
    operation: String,
    /// A string value indicating the version of the engine that performed the operation
    engine_version: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[repr(u8)]
/// Name of file format
pub enum RepresentationType {
    /// Avro file
    Sql = 0,
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixed types.
impl Serialize for RepresentationType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use RepresentationType::*;
        match self {
            Sql => serializer.serialize_str("sql"),
        }
    }
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixed types.
impl<'de> Deserialize<'de> for RepresentationType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "sql" {
            Ok(RepresentationType::Sql)
        } else {
            Err(serde::de::Error::custom("Invalid data file format."))
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "format-version")]
/// Fields for the version 2 of the view metadata.
pub struct Representation {
    /// A string indicating the type of representation. It is set to “sql” for this type.
    r#type: RepresentationType,
    /// A string representing the original view definition in SQL
    sql: String,
    /// A string specifying the dialect of the ‘sql’ field. It can be used by the engines to detect the SQL dialect.
    dialect: String,
    /// ID of the view’s schema when the version was created
    schema_id: Option<i64>,
    /// A string specifying the catalog to use when the table or view references in the view definition do not contain an explicit catalog.
    default_catalog: Option<String>,
    /// The namespace to use when the table or view references in the view definition do not contain an explicit namespace.
    /// Since the namespace may contain multiple parts, it is serialized as a list of strings.
    default_namespace: Option<Vec<String>>,
    /// A list of strings of field aliases optionally specified in the create view statement.
    /// The list should have the same length as the schema’s top level fields. See the example below.
    field_aliases: Option<Vec<String>>,
    /// A list of strings of field comments optionally specified in the create view statement.
    /// The list should have the same length as the schema’s top level fields. See the example below.
    field_docs: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {

    use anyhow::Result;

    use crate::view_spec::view_metadata::ViewMetadata;

    #[test]
    fn test_deserialize_view_data_v1() -> Result<()> {
        let data = r#"
        {
            "format-version" : 1,
            "location" : "s3n://my_company/my/warehouse/anorwood.db/common_view",
            "current-version-id" : 1,
            "properties" : { 
              "comment" : "View captures all the data from the table"
            },
            "versions" : [ {
              "version-id" : 1,
              "parent-version-id" : -1,
              "timestamp-ms" : 1573518431292,
              "summary" : {
                "operation" : "create",
                "engineVersion" : "presto-350"
              },
              "representations" : [ {
                "type" : "sql",
                "sql" : "SELECT *\nFROM\n  base_tab\n",
                "dialect" : "presto",
                "schema-id" : 1,
                "default-catalog" : "iceberg",
                "default-namespace" : [ "anorwood" ]
              } ]
            } ],
            "version-log" : [ {
              "timestamp-ms" : 1573518431292,
              "version-id" : 1
            } ],
            "schemas": [ {
              "schema-id": 1,
              "type" : "struct",
              "fields" : [ {
                "id" : 0,
                "name" : "c1",
                "required" : false,
                "type" : "int",
                "doc" : ""
              }, {
                "id" : 1,
                "name" : "c2",
                "required" : false,
                "type" : "string",
                "doc" : ""
              } ]
            } ],
            "current-schema-id": 1
          }
        "#;
        let metadata =
            serde_json::from_str::<ViewMetadata>(&data).expect("Failed to deserialize json");
        //test serialise deserialise works.
        let metadata_two: ViewMetadata = serde_json::from_str(
            &serde_json::to_string(&metadata).expect("Failed to serialize metadata"),
        )
        .expect("Failed to serialize json");
        dbg!(&metadata, &metadata_two);
        assert_eq!(metadata, metadata_two);

        Ok(())
    }
}
