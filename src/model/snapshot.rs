/*!
Details of [snapshots](https://iceberg.apache.org/spec/#snapshots) for a table.

A [SnapshotV2] contains a pointer to the ManifestList as well as supporting data for the Snapshot.

A [Reference] is a named pointer to a [SnapshotV2] stored in the [refs field of the TableMetadataV2](crate::model::table::TableMetadataV2#structfield.refs).
a [Reference] can be a [Tag](Retention#variant.Tag) or [Branch](Retention#variant.Branch).

*/
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
/// The type of operations included in the snapshot, this allows
/// certain snapshots to be skipped during operation.
pub enum Operation {
    /// Only data files were added and no files were removed.
    Append,
    /// Data and delete files were added and removed without changing
    /// table data; i.e., compaction, changing the data file format,
    /// or relocating data files.
    Replace,
    /// Data and delete files were added and removed in a logical
    /// overwrite operation.
    Overwrite,
    /// Data files were removed and their contents logically deleted
    /// and/or delete files were added to delete rows.
    Delete,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Summarises the changes in the snapshot.
pub struct Summary {
    /// The type of operation in the snapshot
    pub operation: Option<Operation>,
    /// Other summary data.
    #[serde(flatten)]
    pub other: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// A V2 compliant snapshot.
pub struct SnapshotV2 {
    /// A unique long ID
    pub snapshot_id: i64,
    /// The snapshot ID of the snapshot’s parent.
    /// Omitted for any snapshot with no parent
    pub parent_snapshot_id: Option<i64>,
    /// A monotonically increasing long that tracks the order of
    /// changes to a table.
    pub sequence_number: i64,
    /// A timestamp when the snapshot was created, used for garbage
    /// collection and table inspection
    pub timestamp_ms: i64,
    /// The location of a manifest list for this snapshot that
    /// tracks manifest files with additional metadata.
    pub manifest_list: String,
    /// A string map that summarizes the snapshot changes, including operation.
    pub summary: Summary,
    /// ID of the table’s current schema when the snapshot was created.
    pub schema_id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// A V1 compliant snapshot.
pub struct SnapshotV1 {
    /// A unique long ID
    pub snapshot_id: i64,
    /// The snapshot ID of the snapshot’s parent.
    /// Omitted for any snapshot with no parent
    pub parent_snapshot_id: Option<i64>,
    /// A timestamp when the snapshot was created, used for garbage
    /// collection and table inspection
    pub timestamp_ms: i64,
    /// The location of a manifest list for this snapshot that
    /// tracks manifest files with additional metadata.
    pub manifest_list: Option<String>,
    /// A list of manifest file locations. Must be omitted if manifest-list is present
    pub manisfests: Option<Vec<String>>,
    /// A string map that summarizes the snapshot changes, including operation.
    pub summary: Option<Summary>,
    /// ID of the table’s current schema when the snapshot was created.
    pub schema_id: Option<i64>,
}

impl From<SnapshotV1> for SnapshotV2 {
    fn from(v1: SnapshotV1) -> Self {
        SnapshotV2 {
            snapshot_id: v1.snapshot_id,
            parent_snapshot_id: v1.parent_snapshot_id,
            sequence_number: 0,
            timestamp_ms: v1.timestamp_ms,
            manifest_list: v1.manifest_list.unwrap_or("".to_owned()),
            summary: v1.summary.unwrap_or(Summary {
                operation: None,
                other: HashMap::new(),
            }),
            schema_id: v1.schema_id,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Iceberg tables keep track of branches and tags using snapshot references.
pub struct Reference {
    /// A reference’s snapshot ID. The tagged snapshot or latest snapshot of a branch.
    pub snapshot_id: i64,
    #[serde(flatten)]
    /// The retention policy for the reference.
    pub retention: Retention,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase", tag = "type")]
/// Retention policy field, which differ based on it it
/// is a Branch or Tag Reference
pub enum Retention {
    #[serde(rename_all = "kebab-case")]
    /// A branch reference
    Branch {
        /// A positive number for the minimum number of snapshots to keep in a
        /// branch while expiring snapshots.
        min_snapshots_to_keep: Option<i32>,
        /// A positive number for the max age of snapshots to keep when expiring,
        /// including the latest snapshot.
        max_snapshot_age_ms: Option<i64>,
        /// A positive number for the max age of the snapshot reference to
        /// keep while expiring snapshots.
        max_ref_age_ms: Option<i64>,
    },
    #[serde(rename_all = "kebab-case")]
    /// A tag reference.
    Tag {
        /// A positive number for the max age of the snapshot reference to
        /// keep while expiring snapshots.
        max_ref_age_ms: i64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_v2() {
        let data = r#"
            {
                "snapshot-id": 3051729675574597004,  
                "sequence-number": 1,
                "timestamp-ms": 1515100955770,  
                "summary": {    "operation": "append"  },  
                "manifest-list": "s3://b/wh/.../s1.avro",
                "schema-id": 0
            } 
        "#;

        let snapshot: SnapshotV2 = serde_json::from_str(&data).unwrap();
        assert_eq!(Some(Operation::Append), snapshot.summary.operation);
        assert!(snapshot.summary.other.is_empty());
    }

    #[test]
    fn test_tag_ref() {
        let data = r#"
            {
                "snapshot-id": 3051729675574597004,
                "type" : "tag",
                "max-ref-age-ms": 1515100955770
            }
        "#;
        let snapshot_ref: Reference = serde_json::from_str(data).unwrap();
        assert!(matches!(snapshot_ref.retention, Retention::Tag { .. }));
    }

    #[test]
    fn test_branch_ref() {
        let data = r#"
            {
                "snapshot-id": 3051729675574597004,
                "type" : "branch",
                "min-snapshots-to-keep": 1,
                "max-snapshot-age-ms": 1515100955770,
                "max-ref-age-ms": 1515100955770
            }
        "#;
        let snapshot_ref: Reference = serde_json::from_str(data).unwrap();
        assert!(matches!(snapshot_ref.retention, Retention::Branch { .. }));
    }

    #[test]
    fn test_retention_branch() {
        let retention = Retention::Branch {
            min_snapshots_to_keep: Some(1),
            max_snapshot_age_ms: Some(1),
            max_ref_age_ms: Some(1),
        };
        let json = serde_json::to_string(&retention).unwrap();
        let result: Retention = serde_json::from_str(&json).unwrap();
        assert!(matches!(result, Retention::Branch { .. }))
    }

    #[test]
    fn test_retention_tag() {
        let retention = Retention::Tag { max_ref_age_ms: 1 };
        let json = serde_json::to_string(&retention).unwrap();
        let result: Retention = serde_json::from_str(&json).unwrap();
        assert!(matches!(result, Retention::Tag { .. }))
    }
}
