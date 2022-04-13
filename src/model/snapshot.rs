/*!
Details of snapshots for a table.
!*/
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
    operation: Option<Operation>,

    #[serde(flatten)]
    other: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// A V2 compliant snapshot.
pub struct SnapshotV2 {
    /// A unique long ID
    snapshot_id: i64,
    /// The snapshot ID of the snapshot’s parent.
    /// Omitted for any snapshot with no parent
    parent_snapshot_id: Option<i64>,
    /// A monotonically increasing long that tracks the order of
    /// changes to a table.
    sequence_number: i64,
    /// A timestamp when the snapshot was created, used for garbage
    /// collection and table inspection
    timestamp_ms: i64,
    /// The location of a manifest list for this snapshot that
    /// tracks manifest files with additional meadata.
    manifest_list: String,
    /// A string map that summarizes the snapshot changes, including operation.
    summary: Summary,
    /// ID of the table’s current schema when the snapshot was created.
    schema_id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Iceberg tables keep track of branches and tags using snapshot references.
pub struct Reference {
    /// A reference’s snapshot ID. The tagged snapshot or latest snapshot of a branch.
    snapshot_id: i64,
    #[serde(flatten)]
    retention: Retention,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "type")]
/// Retention policy field, which differ based on it it
/// is a Branch or Tag Reference
pub enum Retention {
    #[serde(rename_all = "kebab-case")]
    /// A branch reference
    Branch {
        /// A positive number for the minimum number of snapshots to keep in a
        /// branch while expiring snapshots.
        min_snapshots_to_keep: i32,
        /// A positive number for the max age of snapshots to keep when expiring,
        /// including the latest snapshot.
        max_snapshot_age_ms: i64,
        /// A positive number for the max age of the snapshot reference to
        /// keep while expiring snapshots.
        max_ref_age_ms: i64,
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
}
