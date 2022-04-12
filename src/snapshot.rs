use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
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
pub struct Summary {
    operation: Option<Operation>,

    #[serde(flatten)]
    other: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
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
}
