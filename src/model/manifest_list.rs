/*!
 * Manifest lists
*/

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_repr::{Deserialize_repr, Serialize_repr};

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
/// DataFile found in Manifest.
pub struct FieldSummary {
    /// Whether the manifest contains at least one partition with a null value for the field
    contains_null: bool,
    /// Whether the manifest contains at least one partition with a NaN value for the field
    contains_nan: Option<bool>,
    /// Lower bound for the non-null, non-NaN values in the partition field, or null if all values are null or NaN.
    /// If -0.0 is a value of the partition field, the lower_bound must not be +0.0
    lower_bound: Option<ByteBuf>,
    /// Upper bound for the non-null, non-NaN values in the partition field, or null if all values are null or NaN .
    /// If +0.0 is a value of the partition field, the upper_bound must not be -0.0.
    upper_bound: Option<ByteBuf>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// A manifest list includes summary metadata that can be used to avoid scanning all of the manifests in a snapshot when planning a table scan.
/// This includes the number of added, existing, and deleted files, and a summary of values for each field of the partition spec used to write the manifest.
pub struct ManifestFile {
    /// Location of the manifest file
    manifest_path: String,
    /// Length of the manifest file in bytes
    manifest_length: i64,
    /// ID of a partition spec used to write the manifest; must be listed in table metadata partition-specs
    partition_spec_id: i32,
    /// The type of files tracked by the manifest, either data or delete files; 0 for all v1 manifests
    content: Option<Content>,
    /// The sequence number when the manifest was added to the table; use 0 when reading v1 manifest lists
    sequence_number: Option<i64>,
    /// The minimum sequence number of all data or delete files in the manifest; use 0 when reading v1 manifest lists
    min_sequence_number: Option<i64>,
    /// ID of the snapshot where the manifest file was added
    added_snapshot_id: i64,
    /// Number of entries in the manifest that have status ADDED (1), when null this is assumed to be non-zero
    added_files_count: i32,
    /// Number of entries in the manifest that have status EXISTING (0), when null this is assumed to be non-zero
    existing_files_count: Option<i32>,
    /// Number of entries in the manifest that have status DELETED (2), when null this is assumed to be non-zero
    deleted_files_count: Option<i32>,
    /// Number of rows in all of files in the manifest that have status ADDED, when null this is assumed to be non-zero
    added_rows_count: Option<i64>,
    /// Number of rows in all of files in the manifest that have status EXISTING, when null this is assumed to be non-zero
    existing_rows_count: Option<i64>,
    /// Number of rows in all of files in the manifest that have status DELETED, when null this is assumed to be non-zero
    deleted_rows_count: Option<i64>,
    /// A list of field summaries for each partition field in the spec. Each field in the list corresponds to a field in the manifest file’s partition spec.
    partitions: Option<Vec<FieldSummary>>,
    /// Implementation-specific key metadata for encryption
    key_metadata: Option<ByteBuf>,
}
